import pandas as pd
import networkx as nx
import os
import ast
from operations.policy.utils.definitions import (
    aws_instance_throughput_limit,
    gcp_instance_throughput_limit,
    azure_instance_throughput_limit,
)


def refine_string(s):
    parts = s.split("-")[:4]
    refined = parts[0] + ":" + "-".join(parts[1:])
    return refined


def convert_hyphen_to_colon(s):
    return s.replace(":", "-", 1)


def get_full_path(relative_path: str):
    # Move up to the SkyStore-Simulation directory from helpers.py
    base_path = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    full_path = os.path.join(base_path, relative_path)
    return full_path


def load_profile(file_name: str):
    return pd.read_csv(f"src/profiles/{file_name}")


def make_nx_graph(
    cost_path=None,
    throughput_path=None,
    latency_path=None,
    storage_cost_path=None,
    num_vms=1,
):
    """
    Default graph with capacity constraints and cost info
    nodes: regions, edges: links
    per edge:
        throughput: max tput achievable (gbps)
        cost: $/GB
        flow: actual flow (gbps), must be < throughput, default = 0
    """
    path = os.path.dirname(os.path.abspath(__file__))
    if cost_path is None:
        cost = pd.read_csv(os.path.join(path, "profiles", "cost.csv"))
    else:
        cost = pd.read_csv(cost_path)

    if throughput_path is None:
        throughput = pd.read_csv(os.path.join(path, "profiles", "throughput.csv"))
    else:
        throughput = pd.read_csv(throughput_path)

    if latency_path is None:
        latency = pd.read_csv(os.path.join(path, "profiles", "latency.csv"))
    else:
        latency = pd.read_csv(latency_path)

    if storage_cost_path is None:
        storage = pd.read_csv(os.path.join(path, "profiles", "storage.csv"))
    else:
        storage = pd.read_csv(storage_cost_path)

    G = nx.DiGraph()
    for _, row in throughput.iterrows():
        G.add_edge(
            row["src_region"],
            row["dst_region"],
            cost=None,
            throughput=num_vms * row["throughput_sent"] / 1e9,
        )

    for _, row in latency.iterrows():
        row = row[0]
        row_dict = ast.literal_eval(row)
        src = row_dict["src_region"]
        dst = row_dict["dst_bucket_region"]
        if not G.has_edge(src, dst):
            continue

        G[src][dst]["latency"] = row_dict["download_latency"]

    for _, row in cost.iterrows():
        if row["src"] in G and row["dest"] in G[row["src"]]:
            G[row["src"]][row["dest"]]["cost"] = row["cost"]

    # some pairs not in the cost grid
    no_cost_pairs = []
    for edge in G.edges.data():
        src, dst = edge[0], edge[1]
        if edge[-1]["cost"] is None:
            no_cost_pairs.append((src, dst))
    print("Unable to get egress costs for: ", no_cost_pairs)

    no_storage_cost = set()
    for _, row in storage.iterrows():
        region = row["Vendor"] + ":" + row["Region"]

        if region in G:
            if row["Group"] == "storage" and (
                row["Tier"] == "General Purpose" or row["Tier"] == "Hot"
            ):
                G.nodes[region]["priceStorage"] = row["PricePerUnit"]
        else:
            no_storage_cost.add(region)
    print("Unable to get storage cost for: ", no_storage_cost)

    # TODO: add attributes priceGet, pricePut, and priceStorage to each node
    for node in G.nodes:
        G.nodes[node]["priceGet"] = 4.4e-07
        G.nodes[node]["pricePut"] = 5.5e-06
        if "priceStorage" not in G.nodes[node]:
            G.nodes[node]["priceStorage"] = 0.023

    # NOTE: add default throughput and latency to self-edges
    for node in G.nodes:
        if not G.has_edge(node, node):
            ingress_limit = (
                aws_instance_throughput_limit[1]
                if node.startswith("aws")
                else gcp_instance_throughput_limit[1]
                if node.startswith("gcp")
                else azure_instance_throughput_limit[1]
            )
            G.add_edge(
                node, node, cost=0, throughput=num_vms * ingress_limit, latency=40
            )

    # aws_nodes = [node for node in G.nodes if node.startswith("aws")]
    # just keep aws nodes and edges
    # G = G.subgraph(aws_nodes).copy()
    return G
