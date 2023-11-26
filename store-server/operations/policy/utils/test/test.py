from helpers import make_nx_graph
import numpy as np

G = make_nx_graph()
print(G.edges.data())
# print average edge throughput
print("Average edge cost: ", np.mean([G[u][v]["cost"] for u, v in G.edges()]))

# print average storage price
print(
    "Average storage cost: ", np.mean([G.nodes[u]["priceStorage"] for u in G.nodes()])
)
