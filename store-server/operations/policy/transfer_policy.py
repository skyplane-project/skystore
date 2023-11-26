from ..schemas.object_schemas import LocateObjectRequest, DBPhysicalObjectLocator
from .utils.helpers import make_nx_graph
from .model.config import Config
import sys
from typing import List


class TransferPolicy:
    def __init__(self) -> None:
        self.stat_graph = make_nx_graph()
        pass

    # def get_physical_object_locators(self, req: LocateObjectRequest) -> List[str]:
    #     physical_object_locators = []

    def get(
        self, req: LocateObjectRequest, physical_locators: List[DBPhysicalObjectLocator]
    ) -> DBPhysicalObjectLocator:
        # Assume always direct transfer
        # phyiscal_locators = self.get_physical_object_locators(req)
        # place based on the physical locators
        pass


class CheapestTransfer(TransferPolicy):
    def get(
        self, req: LocateObjectRequest, physical_locators: List[DBPhysicalObjectLocator]
    ) -> DBPhysicalObjectLocator:
        # phyiscal_locators = self.get_physical_object_locators(req)

        client_from_region = req.client_from_region

        for locator in physical_locators:
            if client_from_region == locator.location_tag:
                return locator

        # find the cheapest region to get from client_from_region
        # can get the cost of self.stat_graph[client_from_region][region]['cost']
        # can get the latency of self.stat_graph[client_from_region][region]['latency']
        min_cost = sys.maxsize
        chosen_locator = None
        for locator in physical_locators:
            region = locator.location_tag
            if min_cost > self.stat_graph[client_from_region][region]["cost"]:
                min_cost = self.stat_graph[client_from_region][region]["cost"]
                chosen_locator = locator
        return chosen_locator


class ClosestTransfer(TransferPolicy):
    def get(
        self, req: LocateObjectRequest, physical_locators: List[DBPhysicalObjectLocator]
    ) -> DBPhysicalObjectLocator:
        # phyiscal_locators = self.get_physical_object_locators(req)
        client_from_region = req.client_from_region

        print("type: ", physical_locators)

        for locator in physical_locators:
            if client_from_region == locator.location_tag:
                return locator

        # find the cheapest region to get from client_from_region
        # can get the cost of self.stat_graph[client_from_region][region]['cost']
        # can get the latency of self.stat_graph[client_from_region][region]['latency']
        max_throughput = 0
        chosen_locator = None
        for locator in physical_locators:
            region = locator.location_tag
            print("region: ", region)
            if (
                max_throughput
                < self.stat_graph[client_from_region][region]["throughput"]
            ):
                max_throughput = self.stat_graph[client_from_region][region][
                    "throughput"
                ]
                chosen_locator = locator
        return chosen_locator


class DirectTransfer(TransferPolicy):
    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config
        pass

    def get(
        self, req: LocateObjectRequest, physical_locators: List[DBPhysicalObjectLocator]
    ) -> DBPhysicalObjectLocator:
        for locator in physical_locators:
            if self.config.storage_region == locator.location_tag:
                return locator
        pass


get_policy = TransferPolicy()
