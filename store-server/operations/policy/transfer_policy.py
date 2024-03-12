from operations.schemas.object_schemas import (
    LocateObjectRequest,
    DBPhysicalObjectLocator,
)
from operations.policy.utils.helpers import make_nx_graph
from typing import List


class DataTransferGraph:
    """
    A singleton class representing the graph used for data transfer calculations.
    This ensures that only one instance of the graph is created and used throughout the application.
    """

    _instance = None

    @classmethod
    def get_instance(cls):
        """
        Returns the singleton instance of the graph. If it does not exist, it creates one.
        """
        if cls._instance is None:
            cls._instance = cls._create_graph()
        return cls._instance

    @staticmethod
    def _create_graph():
        """
        Creates the network graph. This method is internal to the class.
        """
        return make_nx_graph()


class TransferPolicy:
    def __init__(self) -> None:
        self.stat_graph = DataTransferGraph.get_instance()
        pass

    def get(
        self, req: LocateObjectRequest, physical_locators: List[DBPhysicalObjectLocator]
    ) -> DBPhysicalObjectLocator:
        pass

    def name(self) -> str:
        return ""


class CheapestTransfer(TransferPolicy):
    def get(
        self, req: LocateObjectRequest, physical_locators: List[DBPhysicalObjectLocator]
    ) -> DBPhysicalObjectLocator:
        """
        Args:
            req: LocateObjectRequest
            physical_locators: List[DBPhysicalObjectLocator]: physical locators of the object
        Returns:
            DBPhysicalObjectLocator: the cheapest physical locator to fetch from
        """

        client_from_region = req.client_from_region

        for locator in physical_locators:
            if client_from_region == locator.location_tag:
                return locator

        # find the cheapest region to get from client_from_region
        return min(
            physical_locators,
            key=lambda loc: self.stat_graph[client_from_region][loc.location_tag][
                "cost"
            ],
        )

    def name(self) -> str:
        return "cheapest"


class ClosestTransfer(TransferPolicy):
    def get(
        self, req: LocateObjectRequest, physical_locators: List[DBPhysicalObjectLocator]
    ) -> DBPhysicalObjectLocator:
        """
        Args:
            req: LocateObjectRequest
            physical_locators: List[DBPhysicalObjectLocator]: physical locators of the object
        Returns:
            DBPhysicalObjectLocator: the closest physical locator to fetch from
        """

        client_from_region = req.client_from_region

        for locator in physical_locators:
            if client_from_region == locator.location_tag:
                return locator

        # find the cheapest region to get from client_from_region
        return max(
            physical_locators,
            key=lambda loc: self.stat_graph[client_from_region][loc.location_tag][
                "throughput"
            ],
        )

    def name(self) -> str:
        return "closest"


class DirectTransfer(TransferPolicy):
    def get(
        self, req: LocateObjectRequest, physical_locators: List[DBPhysicalObjectLocator]
    ) -> DBPhysicalObjectLocator:
        """
        Args:
            req: LocateObjectRequest
            physical_locators: List[DBPhysicalObjectLocator]: physical locators of the object
        Returns:
            DBPhysicalObjectLocator: the single matched region to fetch from
        """

        config_region = ["aws:us-west-1"]

        locator = next(
            (
                locator
                for locator in physical_locators
                if config_region == locator.location_tag
            ),
            None,
        )
        if locator:
            return locator
        pass

    def name(self) -> str:
        return "direct"


def get_transfer_policy(name: str) -> TransferPolicy:
    if name == "cheapest":
        return CheapestTransfer()
    elif name == "closest":
        return ClosestTransfer()
    elif name == "direct":
        return DirectTransfer()
    else:
        raise Exception("Unknown transfer policy name")
