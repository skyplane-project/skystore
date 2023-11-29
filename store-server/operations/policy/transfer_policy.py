from ..schemas.object_schemas import LocateObjectRequest, DBPhysicalObjectLocator
from .utils.helpers import make_nx_graph
from .model.config import Config
from typing import List


class TransferPolicy:
    def __init__(self) -> None:
        self.stat_graph = make_nx_graph()
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
    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config
        pass

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

        locator = next(
            (
                l
                for l in physical_locators
                if self.config.storage_region == l.location_tag
            ),
            None,
        )
        if locator:
            return locator
        pass

    def name(self) -> str:
        return "direct"


get_policy = TransferPolicy()
