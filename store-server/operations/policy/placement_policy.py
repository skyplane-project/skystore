from typing import List
from ..schemas.object_schemas import StartUploadRequest
from ..schemas.bucket_schemas import DBPhysicalBucketLocator
from .utils.helpers import make_nx_graph
from .model.config import Config


class PlacementPolicy:
    def __init__(self) -> None:
        self.stat_graph = make_nx_graph()
        pass

    def place(self, req: StartUploadRequest) -> List[str]:
        pass

    def name(self) -> str:
        pass


class SingleRegionWrite(PlacementPolicy):
    """
    Write to the same region as the original storage region defined in the config
    """

    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config
        pass

    def place(self, req: StartUploadRequest) -> List[str]:
        """
        Args:
            req: StartUploadRequest
        Returns:
            List[str]: single region to write to
        """
        return [self.config.storage_region]

    def name(self) -> str:
        return "single_region"


class ReplicateAll(PlacementPolicy):
    """
    Replicate all objects to all regions
    """

    def place(self, req: StartUploadRequest) -> List[str]:
        """
        Args:
            req: StartUploadRequest
        Returns:
            List[str]: all available regions in the current nodes graph
        """
        return list(self.stat_graph.nodes())

    def get_policy(self) -> str:
        return "replicate_all"


class PushonWrite(PlacementPolicy):
    """
    Write local and push asynchronously to a set of pushed regions
    """

    def place(
        self,
        req: StartUploadRequest,
        physical_bucket_locators: List[DBPhysicalBucketLocator],
    ) -> List[str]:
        """
        Args:
            req: StartUploadRequest
            physical_bucket_locators: List[DBPhysicalBucketLocator]
        Returns:
            List[str]: the regions to push to, including the primary region and the regions that need warmup
        """

        upload_to_region_tags = [
            locator.location_tag
            for locator in physical_bucket_locators
            if locator.is_primary or locator.need_warmup
        ]
        return upload_to_region_tags

    def name(self) -> str:
        return "push"


class PullOnRead(PlacementPolicy):
    """
    Write local, and pull on read if data is not available locally
    """

    def place(self, req: StartUploadRequest) -> List[str]:
        """
        Args:
            req: StartUploadRequest
        Returns:
            List[str]: the region client is from
        """

        return [req.client_from_region]

    def name(self) -> str:
        return "copy_on_read"


class LocalWrite(PlacementPolicy):
    """
    Write to local region
    """

    def place(self, req: StartUploadRequest) -> List[str]:
        """
        Args:
            req: StartUploadRequest
        Returns:
            List[str]: the local region client is from
        """
        return [req.client_from_region]

    def name(self) -> str:
        return "write_local"


put_policy = PlacementPolicy()
