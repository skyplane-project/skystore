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

    def get_policy(self) -> str:
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
        if req.op == "write":
            return [self.config.storage_region]
        else:
            return []

    def get_policy(self) -> str:
        return "single-region-write"


class ReplicateAll(PlacementPolicy):
    """
    Replicate all objects to all regions
    """

    def place(self, req: StartUploadRequest) -> List[str]:
        if req.op == "write":
            return list(self.total_graph.nodes())
        else:
            return []

    def get_policy(self) -> str:
        return "replicate-all"


class PushonWrite(PlacementPolicy):
    """
    Write local and push asynchronously to a set of pushed regions
    """

    def place(
        self,
        req: StartUploadRequest,
        physical_bucket_locators: List[DBPhysicalBucketLocator],
    ) -> List[str]:
        upload_to_region_tags = [
            locator.location_tag
            for locator in physical_bucket_locators
            if locator.is_primary or locator.need_warmup
        ]
        # check the type of physical_bucket_locators
        # print("type of locators: ", type(physical_bucket_locators))
        return upload_to_region_tags

    def get_policy(self) -> str:
        return "push"


class PullOnRead(PlacementPolicy):
    """
    Write local, and pull on read if data is not available locally
    """

    def place(self, req: StartUploadRequest) -> List[str]:
        return [req.client_from_region]

    def get_policy(self) -> str:
        return "copy_on_read"


class LocalWrite(PlacementPolicy):
    """
    Write to local region
    """

    def place(self, req: StartUploadRequest) -> List[str]:
        if req.op == "write":
            return [req.client_from_region]
        else:
            return []

    def get_policy(self) -> str:
        return "write_local"


put_policy = PlacementPolicy()
