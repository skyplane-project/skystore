from datetime import datetime, timedelta
from enum import Enum
from typing import Dict


class Status(str, Enum):
    pending = "pending"
    pending_deletion = "pending_deletion"
    ready = "ready"


class LogicalObject:
    def __init__(self, key: str, size: int, last_modified: str, status: Status = Status.ready):
        self.key = key
        self.size = size
        self.last_modified = last_modified
        self.status = status
        self.physical_objects: Dict[str, PhysicalObject] = {}  # Track the physical objects (locations) linked to this logical object.

    def add_physical_object(self, region, physical_object):
        self.physical_objects[region] = physical_object

    def is_ready_in_region(self, region: str) -> bool:
        return self.physical_objects.get(region, None).status == Status.ready


class PhysicalObject:
    def __init__(self, location_tag: str, key: str, size: int, status: Status = Status.ready):
        self.location_tag = location_tag
        self.cloud, self.region = self.location_tag.split(":")
        self.key = key
        self.status = status  # TODO: this is not used now
        self.storage_start_time: datetime = None
        self.size = size

    def __hash__(self):
        return hash((self.key, self.location_tag))

    def __eq__(self, other):
        return isinstance(other, PhysicalObject) and self.key == other.key and self.location_tag == other.location_tag

    def set_status(self, status: Status):
        self.status = status

    def set_storage_start_time(self, time: datetime):
        self.storage_start_time = time

    def storage_duration(self, end_time: datetime) -> timedelta:
        print(f"Object: {self.key}, Region: {self.location_tag}, Start time: {self.storage_start_time}, end time: {end_time}")
        if self.storage_start_time:
            return end_time - self.storage_start_time
        return timedelta(0)
