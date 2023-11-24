from typing import Dict, Set
import networkx as nx
from src.model.object import PhysicalObject
from datetime import timedelta, datetime
import logging
from src.utils.definitions import GB


class RegionManager:
    def __init__(self, total_graph: nx.DiGraph, logger: logging.Logger):
        self.regions: Dict[str, Set[int]] = {}  # Mapping of region name to the list of PhysicalObject IDs
        self.region_objects: Dict[str, Set[PhysicalObject]] = {}
        self.total_graph = total_graph

        # Metrics: storage cost
        self.storage_costs: Dict[str, float] = {}

        self.logger = logger

    def _price_per_GB(self, region: str, duration: timedelta) -> float:
        price_per_gb_per_month = self.total_graph.nodes[region].get("priceStorage", 0.0)
        # Convert duration to months
        months = duration.total_seconds() / (30 * 24 * 3600)  # Assuming 30 days in a month
        return price_per_gb_per_month * months

    def _get_storage_cost_per_gb(self, region: str) -> float:
        return self.total_graph.nodes[region].get("priceStorage", None)

    def add_object_to_region(self, region: str, physical_object: PhysicalObject):
        """Add object to region."""
        if region not in self.regions:
            self.regions[region] = set()
            self.region_objects[region] = set()

        self.regions[region].add(physical_object.key)
        self.region_objects[region].add(physical_object)
        self.logger.info(f"Adding object {physical_object.key} to region {region}.")

    def get_object_in_region(self, region):
        """Get object in region."""
        objects = self.regions.get(region, [])
        self.logger.info(f"Getting object in region {region}: {objects}.")
        return objects

    def has_object_in_region(self, region: str, physical_object_key: str):
        """Check if object is in region."""
        exist = physical_object_key in self.regions.get(region, [])
        self.logger.info(f"Object {physical_object_key} in region {region}: {exist}.")
        return exist

    def clear_all_objects(self):
        """Clear all objects in all regions."""
        self.region_objects = {}
        self.logger.info("Clearing all objects in all regions.")

    def remove_object_from_region(self, region: str, physical_object: PhysicalObject, end_time: datetime):
        """Remove object from the region and calculate its storage cost."""
        if region in self.regions and physical_object.key in self.regions[region]:
            self.regions[region].remove(physical_object.key)
            self.region_objects[region].remove(physical_object)

            duration = physical_object.storage_duration(end_time)
            cost = self._price_per_GB(region, duration)
            self.storage_costs[region] = self.storage_costs.get(region, 0.0) + cost
            self.logger.info(f"Removing object {physical_object.key} from region {region}.")

    def calculate_remaining_storage_costs(self, end_time: datetime):
        """Calculate and aggregate storage costs for objects still stored."""
        print(f"Region objects: {self.region_objects}")
        for region, objects in self.region_objects.items():
            # For each of the object
            for physical_object in objects:
                # Duration is the time object is stored
                duration = physical_object.storage_duration(end_time)

                cost = self._price_per_GB(region, duration) * (physical_object.size / GB)
                self.storage_costs[region] = self.storage_costs.get(region, 0.0) + cost
                print(
                    f"Remaining cost for object {physical_object.key} in region {region} for duration {duration.total_seconds()} (seconds): {round(cost, 9)}."
                )

    def aggregate_storage_cost(self):
        return sum(self.storage_costs.values())

    def print_stat(self):
        """Print statistics."""
        self.logger.info(f"Total number of regions storing objects: {len(self.regions)}")
        print_rst = [(region, len(objects)) for region, objects in self.regions.items()]
        self.logger.info(f"Number of objects stored in each region: {print_rst}")
        size = 0
        for _, objects in self.region_objects.items():
            for physical_object in objects:
                size += physical_object.size / GB
        self.logger.info(f"Sum of all object sizes stored in all regions: {size} GB")
