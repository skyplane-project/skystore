from src.utils.definitions import GB


class Tracker:
    def __init__(self):
        self.latency_data = {"read": [], "write": []}
        self.transfer_costs = []
        self.storage_costs = []
        self.request_costs = []
        self.request_sizes = []
        self.total_cost = 0

        self.num_req = 0
        self.round_digit = 9
        self.duration = 0

    def set_duration(self, duration):
        self.duration = duration

    def add_latency(self, operation, latency):
        print(f"Adding latency {latency} for {operation}")
        self.latency_data[operation].append(latency)

    def add_transfer_cost(self, cost):
        print(f"Adding transfer cost {cost}")
        self.transfer_costs.append(cost)

    def add_storage_cost(self, cost):
        self.storage_costs.append(cost)

    def add_request_cost(self, cost):
        print(f"Adding request cost {cost}")
        self.request_costs.append(cost)

    def add_request_size(self, size):
        self.request_sizes.append(size)
        self.num_req += 1

    def compute_average(self, data_list):
        return round(sum(data_list) / len(data_list), 4) if data_list else 0

    def compute_sum(self, data_list):
        return round(sum(data_list), self.round_digit) if data_list else 0

    def get_metrics(self):
        tot_transfer_cost = self.compute_sum(self.transfer_costs)
        tot_storage_cost = self.compute_sum(self.storage_costs)
        tot_request_cost = self.compute_sum(self.request_costs)
        self.total_cost = tot_transfer_cost + tot_storage_cost + tot_request_cost

        print(f"Read latency: {self.latency_data['read']}")
        print(f"Write latency: {self.latency_data['write']}")
        metrics = {
            "Trace Duration (s)": self.duration,
            "total requests": self.num_req,
            "avg request size (GB)": self.compute_average(self.request_sizes) / GB,
            "aggregate size (GB)": self.compute_sum(self.request_sizes) / GB,
            "avg read latency (ms)": self.compute_average(self.latency_data["read"]) * 1000,
            "avg write latency (ms)": self.compute_average(self.latency_data["write"]) * 1000,
            "total transfer cost ($)": tot_transfer_cost,
            "total storage cost ($)": tot_storage_cost,
            "total request cost ($)": tot_request_cost,
            "total cost ($)": round(self.total_cost, self.round_digit),
        }
        return metrics

    def get_detailed_metrics(self):
        return {
            "read_latencies": self.latency_data["read"],
            "write_latencies": self.latency_data["write"],
            "transfer_costs": self.transfer_costs,
            "storage_costs": self.storage_costs,
            "request_costs": self.request_costs,
            "total_cost": round(self.total_cost, self.round_digit),
        }
