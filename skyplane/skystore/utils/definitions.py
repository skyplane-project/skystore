from pathlib import Path
from skyplane.utils.definitions import MB, GB, KB


def parse_size(size_str):
    size_str = size_str.strip().upper()
    if size_str.endswith("KB"):
        return int(size_str[:-2]) * KB
    elif size_str.endswith("MB"):
        return int(size_str[:-2]) * MB
    elif size_str.endswith("GB"):
        return int(size_str[:-2]) * GB
    else:
        return int(size_str)


tmp_log_dir = Path("/tmp/skystore")
