"""Set up project paths."""
from pathlib import Path


def project_root() -> Path:
    """Get project path."""
    return Path(__file__).parent.parent


def storage_root() -> Path:
    """Get storage path."""
    return Path(__file__).parent.parent / "storage"


def storage_external_root() -> Path:
    """Get storage external path."""
    path = storage_root() / "external"
    Path(path).mkdir(exist_ok=True, parents=True)
    return path


def storage_interim_root() -> Path:
    """Get storage interim path."""
    path = storage_root() / "interim"
    Path(path).mkdir(exist_ok=True, parents=True)
    return path


def storage_processed_root() -> Path:
    """Get storage procesTsed path."""
    path = storage_root() / "processed"
    Path(path).mkdir(exist_ok=True, parents=True)
    return path


def outputs_root() -> Path:
    """Get output path."""
    path = Path(__file__).parent.parent / "output"
    Path(path).mkdir(exist_ok=True, parents=True)
    return path


def get_path(path) -> Path:
    """Get path, if exists. If not, create it."""
    Path(path).mkdir(exist_ok=True, parents=True)
    return path


# https://stackoverflow.com/a/50194143/1889006
# https://stackoverflow.com/a/53465812/1889006
