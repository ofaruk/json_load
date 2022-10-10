from pathlib import Path
from dataclasses import dataclass


@dataclass
class Folders:
    config_folder = Path("config/")
    data_folder = Path("data/")
    resource_folder = Path("resources/")
    tools_folder = Path("tools/")
