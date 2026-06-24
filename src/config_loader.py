"""
Centralised config loader.
All modules call load_config() rather than each opening config.yaml themselves.
"""

import yaml
from functools import lru_cache
from src.paths import BASE_DIR

CONFIG_PATH = BASE_DIR / "config" / "config.yaml"


@lru_cache(maxsize=1)
def load_config() -> dict:
    """Load and cache config.yaml. Call load_config.cache_clear() to force reload."""
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)
