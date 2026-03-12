"""
config_loader.py
Loads and validates job configuration from a JSON file.
Supports environment variable substitution for secrets and paths.
"""
 
import json
import os
import logging
from typing import Any
 
logger = logging.getLogger(__name__)
 
 
REQUIRED_KEYS = ["job_name", "source", "target"]
 
 
class ConfigValidationError(Exception):
    pass
 
 
class ConfigLoader:
    """
    Loads a JSON job config, validates required keys, and resolves
    environment variable placeholders (e.g. ${MY_VAR}).
    """
 
    def __init__(self, config_path: str):
        self.config_path = config_path
        self._config: dict = {}
 
    def load(self) -> dict:
        """Load, validate, and return the job config as a dict."""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
 
        with open(self.config_path, "r") as f:
            raw = json.load(f)
 
        resolved = self._resolve_env_vars(raw)
        self._validate(resolved)
        self._config = resolved
 
        logger.info(f"Loaded config for job: {resolved.get('job_name')}")
        return resolved
 
    def get(self, key: str, default: Any = None) -> Any:
        """Return a value from the loaded config."""
        return self._config.get(key, default)
 
    def _validate(self, config: dict) -> None:
        """Raise ConfigValidationError if required keys are missing."""
        missing = [k for k in REQUIRED_KEYS if k not in config]
        if missing:
            raise ConfigValidationError(
                f"Missing required config keys: {missing}"
            )
 
        # Validate source block
        source = config.get("source", {})
        if "type" not in source or "path" not in source:
            raise ConfigValidationError(
                "Config 'source' must contain 'type' and 'path'"
            )
 
        # Validate target block
        target = config.get("target", {})
        if "type" not in target or "path" not in target:
            raise ConfigValidationError(
                "Config 'target' must contain 'type' and 'path'"
            )
 
    def _resolve_env_vars(self, obj: Any) -> Any:
        """Recursively replace ${VAR} placeholders with environment variables."""
        if isinstance(obj, str):
            if obj.startswith("${") and obj.endswith("}"):
                var_name = obj[2:-1]
                value = os.environ.get(var_name)
                if value is None:
                    logger.warning(f"Environment variable '{var_name}' not set")
                return value or obj
            return obj
        elif isinstance(obj, dict):
            return {k: self._resolve_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._resolve_env_vars(i) for i in obj]
        return obj
