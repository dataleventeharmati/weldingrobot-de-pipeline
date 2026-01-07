from __future__ import annotations

from pathlib import Path
import yaml


DEFAULT_CONFIG_PATH = Path("config/thresholds.yaml")


class ConfigLoadError(RuntimeError):
    """Raised when the threshold configuration cannot be loaded."""


def load_thresholds(config_path: Path | str = DEFAULT_CONFIG_PATH) -> dict:
    """
    Load alert thresholds from a YAML configuration file.

    Parameters
    ----------
    config_path : Path | str
        Path to the YAML config file (default: config/thresholds.yaml)

    Returns
    -------
    dict
        Parsed thresholds configuration.

    Raises
    ------
    ConfigLoadError
        If the file does not exist or cannot be parsed.
    """
    path = Path(config_path)

    if not path.exists():
        raise ConfigLoadError(f"Threshold config not found: {path}")

    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except Exception as exc:
        raise ConfigLoadError(f"Failed to load threshold config: {path}") from exc

    if not isinstance(data, dict):
        raise ConfigLoadError("Threshold config must be a YAML mapping (dict)")

    return data
