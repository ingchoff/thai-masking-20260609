"""Shared helpers for interacting with JamAI services."""
from __future__ import annotations

import logging
import os
from typing import Any, Mapping, Optional


def resolve_jamai_timeout(
    default: float = 900.0,
    *,
    env_var: str = "JAMAI_TIMEOUT_SEC",
    logger: Optional[object] = None,
) -> float:
    """Return the JamAI timeout configured in the environment.

    Args:
        default: Fallback timeout in seconds when the environment variable is
            missing or invalid.
        env_var: Name of the environment variable to read the timeout from.
        logger: Optional logger with a ``warning`` method for reporting invalid
            values. When omitted a module level ``logging.Logger`` is used.
    """

    raw_value = os.getenv(env_var)
    if not raw_value:
        return default

    try:
        return float(raw_value)
    except ValueError:
        message = f"Invalid {env_var} value '{raw_value}'; falling back to {default} seconds"
        if logger is not None:
            try:
                logger.warning(message)  # type: ignore[attr-defined]
            except Exception:
                logging.getLogger(__name__).warning(message)
        else:
            logging.getLogger(__name__).warning(message)
        return default


def extract_cell_value(field: Any) -> Any:
    """Extract the "real" cell value from JamAI table row fields.

    Newer `jamaibase` SDK versions may return table cell values as objects like
    `{"value": ...}` instead of returning the value directly.
    """

    if isinstance(field, dict) and "value" in field:
        return field.get("value")
    return field


def normalize_row_values(row: Mapping[str, Any]) -> dict[str, Any]:
    """Return a shallow copy of a JamAI table row with extracted cell values."""

    return {key: extract_cell_value(value) for key, value in row.items()}
