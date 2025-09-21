# -*- coding: utf-8 -*-
"""Public package surface for openfpv-compat."""

__all__ = ["__version__", "load_parts", "build_compat", "summarize"]

# Keep in sync with pyproject.toml
__version__ = "0.1.0"

from .engine import load_parts, build_compat  # noqa: E402
from .summarize import summarize  # noqa: E402
