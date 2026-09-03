"""Sphinx configuration for the nf-fine-mapping pipeline."""

from __future__ import annotations

import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
COLLECTOR_ROOT = REPOSITORY_ROOT / "tools" / "collector"
sys.path.insert(0, str(COLLECTOR_ROOT / "src"))

project = "nf-fine-mapping"
copyright = "2026, Open Targets"
author = "Open Targets"
release = "0.1"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "abstract", "Thumbs.db", ".DS_Store"]

html_theme = "pydata_sphinx_theme"
html_theme_options = {
    "github_url": "https://github.com/opentargets/nf-fine-mapping",
    "show_toc_level": 2,
    "navigation_with_keys": True,
}
html_title = "nf-fine-mapping"
html_static_path = ["_static"]
