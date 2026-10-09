# Copyright 2026 Softwell S.r.l.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Sphinx configuration for the kbus documentation."""

import sys
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from pathlib import Path

# The package is expected to be installed (``pip install -e ".[docs]"``); add
# ``src`` to the path as a fallback so autodoc resolves imports either way.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

project = "kbus"
copyright = "2026, Softwell S.r.l."
author = "Genropy Team"
try:
    release = _pkg_version("kajenn-kbus")
except PackageNotFoundError:
    release = "0.0.0.dev0"
version = release

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "myst_parser",
]

templates_path = []
exclude_patterns = ["_build", "design", "Thumbs.db", ".DS_Store"]

# The narrative pages are Markdown (the README is the specification and is
# included as is); the API pages are rst.
source_suffix = {".rst": "restructuredtext", ".md": "markdown"}
myst_heading_anchors = 3

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_css_files = ["readability.css"]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_include_init_with_doc = True

# Every class is documented once, at its public name (``kbus.Message``, not
# ``kbus.message.Message``): a second entry for the same object is a warning,
# and warnings fail the build. Each class directive names its members: the
# public API of the README, not every method without a leading underscore.
autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_default_options = {
    "show-inheritance": True,
    "undoc-members": True,
}
