"""Shared fixtures for the whole suite."""

import pathlib
import tempfile

import pytest


@pytest.fixture
def tmp_path():
    """A short temporary directory: unix socket paths are capped at 104 bytes on macOS."""
    with tempfile.TemporaryDirectory(prefix="kbus-", dir="/tmp") as path:
        yield pathlib.Path(path)
