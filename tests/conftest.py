"""
Pytest configuration and fixtures for the brasa test suite.

This module sets up the test environment, including:
- Temporary directory for BRASA_DATA_PATH
- Cache cleanup between tests
- Singleton reset mechanisms
"""

import os
import shutil
from contextlib import closing
from pathlib import Path

import pytest


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment(tmp_path_factory):
    """
    Session-scoped fixture to set up BRASA_DATA_PATH to a temporary directory.

    This fixture runs once at the start of the test session and ensures all tests
    use a temporary cache directory instead of polluting the project directory.

    IMPORTANT: This must run BEFORE any imports of brasa.engine.CacheManager,
    as CacheManager reads BRASA_DATA_PATH during initialization.
    """
    # Create a session-scoped temporary directory
    tmp_dir = tmp_path_factory.mktemp("brasa_test_cache")

    # Set the environment variable
    os.environ["BRASA_DATA_PATH"] = str(tmp_dir)

    yield tmp_dir

    # Cleanup after all tests complete
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir, ignore_errors=True)


@pytest.fixture(autouse=True)
def cleanup_cache_between_tests():
    """
    Cleanup cache directories after each test to ensure test isolation.

    This prevents:
    - DuplicatedFolderException errors from previous test runs
    - Test interdependencies
    - Cache pollution between tests
    """
    yield  # Let the test run

    # Cleanup after each test
    from brasa.engine import CacheManager

    try:
        man = CacheManager()
        cache_path = Path(man.cache_folder)

        # Remove raw folder (downloaded files)
        raw_path = cache_path / "raw"
        if raw_path.exists():
            shutil.rmtree(raw_path, ignore_errors=True)

        # Remove db folder (processed files)
        db_path = cache_path / "db"
        if db_path.exists():
            shutil.rmtree(db_path, ignore_errors=True)
            # Recreate it for next test
            db_path.mkdir(parents=True, exist_ok=True)

        # Clean metadata database
        try:
            with closing(man.meta_db_connection) as conn, conn:
                c = conn.cursor()
                c.execute("DELETE FROM cache_metadata")
                c.execute("DELETE FROM download_trials")
        except Exception:
            # If there's an issue with the database, just continue
            pass

    except Exception:
        # If CacheManager hasn't been initialized yet, that's fine
        pass
