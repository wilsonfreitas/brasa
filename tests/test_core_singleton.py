"""Singleton must not cache an instance whose init() raised."""

import pytest

from brasa.engine.core import Singleton


class FlakyService(Singleton):
    fail_next = True

    def init(self):
        if type(self).fail_next:
            type(self).fail_next = False
            raise RuntimeError("boom")
        self.ready = True


def test_failed_init_does_not_cache_instance():
    FlakyService.fail_next = True
    with pytest.raises(RuntimeError):
        FlakyService()
    instance = FlakyService()  # must run init again, not return a broken instance
    assert instance.ready
