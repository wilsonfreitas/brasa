"""SuppressUserWarnings must restore pre-existing filters (audit Q8.7)."""

import warnings

from brasa.util import SuppressUserWarnings


def test_suppresses_user_warnings_inside_block(recwarn):
    with SuppressUserWarnings():
        warnings.warn("hidden", UserWarning, stacklevel=1)
    assert len(recwarn) == 0


def test_restores_previous_filters_on_exit():
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        before = list(warnings.filters)
        with SuppressUserWarnings():
            pass
        assert warnings.filters == before
