from __future__ import annotations

import signal

from dr_exec import CancelToken, forward_parent_signals


def test_forward_parent_signals_cancels_the_token_on_sigterm() -> None:
    token = CancelToken()
    previous = signal.getsignal(signal.SIGTERM)

    with forward_parent_signals(token):
        signal.raise_signal(signal.SIGTERM)

    assert token.cancelled
    assert signal.getsignal(signal.SIGTERM) is previous


def test_forward_parent_signals_restores_previous_handlers() -> None:
    token = CancelToken()
    previous_term = signal.getsignal(signal.SIGTERM)
    previous_int = signal.getsignal(signal.SIGINT)

    with forward_parent_signals(token):
        assert signal.getsignal(signal.SIGTERM) is not previous_term
        assert signal.getsignal(signal.SIGINT) is not previous_int

    assert signal.getsignal(signal.SIGTERM) is previous_term
    assert signal.getsignal(signal.SIGINT) is previous_int


def test_forward_parent_signals_cancels_the_token_on_sigint() -> None:
    token = CancelToken()

    with forward_parent_signals(token):
        signal.raise_signal(signal.SIGINT)

    assert token.cancelled
