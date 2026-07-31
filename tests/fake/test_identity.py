"""The fake's identity: its own, distinct, and never the production one."""

from __future__ import annotations

import pytest

from dr_exec.errors import DeclarationError
from dr_exec.fake import FakeExecutor
from dr_exec.record import EXECUTOR_IDENTITY, FAKE_EXECUTOR_IDENTITY


class TestIdentity:
    def test_defaults_to_the_fake_identity(self) -> None:
        assert FakeExecutor().identity == FAKE_EXECUTOR_IDENTITY

    def test_the_fake_identity_is_not_the_production_identity(self) -> None:
        assert FAKE_EXECUTOR_IDENTITY != EXECUTOR_IDENTITY

    def test_refuses_construction_claiming_the_production_identity(self) -> None:
        with pytest.raises(DeclarationError, match="may not claim"):
            FakeExecutor(identity=EXECUTOR_IDENTITY)

    def test_accepts_a_consumer_declared_fake_name(self) -> None:
        assert FakeExecutor(identity="corpus-double@1").identity == "corpus-double@1"

    @pytest.mark.parametrize("bad", ["", None, 7])
    def test_refuses_an_identity_that_is_not_a_nonempty_string(
        self, bad: object
    ) -> None:
        with pytest.raises(DeclarationError, match="nonempty string"):
            FakeExecutor(identity=bad)  # type: ignore[arg-type]
