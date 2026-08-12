from __future__ import annotations

import pytest
from dr_serialize import build_identity_document

from dr_exec.core.identity import require_identity_role

EXPECTED_SCHEMA = "dr_exec.test_identity"


@pytest.mark.parametrize(
    ("schema", "schema_version", "payload", "message"),
    [
        pytest.param(
            "dr_exec.foreign_identity",
            1,
            {},
            f"identity must use schema {EXPECTED_SCHEMA}",
            id="wrong-schema",
        ),
        pytest.param(
            EXPECTED_SCHEMA,
            2,
            {},
            "identity must use schema version 1",
            id="wrong-version",
        ),
        pytest.param(
            EXPECTED_SCHEMA,
            1,
            [],
            "identity payload must be a mapping",
            id="nonmapping-payload",
        ),
    ],
)
def test_identity_roles_reject_wrong_envelopes(
    schema: str,
    schema_version: int,
    payload: dict[str, object] | list[object],
    message: str,
) -> None:
    document = build_identity_document(
        schema=schema,
        schema_version=schema_version,
        payload=payload,
    )

    with pytest.raises(ValueError, match=message):
        require_identity_role(document, schema=EXPECTED_SCHEMA)
