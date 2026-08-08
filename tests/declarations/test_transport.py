from dr_serialize import IdentityDocument, canonical_identity_json_bytes

from dr_exec.declarations.transport import (
    request_transport_bytes,
    request_transport_digest,
)


def test_the_request_transport_is_exactly_canonical_identity_bytes(
    request_document: IdentityDocument,
) -> None:
    transport = request_transport_bytes(request_document)
    assert transport == canonical_identity_json_bytes(request_document)
    assert not transport.startswith(b"\xef\xbb\xbf")
    assert not transport.endswith(b"\n")
    assert b"\n" not in transport
    assert request_transport_digest(transport) == (
        "d3db717a052abddfd875dcbfb17c9f585d1457d60c0bc821fd1c020f9aef3bdb"
    )
