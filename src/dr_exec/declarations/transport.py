import hashlib

from dr_serialize import (
    IdentityDocument,
    Sha256Digest,
    canonical_identity_json_bytes,
)


def request_transport_bytes(request: IdentityDocument, /) -> bytes:
    return canonical_identity_json_bytes(request)


def request_transport_digest(transport: bytes, /) -> Sha256Digest:
    return Sha256Digest(hashlib.sha256(transport).hexdigest())


__all__ = ["request_transport_bytes", "request_transport_digest"]
