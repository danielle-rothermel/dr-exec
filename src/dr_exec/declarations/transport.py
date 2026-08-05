from dr_serialize import IdentityDocument, canonical_identity_json_bytes


def request_transport_bytes(request: IdentityDocument, /) -> bytes:
    return canonical_identity_json_bytes(request)


__all__ = ["request_transport_bytes"]
