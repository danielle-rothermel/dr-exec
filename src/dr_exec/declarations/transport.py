"""The exact child-input transport owned by a Python declaration."""

from dr_serialize import IdentityDocument, canonical_identity_json_bytes


def request_transport_bytes(request: IdentityDocument, /) -> bytes:
    """Return the exact bytes the child reads on stdin before EOF.

    Complete canonical identity-document bytes have no BOM, length prefix,
    delimiter, or trailing LF. Their length is both the value compared against
    the workload input budget before spawn and the recorded input byte count.
    """
    return canonical_identity_json_bytes(request)


__all__ = ["request_transport_bytes"]
