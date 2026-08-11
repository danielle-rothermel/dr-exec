from __future__ import annotations

import keyword

from pydantic import field_validator

from dr_exec.core.model import ContractModel


class ImportableEntryPoint(ContractModel):
    """One absolute module and one exact module-level attribute."""

    module_name: str
    attribute_name: str

    @field_validator("module_name")
    @classmethod
    def module_name_must_be_absolute(cls, value: str) -> str:
        parts = value.split(".")
        if not parts or any(
            not part.isidentifier() or keyword.iskeyword(part)
            for part in parts
        ):
            raise ValueError(
                "module_name must be an absolute dotted Python module name"
            )
        return value

    @field_validator("attribute_name")
    @classmethod
    def attribute_name_must_be_exact(cls, value: str) -> str:
        if not value.isidentifier() or keyword.iskeyword(value):
            raise ValueError("attribute_name must be one Python identifier")
        return value


__all__ = ["ImportableEntryPoint"]
