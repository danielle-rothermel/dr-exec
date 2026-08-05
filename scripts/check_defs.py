from __future__ import annotations

import importlib
import tomllib
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import cast

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
TERMS_PATH = REPOSITORY_ROOT / ".defs" / "terms.toml"
CONTRACTS_PATH = REPOSITORY_ROOT / ".defs" / "contracts.toml"
TERMS_SCHEMA_PATH = REPOSITORY_ROOT / ".defs" / "terms.schema.json"
TERMS_SCHEMA_DIRECTIVE = "#:schema ./terms.schema.json"


def _load_tables(path: Path, key: str) -> tuple[dict[str, object], ...]:
    document = tomllib.loads(path.read_text(encoding="utf-8"))
    value = document.get(key)
    if not isinstance(value, list):
        raise TypeError(f"{path}: expected a {key!r} array")

    tables: list[dict[str, object]] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict) or not all(
            isinstance(item_key, str) for item_key in item
        ):
            raise ValueError(f"{path}: {key}[{index}] must be a table")
        tables.append(cast(dict[str, object], item))
    return tuple(tables)


def _required_string(
    table: Mapping[str, object],
    key: str,
    *,
    location: str,
) -> str:
    value = table.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{location}: {key!r} must be a non-empty string")
    return value


def _string_list(
    table: Mapping[str, object],
    key: str,
    *,
    location: str,
) -> tuple[str, ...]:
    value = table.get(key)
    if value is None:
        return ()
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item.strip() for item in value
    ):
        raise ValueError(
            f"{location}: {key!r} must be a non-empty string array"
        )
    values = cast(list[str], value)
    if len(values) != len(set(values)):
        raise ValueError(f"{location}: {key!r} contains duplicates")
    return tuple(values)


def _resolve_exported_symbol(reference: str, *, location: str) -> None:
    parts = reference.split(".")
    if len(parts) < 2 or any(not part for part in parts):
        raise ValueError(
            f"{location}: exported symbol must be a dotted public reference"
        )

    try:
        value: object = importlib.import_module(parts[0])
        for part in parts[1:]:
            value = getattr(value, part)
    except (AttributeError, ImportError) as error:
        raise ValueError(
            f"{location}: exported symbol {reference!r} does not resolve"
        ) from error


def _validate_acyclic(
    edges: Mapping[str, tuple[str, ...]],
    *,
    relation: str,
) -> None:
    complete: set[str] = set()
    active: list[str] = []

    def visit(name: str) -> None:
        if name in complete:
            return
        if name in active:
            cycle_start = active.index(name)
            cycle = " -> ".join((*active[cycle_start:], name))
            raise ValueError(f"{relation} cycle: {cycle}")

        active.append(name)
        for target in edges[name]:
            visit(target)
        active.pop()
        complete.add(name)

    for name in edges:
        visit(name)


def validate_terms() -> int:
    if not TERMS_SCHEMA_PATH.is_file():
        raise ValueError(f"missing terms schema: {TERMS_SCHEMA_PATH}")
    first_line = TERMS_PATH.read_text(encoding="utf-8").splitlines()[0]
    if first_line != TERMS_SCHEMA_DIRECTIVE:
        raise ValueError(
            f"{TERMS_PATH}: first line must be {TERMS_SCHEMA_DIRECTIVE!r}"
        )

    terms = _load_tables(TERMS_PATH, "terms")
    names: list[str] = []
    relations: dict[str, dict[str, tuple[str, ...]]] = {
        "is_a": {},
        "part_of": {},
    }

    for index, term in enumerate(terms):
        location = f"{TERMS_PATH}: terms[{index}]"
        name = _required_string(term, "name", location=location)
        _required_string(term, "definition", location=location)
        names.append(name)

        for key in ("categories", "is_a", "part_of"):
            values = _string_list(term, key, location=location)
            if key in relations:
                relations[key][name] = values

        for reference in _string_list(
            term,
            "exported_symbols",
            location=location,
        ):
            _resolve_exported_symbol(reference, location=location)

    if len(names) != len(set(names)):
        raise ValueError(f"{TERMS_PATH}: term names must be unique")

    known_names = set(names)
    for relation, edges in relations.items():
        for name, targets in edges.items():
            for target in targets:
                if target == name:
                    raise ValueError(f"{relation} self-link: {name!r}")
                if target not in known_names:
                    raise ValueError(
                        f"{relation} target {target!r} from {name!r} is unknown"
                    )
        _validate_acyclic(edges, relation=relation)

    return len(terms)


def validate_contracts() -> int:
    contracts = _load_tables(CONTRACTS_PATH, "contracts")
    required_keys = {"title", "statement", "rationale", "date"}
    allowed_keys = required_keys | {"check"}
    titles: list[str] = []

    for index, contract in enumerate(contracts):
        location = f"{CONTRACTS_PATH}: contracts[{index}]"
        extra_keys = set(contract) - allowed_keys
        missing_keys = required_keys - set(contract)
        if extra_keys:
            raise ValueError(
                f"{location}: unexpected keys: {sorted(extra_keys)!r}"
            )
        if missing_keys:
            raise ValueError(
                f"{location}: missing keys: {sorted(missing_keys)!r}"
            )

        titles.append(_required_string(contract, "title", location=location))
        _required_string(contract, "statement", location=location)
        _required_string(contract, "rationale", location=location)
        contract_date = contract["date"]
        if not isinstance(contract_date, str):
            raise TypeError(f"{location}: 'date' must be an ISO date string")
        try:
            date.fromisoformat(contract_date)
        except ValueError as error:
            raise ValueError(
                f"{location}: 'date' must be an ISO date string"
            ) from error
        if "check" in contract:
            _required_string(contract, "check", location=location)

    if len(titles) != len(set(titles)):
        raise ValueError(f"{CONTRACTS_PATH}: contract titles must be unique")
    return len(contracts)


def main() -> None:
    term_count = validate_terms()
    contract_count = validate_contracts()
    print(f"Validated {term_count} terms and {contract_count} contracts.")


if __name__ == "__main__":
    main()
