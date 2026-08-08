from __future__ import annotations

from pathlib import Path
from sys import argv

import dr_exec
from dr_exec.capabilities import CachedRecordReceipt, CachingExecutor

EXPECTED_ROOT_EXPORT_COUNT = 111


def main() -> None:
    if len(argv) != 2:
        raise SystemExit("usage: check_built_install.py REPOSITORY_ROOT")

    repository_root = Path(argv[1]).resolve(strict=True)
    package_file = Path(dr_exec.__file__).resolve(strict=True)
    if package_file.is_relative_to(repository_root):
        raise ValueError(
            f"import resolved to repository source: {package_file}"
        )

    exports = dr_exec.__all__
    if len(exports) != EXPECTED_ROOT_EXPORT_COUNT:
        raise ValueError(
            f"expected {EXPECTED_ROOT_EXPORT_COUNT} root exports, "
            f"found {len(exports)}"
        )
    missing_exports = [name for name in exports if not hasattr(dr_exec, name)]
    if missing_exports:
        raise ValueError(f"missing root exports: {missing_exports!r}")

    capability_exports = (CachedRecordReceipt, CachingExecutor)
    print(
        f"Validated {len(exports)} root exports from installed wheel at "
        f"{package_file}, plus {len(capability_exports)} capability exports."
    )


if __name__ == "__main__":
    main()
