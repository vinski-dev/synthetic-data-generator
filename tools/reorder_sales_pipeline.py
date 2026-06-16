from __future__ import annotations

import re
import shutil
from pathlib import Path


TARGET_FILE = Path("pipeline/sales_pipeline.py")

DESIRED_FUNCTION_ORDER = [
    # Data generation
    "generate_sales_data",
    "generate_late_arriving_sales_data",
    "generate_sales_data_for_business_date",

    # Validation
    "validate_sales_data",
    "validate_sales_file",

    # Save/write
    "save_to_csv",
    "save_late_arriving_to_csv",
    "save_backfill_to_csv",

    # Checksum helpers
    "calculate_file_sha256",
    "get_csv_row_count",
    "calculate_s3_object_sha256",
    "save_checksum_manifest",

    # Upload
    "upload_file_to_s3",
    "upload_to_s3",
    "upload_backfill_to_s3",

    # Airflow-friendly wrappers
    "generate_sales_file",
]


def find_top_level_functions(lines: list[str]) -> list[tuple[str, int, int]]:
    """
    Find top-level function blocks in a Python file.

    Returns:
        List of tuples: (function_name, start_line_index, end_line_index)
    """

    function_starts: list[tuple[str, int]] = []

    pattern = re.compile(r"^def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(")

    for index, line in enumerate(lines):
        match = pattern.match(line)

        if match:
            function_starts.append((match.group(1), index))

    function_blocks: list[tuple[str, int, int]] = []

    for index, (function_name, start_line) in enumerate(function_starts):
        if index + 1 < len(function_starts):
            end_line = function_starts[index + 1][1]
        else:
            end_line = len(lines)

        function_blocks.append((function_name, start_line, end_line))

    return function_blocks


def main() -> None:
    if not TARGET_FILE.exists():
        raise FileNotFoundError(f"File not found: {TARGET_FILE}")

    backup_file = TARGET_FILE.with_suffix(".py.bak")
    shutil.copyfile(TARGET_FILE, backup_file)

    lines = TARGET_FILE.read_text(encoding="utf-8").splitlines(keepends=True)

    function_blocks = find_top_level_functions(lines)

    if not function_blocks:
        raise RuntimeError("No top-level functions found.")

    first_function_start = function_blocks[0][1]
    header = lines[:first_function_start]

    functions_by_name: dict[str, list[str]] = {}
    original_order: list[str] = []

    for function_name, start_line, end_line in function_blocks:
        if function_name in functions_by_name:
            raise RuntimeError(
                f"Duplicate function found: {function_name}. "
                "Please remove duplicates before reordering."
            )

        functions_by_name[function_name] = lines[start_line:end_line]
        original_order.append(function_name)

    reordered_lines: list[str] = []
    reordered_lines.extend(header)

    added_functions: set[str] = set()

    for function_name in DESIRED_FUNCTION_ORDER:
        if function_name in functions_by_name:
            reordered_lines.extend(functions_by_name[function_name])
            added_functions.add(function_name)

            if reordered_lines and not reordered_lines[-1].endswith("\n"):
                reordered_lines[-1] = reordered_lines[-1] + "\n"

            reordered_lines.append("\n")

    for function_name in original_order:
        if function_name not in added_functions:
            reordered_lines.extend(functions_by_name[function_name])
            added_functions.add(function_name)

            if reordered_lines and not reordered_lines[-1].endswith("\n"):
                reordered_lines[-1] = reordered_lines[-1] + "\n"

            reordered_lines.append("\n")

    TARGET_FILE.write_text("".join(reordered_lines).rstrip() + "\n", encoding="utf-8")

    print(f"Reordered functions in: {TARGET_FILE}")
    print(f"Backup created at: {backup_file}")


if __name__ == "__main__":
    main()