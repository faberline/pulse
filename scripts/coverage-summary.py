"""Print all first-party LCOV entries, including unexecuted desktop/CLI code."""
import pathlib
import sys

totals = {key: 0 for key in ("LF", "LH", "FNF", "FNH", "BRF", "BRH")}
current = {}
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    if line == "end_of_record":
        lines = int(current.get("LF", 0))
        hit = int(current.get("LH", 0))
        print(f"{current['SF']}: {hit}/{lines} lines ({100 * hit / lines if lines else 0:.2f}%)")
        for key in totals:
            totals[key] += int(current.get(key, 0))
        current = {}
    elif ":" in line:
        key, value = line.split(":", 1)
        if key == "SF" or key in totals:
            current[key] = value
print(f"TOTAL lines {totals['LH']}/{totals['LF']}; functions {totals['FNH']}/{totals['FNF']}")
print(f"Branches {totals['BRH']}/{totals['BRF']} (zero denominator means no branch instrumentation)")
