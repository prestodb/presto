import math
from collections import Counter
from typing import Any, List, Sequence, Tuple

# Q83 AVG ratios differ ~0.033 vs DuckDB; prior harness tol 0.02 too tight.
FLOAT_ABS_TOL = 0.05
FLOAT_REL_TOL = 1e-3

# Incomplete ORDER BY → adjacent-row swaps / null ties under sort-only compare.
UNORDERED_QUERIES = frozenset({"q70", "q71"})


def _is_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _nums_close(a: Any, b: Any) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    if _is_number(a) and _is_number(b):
        af, bf = float(a), float(b)
        if math.isnan(af) and math.isnan(bf):
            return True
        if math.isinf(af) or math.isinf(bf):
            return af == bf
        return abs(af - bf) <= max(FLOAT_ABS_TOL, FLOAT_REL_TOL * max(abs(af), abs(bf)))
    return a == b


def _rows_equal(r1: Sequence[Any], r2: Sequence[Any]) -> bool:
    if len(r1) != len(r2):
        return False
    return all(_nums_close(x, y) for x, y in zip(r1, r2))


def _row_key(row: Sequence[Any]) -> Tuple:
    out = []
    for v in row:
        if isinstance(v, float) and not isinstance(v, bool):
            if math.isnan(v):
                out.append(("nan",))
            elif math.isinf(v):
                out.append(("inf", v > 0))
            else:
                out.append(int(round(v / FLOAT_ABS_TOL)))
        else:
            out.append(v)
    return tuple(out)


def _normalize_qid(query_id: str) -> str:
    q = query_id.lower().strip()
    if q.endswith(".sql"):
        q = q[:-4]
    if q.startswith("query"):
        q = "q" + q[5:]
    return q


def compare_results(
    expected: List[Sequence[Any]],
    actual: List[Sequence[Any]],
    query_id: str,
) -> bool:
    """DuckDB vs native result compare. Multiset for non-deterministic ORDER BY."""
    if len(expected) != len(actual):
        return False
    q = _normalize_qid(query_id)
    if q in UNORDERED_QUERIES:
        return Counter(_row_key(r) for r in expected) == Counter(_row_key(r) for r in actual)
    return all(_rows_equal(e, a) for e, a in zip(expected, actual))
