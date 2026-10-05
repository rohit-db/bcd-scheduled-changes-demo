"""A small, dependency-free JSON-Logic evaluator (https://jsonlogic.com) for trip rules.

Supported: var, missing, ==, !=, ===, !==, <, >, <=, >=, !, !!, and, or, if, in, some, all, none, cat.
Differences from the reference: equality is Python equality (no type coercion) and any
comparison involving None is False instead of raising — rules must never crash the stream.
"""

OPERATORS = frozenset({"var", "missing", "==", "!=", "===", "!==", "<", ">", "<=", ">=", "!", "!!",
                       "and", "or", "if", "in", "some", "all", "none", "cat"})


def truthy(value) -> bool:
    return bool(value)


def _var(data, path, default=None):
    if path in ("", None):
        return data
    current = data
    for part in str(path).split("."):
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and part.isdigit() and int(part) < len(current):
            current = current[int(part)]
        else:
            return default
        if current is None:
            return default
    return current


def _compare(op, values):
    if any(v is None for v in values):
        return False
    try:
        pairs = list(zip(values, values[1:]))
        if op == "<":
            return all(a < b for a, b in pairs)
        if op == ">":
            return all(a > b for a, b in pairs)
        if op == "<=":
            return all(a <= b for a, b in pairs)
        return all(a >= b for a, b in pairs)
    except TypeError:
        return False


def apply(logic, data):
    if isinstance(logic, list):
        return [apply(item, data) for item in logic]
    if not isinstance(logic, dict) or len(logic) != 1:
        return logic
    op, args = next(iter(logic.items()))
    if op not in OPERATORS:
        raise ValueError(f"unknown operator: {op}")
    if not isinstance(args, list):
        args = [args]

    if op in ("some", "all", "none"):
        items = apply(args[0], data) or []
        results = [truthy(apply(args[1], item)) for item in items]
        if op == "some":
            return any(results)
        if op == "all":
            return bool(results) and all(results)
        return not any(results)
    if op == "and":
        value = True
        for arg in args:
            value = apply(arg, data)
            if not truthy(value):
                return value
        return value
    if op == "or":
        value = False
        for arg in args:
            value = apply(arg, data)
            if truthy(value):
                return value
        return value
    if op == "if":
        for i in range(0, len(args) - 1, 2):
            if truthy(apply(args[i], data)):
                return apply(args[i + 1], data)
        return apply(args[-1], data) if len(args) % 2 else None

    values = [apply(arg, data) for arg in args]
    if op == "var":
        return _var(data, values[0] if values else "", values[1] if len(values) > 1 else None)
    if op == "missing":
        keys = values[0] if len(values) == 1 and isinstance(values[0], list) else values
        return [k for k in keys if _var(data, k) in (None, "")]
    if op in ("==", "==="):
        return values[0] == values[1]
    if op in ("!=", "!=="):
        return values[0] != values[1]
    if op in ("<", ">", "<=", ">="):
        return _compare(op, values)
    if op == "!":
        return not truthy(values[0])
    if op == "!!":
        return truthy(values[0])
    if op == "in":
        needle, haystack = values[0], values[1]
        return haystack is not None and needle in haystack
    return "".join("" if v is None else str(v) for v in values)  # cat


def validate(logic) -> None:
    if isinstance(logic, list):
        for item in logic:
            validate(item)
        return
    if isinstance(logic, dict):
        if len(logic) != 1:
            raise ValueError(f"operator objects must have exactly one key, got {sorted(logic)}")
        op, args = next(iter(logic.items()))
        if op not in OPERATORS:
            raise ValueError(f"unknown operator: {op}")
        validate(args)
