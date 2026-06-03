"""
SQL Dialect Translation Engine
Converts Oracle/MSSQL SQL to ANSI/Spark SQL for cross-database compatibility.
Only used when source and target database types differ.
"""
import re

# Oracle → ANSI/Spark SQL patterns
ORACLE_TO_SPARK = [
    (re.compile(r"\bNVL2\s*\(\s*([^,]+?)\s*,\s*([^,]+?)\s*,\s*([^)]+?)\s*\)", re.IGNORECASE),
     r"CASE WHEN \1 IS NOT NULL THEN \2 ELSE \3 END"),
    (re.compile(r"\bNVL\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)", re.IGNORECASE),
     r"COALESCE(\1, \2)"),
    (re.compile(r"\bSYSDATE\b", re.IGNORECASE), "CURRENT_DATE"),
    (re.compile(r"\bSYSTIMESTAMP\b", re.IGNORECASE), "CURRENT_TIMESTAMP"),
    (re.compile(r"\bTRUNC\s*\(\s*([^)]+?)\s*\)", re.IGNORECASE),
     r"CAST(\1 AS DATE)"),
    (re.compile(r"\bTO_CHAR\s*\(\s*([^,]+?)\s*,\s*'([^']+?)'\s*\)", re.IGNORECASE),
     r"DATE_FORMAT(\1, '\2')"),
    (re.compile(r"\bTO_DATE\s*\(\s*([^,]+?)\s*,\s*'([^']+?)'\s*\)", re.IGNORECASE),
     r"TO_DATE(\1, '\2')"),
    (re.compile(r"\bADD_MONTHS\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)", re.IGNORECASE),
     r"ADD_MONTHS(\1, \2)"),
]

# Oracle outer join patterns: t1.col = t2.col (+)
_ORACLE_LEFT_JOIN = re.compile(
    r"(\w+\.\w+)\s*=\s*(\w+\.\w+)\s*\(\+\)"
)
_ORACLE_RIGHT_JOIN = re.compile(
    r"(\w+\.\w+)\s*\(\+\)\s*=\s*(\w+\.\w+)"
)

# Oracle ROWNUM
_ROWNUM_RE = re.compile(
    r"\bAND\s+ROWNUM\s*<=?\s*(\d+)\b|\bWHERE\s+ROWNUM\s*<=?\s*(\d+)\b",
    re.IGNORECASE
)

# DECODE function
_DECODE_RE = re.compile(r"\bDECODE\s*\(", re.IGNORECASE)

# Oracle implicit joins (FROM table1, table2 WHERE ...)
_IMPLICIT_JOIN_RE = re.compile(
    r"FROM\s+(.+?)\s+WHERE\s+",
    re.IGNORECASE | re.DOTALL
)


def _split_args(s):
    """Split function arguments respecting nested parentheses and quotes."""
    args = []
    depth = 0
    in_quote = None
    current = []
    for ch in s:
        if in_quote:
            current.append(ch)
            if ch == in_quote:
                in_quote = None
        elif ch in ("'", '"'):
            in_quote = ch
            current.append(ch)
        elif ch == '(':
            depth += 1
            current.append(ch)
        elif ch == ')':
            depth -= 1
            current.append(ch)
        elif ch == ',' and depth == 0:
            args.append(''.join(current))
            current = []
        else:
            current.append(ch)
    if current:
        args.append(''.join(current))
    return args


def _convert_decode(sql):
    """Convert Oracle DECODE to CASE WHEN."""
    result = sql
    idx = 0
    while True:
        m = _DECODE_RE.search(result, idx)
        if not m:
            break
        start = m.start()
        paren_start = m.end() - 1
        depth = 1
        pos = paren_start + 1
        while pos < len(result) and depth > 0:
            if result[pos] == '(':
                depth += 1
            elif result[pos] == ')':
                depth -= 1
            pos += 1
        if depth != 0:
            idx = pos
            continue
        inner = result[paren_start + 1:pos - 1]
        args = _split_args(inner)
        if len(args) < 3:
            idx = pos
            continue
        expr = args[0].strip()
        pairs = args[1:]
        case_parts = [f"CASE {expr}"]
        i = 0
        while i < len(pairs) - 1:
            case_parts.append(f" WHEN {pairs[i].strip()} THEN {pairs[i+1].strip()}")
            i += 2
        if i < len(pairs):
            case_parts.append(f" ELSE {pairs[i].strip()}")
        case_parts.append(" END")
        replacement = "".join(case_parts)
        result = result[:start] + replacement + result[pos:]
        idx = start + len(replacement)
    return result


def detect_sql_dialect(sql):
    """Detect SQL dialect (oracle/mssql/generic) from SQL text."""
    if not sql:
        return "generic"

    oracle_indicators = [
        r"\bTO_DATE\b", r"\bADD_MONTHS\b", r"\bSYSDATE\b",
        r"\bNVL\b", r"\bDECODE\b", r"\bTRUNC\b",
        r"\(\+\)", r"\bROWNUM\b",
    ]
    mssql_indicators = [
        r"\bGETDATE\b", r"\bISNULL\b", r"\bCHARINDEX\b",
        r"\bCONVERT\s*\(", r"\bTOP\s+\d+",
    ]

    oracle_score = sum(1 for p in oracle_indicators if re.search(p, sql, re.IGNORECASE))
    mssql_score = sum(1 for p in mssql_indicators if re.search(p, sql, re.IGNORECASE))

    if oracle_score > mssql_score:
        return "oracle"
    elif mssql_score > oracle_score:
        return "mssql"
    return "generic"


def translate_sql(sql, source_dialect="auto", target_dialect="spark"):
    """
    Translate SQL from source dialect to target dialect.
    
    Args:
        sql: SQL query to translate
        source_dialect: Source dialect ('oracle', 'mssql', 'generic', or 'auto')
        target_dialect: Target dialect ('spark', 'ansi')
    
    Returns:
        Translated SQL string
    """
    if not sql or not sql.strip():
        return sql

    # Preserve the original for comparison
    original = sql

    if source_dialect == "auto":
        source_dialect = detect_sql_dialect(sql)

    # Only translate if dialects differ
    if source_dialect == target_dialect:
        return sql

    translated = sql

    if source_dialect == "oracle":
        # Convert outer join syntax
        translated = _ORACLE_LEFT_JOIN.sub(
            lambda m: f'{m.group(1)} = {m.group(2)} -- (+) converted: verify LEFT JOIN',
            translated
        )
        translated = _ORACLE_RIGHT_JOIN.sub(
            lambda m: f'{m.group(1)} = {m.group(2)} -- (+) converted: verify RIGHT JOIN',
            translated
        )
        # Convert DECODE
        translated = _convert_decode(translated)
        # Convert ROWNUM
        rownum_m = _ROWNUM_RE.search(translated)
        if rownum_m:
            limit_val = rownum_m.group(1) or rownum_m.group(2)
            cleaned = _ROWNUM_RE.sub('', translated).strip()
            if cleaned.endswith('AND'):
                cleaned = cleaned[:-3].strip()
            if cleaned.endswith('WHERE'):
                cleaned = cleaned[:-5].strip()
            cleaned = cleaned.rstrip(';')
            translated = f"{cleaned}\nLIMIT {limit_val}"
        # Apply Oracle→Spark function mapping
        for pattern, replacement in ORACLE_TO_SPARK:
            translated = pattern.sub(replacement, translated)

    elif source_dialect == "mssql" and target_dialect != "mssql":
        # MSSQL→Spark translations
        translated = re.sub(r"\bISNULL\s*\(([^,]+),([^)]+)\)", r"COALESCE(\1, \2)", translated, flags=re.IGNORECASE)
        translated = re.sub(r"\bGETDATE\s*\(\)", "CURRENT_TIMESTAMP", translated, flags=re.IGNORECASE)

    return translated
