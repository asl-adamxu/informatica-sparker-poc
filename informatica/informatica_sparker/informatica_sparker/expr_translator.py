import re
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from .logger import ConversionLogger


def sanitize_for_expr(expr: str) -> str:
    if not expr:
        return ""

    result = expr.replace('\\', '\\\\')
    result = result.replace('"', '\\"')
    result = re.sub(r'""([^"]*?)""', r'"\1"', result)

    return result


def wrap_with_expr(condition: str) -> str:
    if not condition or condition.strip().lower() in ('true', 'false'):
        # Wrap bare True/False in expr() so filter(expr("TRUE")) works in all
        # contexts (bare True works in filter() but bare TRUE fails in Router
        # templates where string casing may differ from Python builtins).
        _lower = condition.strip().lower()
        if _lower == 'true':
            return 'expr("TRUE")'
        if _lower == 'false':
            return 'expr("FALSE")'
        return condition

    sanitized = sanitize_for_expr(condition)
    return f'expr("{sanitized}")'


class ExpressionTranslator:

    FUNCTION_MAP = {
        'NVL': 'coalesce',
        'NVL2': None,
        'NULLIF': 'nullif',
        'IS_SPACES': "trim({}) = ''",
        'IS_NUMBER': 'cast({} as double) is not null',
        'TO_CHAR': 'cast({} as string)',
        'TO_DATE': 'to_date',
        'TO_DECIMAL': 'cast({} as decimal)',
        'TO_NUMBER': 'cast({} as decimal)',
        'TO_INTEGER': 'cast({} as int)',
        'TO_BIGINT': 'cast({} as bigint)',
        'TO_FLOAT': 'cast({} as float)',
        'SUBSTR': 'substring',
        'INSTR': 'instr',
        'LTRIM': 'ltrim',
        'RTRIM': 'rtrim',
        'TRIM': 'trim',
        'UPPER': 'upper',
        'LOWER': 'lower',
        'LENGTH': 'length',
        'LPAD': 'lpad',
        'RPAD': 'rpad',
        'CONCAT': 'concat',
        'REPLACE': 'replace',
        'REG_REPLACE': 'regexp_replace',
        'REG_MATCH': 'regexp_extract',
        'REG_EXTRACT': 'regexp_extract',
        'INITCAP': 'initcap',
        'REVERSE': 'reverse',
        'CHR': 'chr',
        'ASCII': 'ascii',
        'SOUNDEX': 'soundex',
        'REPLACESTR': 'regexp_replace',
        # REPLACECHR handled by _translate_replacechr (4 args, start_pos dropped)
        'ROUND': 'round',
        'TRUNC': 'floor',
        'ABS': 'abs',
        'MOD': 'mod',
        'POWER': 'pow',
        'SQRT': 'sqrt',
        'FLOOR': 'floor',
        'CEIL': 'ceil',
        'CEILING': 'ceil',
        'LOG': 'log',
        'LN': 'ln',
        'EXP': 'exp',
        'SIGN': 'sign',
        'SIN': 'sin',
        'COS': 'cos',
        'TAN': 'tan',
        'ASIN': 'asin',
        'ACOS': 'acos',
        'ATAN': 'atan',
        'RAND': 'rand',
        'SYSDATE': 'current_timestamp()',
        'SYSTIMESTAMP': 'current_timestamp()',
        'SESSSTARTTIME': 'current_timestamp()',
        'ADD_TO_DATE': 'date_add',
        'DATE_DIFF': None,  # handled by _translate_date_diff
        'GET_DATE_PART': 'date_part',
        'SET_DATE_PART': None,
        'LAST_DAY': 'last_day',
        'MAKE_DATE_TIME': 'to_timestamp',
        'NEXT_DAY': 'next_day',
        'MONTHS_BETWEEN': 'months_between',
        'ADD_MONTHS': 'add_months',
        'SUM': 'sum',
        'COUNT': 'count',
        'AVG': 'avg',
        'MIN': 'min',
        'MAX': 'max',
        'FIRST': 'first',
        'LAST': 'last',
        'MEDIAN': 'percentile_approx({}, 0.5)',
        'STDDEV': 'stddev',
        'VARIANCE': 'variance',
        'PERCENTILE': 'percentile_approx',
        'LOOKUP': None,
        'GREATEST': 'greatest',
        'LEAST': 'least',
        'COALESCE': 'coalesce',
    }

    def __init__(self, mapping_name: str = "", pm_variables: Dict[str, str] = None, logger: Optional["ConversionLogger"] = None):
        self.mapping_name = mapping_name
        self.pm_variables = pm_variables or {}
        self.warnings: List[str] = []
        self.logger = logger

    def translate(self, expression: str, context: str = "column", field_name: str = "") -> str:
        if not expression:
            return ""

        original = expression
        result = expression

        result = self._replace_pm_variables(result)
        result = self._translate_lkp_references(result)
        result = self._translate_isnull(result)
        result = self._translate_in_function(result)
        result = self._translate_instr(result)
        result = self._translate_is_number(result)
        result = self._translate_is_date(result)
        result = self._translate_nvl2(result)
        result = self._translate_iif(result)
        result = self._translate_decode(result)
        result = self._translate_get_date_part(result)
        result = self._translate_set_date_part(result)
        result = self._translate_date_diff(result)
        result = self._translate_add_to_date(result)
        result = self._translate_date_compare(result)
        result = self._translate_error_abort(result)
        result = self._translate_cume(result)
        result = self._translate_movingsum(result)
        result = self._translate_movingavg(result)
        result = self._translate_replacechr(result)
        result = self._translate_date_trunc(result)
        result = self._translate_functions(result)
        result = self._translate_operators(result)
        result = self._translate_string_literals(result)
        result = self._translate_date_format_patterns(result)
        result = self._translate_dd_keywords(result)
        result = self._clean_up(result)

        if self.logger and field_name:
            from .logger import LogLevel
            if result != original:
                self.logger.log_expression(field_name, original, result, LogLevel.SUCCESS)
            else:
                self.logger.log_expression(field_name, original, result, LogLevel.INFO, "Expression unchanged (already PySpark compatible)")

        return result

    def translate_for_filter(self, expression: str, field_name: str = "") -> str:
        if not expression:
            translated = "True"
        else:
            # Temporarily remove user-defined $$ mapping variables so they're not
            # replaced with hardcoded default values by _replace_pm_variables.
            # $PM system variables (e.g. $PMMappingName) are kept as-is.
            _saved = {}
            for _k in list(self.pm_variables.keys()):
                if _k.startswith('$$'):
                    _saved[_k] = self.pm_variables.pop(_k)
            try:
                translated = self.translate(expression, "filter", field_name)
            finally:
                self.pm_variables.update(_saved)

        # Always route through wrap_with_expr so that bare True/False become
        # expr("TRUE")/expr("FALSE") — these work in all filter() contexts
        # (including Router templates) unlike bare Python True/False.
        return wrap_with_expr(translated)

    def _translate_isnull(self, expr: str) -> str:
        pattern = re.compile(r'\bISNULL\s*\(\s*([^)]+)\s*\)', re.IGNORECASE)

        def replace_isnull(match):
            arg = match.group(1).strip()
            return f'({arg} IS NULL)'

        return pattern.sub(replace_isnull, expr)

    def _translate_in_function(self, expr: str) -> str:
        marker = "__SPARK_IN__"
        pattern = re.compile(r'\bIN\s*\(', re.IGNORECASE)

        max_iterations = 100
        iteration = 0

        while pattern.search(expr) and iteration < max_iterations:
            iteration += 1
            match = pattern.search(expr)
            if not match:
                break

            args = self._extract_function_args(expr, match.end() - 1)

            if len(args) >= 2:
                test_value = args[0]
                list_items = args[1:]
                sql_expr = f"({test_value} {marker}({', '.join(list_items)}))"

                full_match = expr[match.start():self._find_closing_paren(expr, match.end() - 1) + 1]
                expr = expr.replace(full_match, sql_expr, 1)
            else:
                break

        expr = expr.replace(marker, "IN ")
        return expr

    def _translate_instr(self, expr: str) -> str:
        pattern = re.compile(r'\bINSTR\s*\(', re.IGNORECASE)

        max_iterations = 100
        iteration = 0

        while pattern.search(expr) and iteration < max_iterations:
            iteration += 1
            match = pattern.search(expr)
            if not match:
                break

            args = self._extract_function_args(expr, match.end() - 1)

            if len(args) >= 2:
                string_arg = args[0]
                substring_arg = args[1]
                sql_expr = f"instr({string_arg}, {substring_arg})"

                full_match = expr[match.start():self._find_closing_paren(expr, match.end() - 1) + 1]
                expr = expr.replace(full_match, sql_expr, 1)
            else:
                break

        return expr

    def _translate_is_number(self, expr: str) -> str:
        pattern = re.compile(r'\bIS_NUMBER\s*\(\s*([^)]+)\s*\)', re.IGNORECASE)

        def replace_is_number(match):
            arg = match.group(1).strip()
            return f"(CAST({arg} AS DOUBLE) IS NOT NULL AND TRIM({arg}) != '')"

        return pattern.sub(replace_is_number, expr)

    def _translate_is_date(self, expr: str) -> str:
        pattern = re.compile(r'\bIS_DATE\s*\(', re.IGNORECASE)

        while pattern.search(expr):
            match = pattern.search(expr)
            if not match:
                break

            args = self._extract_function_args(expr, match.end() - 1)
            if args:
                date_val = args[0]
                format_str = args[1] if len(args) > 1 else "'yyyy-MM-dd'"
                sql_expr = f"(to_date({date_val}, {format_str}) IS NOT NULL)"

                full_match = expr[match.start():self._find_closing_paren(expr, match.end() - 1) + 1]
                expr = expr.replace(full_match, sql_expr, 1)
            else:
                break

        return expr

    def _translate_nvl2(self, expr: str) -> str:
        pattern = re.compile(r'\bNVL2\s*\(', re.IGNORECASE)

        while pattern.search(expr):
            match = pattern.search(expr)
            if not match:
                break

            args = self._extract_function_args(expr, match.end() - 1)
            if len(args) >= 3:
                test_val = args[0]
                not_null_val = args[1]
                null_val = args[2]
                sql_expr = f"CASE WHEN {test_val} IS NOT NULL THEN {not_null_val} ELSE {null_val} END"

                full_match = expr[match.start():self._find_closing_paren(expr, match.end() - 1) + 1]
                expr = expr.replace(full_match, sql_expr, 1)
            else:
                break

        return expr

    def _translate_get_date_part(self, expr: str) -> str:
        pattern = re.compile(r'\bGET_DATE_PART\s*\(', re.IGNORECASE)

        while pattern.search(expr):
            match = pattern.search(expr)
            if not match:
                break

            args = self._extract_function_args(expr, match.end() - 1)
            if len(args) >= 2:
                date_val = args[0]
                part = args[1].strip().strip("'\"").upper()

                part_map = {
                    'YEAR': 'year', 'YY': 'year', 'YYYY': 'year',
                    'MONTH': 'month', 'MM': 'month', 'MON': 'month',
                    'DAY': 'day', 'DD': 'day', 'D': 'dayofweek', 'DDD': 'dayofyear',
                    'HOUR': 'hour', 'HH': 'hour', 'HH24': 'hour',
                    'MINUTE': 'minute', 'MI': 'minute',
                    'SECOND': 'second', 'SS': 'second',
                    'WEEK': 'weekofyear', 'WW': 'weekofyear',
                    'QUARTER': 'quarter', 'Q': 'quarter'
                }

                spark_func = part_map.get(part, 'day')
                sql_expr = f"{spark_func}({date_val})"

                full_match = expr[match.start():self._find_closing_paren(expr, match.end() - 1) + 1]
                expr = expr.replace(full_match, sql_expr, 1)
            else:
                break

        return expr

    def _translate_set_date_part(self, expr: str) -> str:
        pattern = re.compile(r'\bSET_DATE_PART\s*\(', re.IGNORECASE)

        while pattern.search(expr):
            match = pattern.search(expr)
            if not match:
                break

            args = self._extract_function_args(expr, match.end() - 1)
            if len(args) >= 3:
                date_val = args[0]
                part = args[1].strip().strip("'\"").upper()
                new_val = args[2]

                if part in ('YEAR', 'YY', 'YYYY'):
                    sql_expr = f"add_months({date_val}, ({new_val} - year({date_val})) * 12)"
                elif part in ('MONTH', 'MM', 'MON'):
                    sql_expr = f"add_months({date_val}, {new_val} - month({date_val}))"
                elif part in ('DAY', 'DD', 'D'):
                    sql_expr = f"date_add({date_val}, {new_val} - day({date_val}))"
                else:
                    sql_expr = date_val
                    self.warnings.append(f"SET_DATE_PART with {part} not fully supported")

                full_match = expr[match.start():self._find_closing_paren(expr, match.end() - 1) + 1]
                expr = expr.replace(full_match, sql_expr, 1)
            else:
                break

        return expr

    def _translate_date_diff(self, expr: str) -> str:
        """Translate DATE_DIFF(date1, date2, format) → datediff(date1, date2).
        Spark's datediff only accepts 2 args (no format specifier).
        """
        pattern = re.compile(r'\bDATE_DIFF\s*\(', re.IGNORECASE)
        while pattern.search(expr):
            match = pattern.search(expr)
            if not match:
                break
            _all_args = self._extract_function_args(expr, match.end() - 1)
            if _all_args and len(_all_args) >= 2:
                if len(_all_args) >= 3:
                    # Strip the 3rd arg (format specifier like 'D', 'M', 'Y')
                    _all_args = _all_args[:2]
                new_call = f"datediff({', '.join(_all_args)})"
                full_match = expr[match.start():self._find_closing_paren(expr, match.end() - 1) + 1]
                expr = expr.replace(full_match, new_call, 1)
            else:
                break
        return expr

    def _translate_add_to_date(self, expr: str) -> str:
        pattern = re.compile(r'\bADD_TO_DATE\s*\(', re.IGNORECASE)

        while pattern.search(expr):
            match = pattern.search(expr)
            if not match:
                break

            args = self._extract_function_args(expr, match.end() - 1)
            if len(args) >= 3:
                date_val = args[0]
                part = args[1].strip().strip("'\"").upper()
                amount = args[2]

                if part in ('YEAR', 'YY', 'YYYY'):
                    sql_expr = f"add_months({date_val}, ({amount}) * 12)"
                elif part in ('MONTH', 'MM', 'MON'):
                    sql_expr = f"add_months({date_val}, {amount})"
                elif part in ('DAY', 'DD', 'D'):
                    sql_expr = f"date_add({date_val}, CAST({amount} AS INT))"
                elif part in ('HOUR', 'HH', 'HH24'):
                    sql_expr = f"CAST({date_val} AS TIMESTAMP) + make_interval(0, 0, 0, 0, CAST({amount} AS INT), 0, 0)"
                elif part in ('MINUTE', 'MI'):
                    sql_expr = f"CAST({date_val} AS TIMESTAMP) + make_interval(0, 0, 0, 0, 0, CAST({amount} AS INT), 0)"
                elif part in ('SECOND', 'SS'):
                    sql_expr = f"CAST({date_val} AS TIMESTAMP) + make_interval(0, 0, 0, 0, 0, 0, CAST({amount} AS DOUBLE))"
                else:
                    sql_expr = f"date_add({date_val}, CAST({amount} AS INT))"

                full_match = expr[match.start():self._find_closing_paren(expr, match.end() - 1) + 1]
                expr = expr.replace(full_match, sql_expr, 1)
            else:
                break

        return expr

    def _translate_date_compare(self, expr: str) -> str:
        pattern = re.compile(r'\bDATE_COMPARE\s*\(', re.IGNORECASE)

        while pattern.search(expr):
            match = pattern.search(expr)
            if not match:
                break

            args = self._extract_function_args(expr, match.end() - 1)
            if len(args) >= 2:
                date1 = args[0]
                date2 = args[1]
                sql_expr = f"CASE WHEN {date1} < {date2} THEN -1 WHEN {date1} > {date2} THEN 1 ELSE 0 END"

                full_match = expr[match.start():self._find_closing_paren(expr, match.end() - 1) + 1]
                expr = expr.replace(full_match, sql_expr, 1)
            else:
                break

        return expr

    def _translate_error_abort(self, expr: str) -> str:
        error_pattern = re.compile(r'\bERROR\s*\(\s*([^)]+)\s*\)', re.IGNORECASE)
        abort_pattern = re.compile(r'\bABORT\s*\(\s*([^)]+)\s*\)', re.IGNORECASE)

        def replace_error(match):
            msg = match.group(1).strip()
            self.warnings.append(f"ERROR function found: {msg}")
            return f"raise_error({msg})"

        def replace_abort(match):
            msg = match.group(1).strip()
            self.warnings.append(f"ABORT function found: {msg}")
            return f"raise_error({msg})"

        expr = error_pattern.sub(replace_error, expr)
        expr = abort_pattern.sub(replace_abort, expr)

        return expr

    def _translate_cume(self, expr: str) -> str:
        pattern = re.compile(r'\bCUME\s*\(', re.IGNORECASE)

        while pattern.search(expr):
            match = pattern.search(expr)
            if not match:
                break

            args = self._extract_function_args(expr, match.end() - 1)
            if args:
                value = args[0]
                self.warnings.append(f"CUME({value}) converted - ensure ORDER BY is set in window spec at transformation level")
                sql_expr = f"sum({value}) OVER (ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"

                full_match = expr[match.start():self._find_closing_paren(expr, match.end() - 1) + 1]
                expr = expr.replace(full_match, sql_expr, 1)
            else:
                break

        return expr

    def _translate_movingsum(self, expr: str) -> str:
        pattern = re.compile(r'\bMOVINGSUM\s*\(', re.IGNORECASE)

        while pattern.search(expr):
            match = pattern.search(expr)
            if not match:
                break

            args = self._extract_function_args(expr, match.end() - 1)
            if args:
                value = args[0]
                rows = args[1].strip() if len(args) > 1 else "1"
                self.warnings.append(f"MOVINGSUM converted - ensure ORDER BY is set in window spec")
                sql_expr = f"sum({value}) OVER (ORDER BY 1 ROWS BETWEEN {rows} PRECEDING AND CURRENT ROW)"

                full_match = expr[match.start():self._find_closing_paren(expr, match.end() - 1) + 1]
                expr = expr.replace(full_match, sql_expr, 1)
            else:
                break

        return expr

    def _translate_movingavg(self, expr: str) -> str:
        pattern = re.compile(r'\bMOVINGAVG\s*\(', re.IGNORECASE)

        while pattern.search(expr):
            match = pattern.search(expr)
            if not match:
                break

            args = self._extract_function_args(expr, match.end() - 1)
            if args:
                value = args[0]
                rows = args[1].strip() if len(args) > 1 else "1"
                self.warnings.append(f"MOVINGAVG converted - ensure ORDER BY is set in window spec")
                sql_expr = f"avg({value}) OVER (ORDER BY 1 ROWS BETWEEN {rows} PRECEDING AND CURRENT ROW)"

                full_match = expr[match.start():self._find_closing_paren(expr, match.end() - 1) + 1]
                expr = expr.replace(full_match, sql_expr, 1)
            else:
                break

        return expr

    def _translate_replacechr(self, expr: str) -> str:
        """Translate Informatica REPLACECHR(start, string, from, to) to PySpark
        translate(string, from, to).

        REPLACECHR has 4 args (start_pos is Informatica-specific and dropped):
          REPLACECHR(0, col, ' ', NULL)  ->  translate(col, ' ', '')
        When to_chars is NULL/empty, use '' (remove matched characters).
        """
        pattern = re.compile(r'\bREPLACECHR\s*\(', re.IGNORECASE)
        max_iterations = 100
        iteration = 0
        while pattern.search(expr) and iteration < max_iterations:
            iteration += 1
            match = pattern.search(expr)
            if not match:
                break
            args = self._extract_function_args(expr, match.end() - 1)
            if len(args) == 4:
                _str = args[1]
                _from = args[2]
                _to = args[3]
                if _to.upper() == 'NULL' or _to.strip() == "''":
                    _to = "''"
                replacement = f'translate({_str}, {_from}, {_to})'
                full_match = expr[match.start():self._find_closing_paren(expr, match.end() - 1) + 1]
                expr = expr.replace(full_match, replacement, 1)
            elif len(args) > 1:
                replacement = f'translate({", ".join(args[1:])})'
                full_match = expr[match.start():self._find_closing_paren(expr, match.end() - 1) + 1]
                expr = expr.replace(full_match, replacement, 1)
            else:
                break
        return expr

    def _translate_date_format_patterns(self, expr: str) -> str:
        """Fix Informatica date format patterns in date-related function calls.
        Spark 3+ uses Java 8 DateTimeFormatter which is stricter:
          YYYY (week-year) → yyyy (year)
          DD (day-of-year) → dd (day-of-month)
          MI (minute)      → mm
          SS               → ss
          MON              → MMM
          MONTH            → MMMM
        Applies to single-quoted format strings in to_date/to_char/date_format.
        Uses _extract_function_args to handle nested parentheses.
        """
        result = expr
        _funcs = ['to_date', 'to_char', 'date_format']
        for _func in _funcs:
            _pat = re.compile(r'\b' + _func + r'\s*\(', re.IGNORECASE)
            for _m in list(_pat.finditer(result)):
                _args = self._extract_function_args(result, _m.end() - 1)
                if _args and len(_args) >= 2:
                    _fmt = _args[-1].strip()
                    if _fmt.startswith("'") and _fmt.endswith("'"):
                        _inner = _fmt[1:-1]
                        _new = _inner
                        _new = _new.replace('YYYY', 'yyyy')
                        _new = _new.replace('YY', 'yy')
                        _new = _new.replace('DD', 'dd')
                        _new = _new.replace('MI', 'mm')
                        _new = _new.replace('SS', 'ss')
                        _new = _new.replace('MONTH', 'MMMM')
                        _new = _new.replace('MON', 'MMM')
                        if _new != _inner:
                            result = result.replace("'" + _inner + "'", "'" + _new + "'", 1)
        return result

    def _translate_string_literals(self, expr: str) -> str:
        return expr

    def _translate_lkp_references(self, expr: str) -> str:
        pattern = re.compile(r':LKP\.(\w+)\s*\(\s*([^)]+)\s*\)', re.IGNORECASE)

        def replace_lkp(match):
            lookup_name = match.group(1)
            column_name = match.group(2).strip()
            self.warnings.append(f"Replaced :LKP.{lookup_name}({column_name}) with direct column reference")
            return column_name

        return pattern.sub(replace_lkp, expr)

    def _replace_pm_variables(self, expr: str) -> str:
        result = expr

        safe_mapping_name = self.mapping_name.replace("'", "''")

        result = re.sub(r'\$PMMappingName', f"'{safe_mapping_name}'", result, flags=re.IGNORECASE)
        result = re.sub(r'\$PMWorkflowName', f"'{safe_mapping_name}'", result, flags=re.IGNORECASE)
        result = re.sub(r'\$PMSessionName', f"'{safe_mapping_name}'", result, flags=re.IGNORECASE)
        result = re.sub(r'\$PMFolderName', "'default'", result, flags=re.IGNORECASE)

        for var_name, var_value in self.pm_variables.items():
            if var_value:
                safe_value = str(var_value).replace("'", "''")
                result = re.sub(re.escape(var_name), f"'{safe_value}'", result, flags=re.IGNORECASE)
            else:
                safe_var = var_name.replace("'", "''")
                result = re.sub(re.escape(var_name), f"'{safe_var}'", result, flags=re.IGNORECASE)

        remaining_pm = re.findall(r'\$PM[A-Za-z_][A-Za-z0-9_]*', result)
        for pm_var in remaining_pm:
            self.warnings.append(f"Unresolved PM variable: {pm_var}")
            safe_pm = pm_var.replace("'", "''")
            result = result.replace(pm_var, f"'{safe_pm}'")

        # Handle $$ mapping variables (e.g., $$v_snsh_date)
        # If value is known, substitute directly; otherwise keep the marker for
        # template-level substitution (handled in mapping.py.j2).
        remaining_global = re.findall(r'\$\$[A-Za-z_][A-Za-z0-9_]*', result)
        for var_name in remaining_global:
            var_value = self.pm_variables.get(var_name, "")
            if var_value:
                safe_value = str(var_value).replace("'", "''")
                result = result.replace(var_name, f"'{safe_value}'")
            else:
                # Keep $$var_name as-is; the template will do Python-level
                # .replace() to substitute the runtime variable value.
                pass

        return result

    def _translate_iif(self, expr: str) -> str:
        pattern = re.compile(r'IIF\s*\(', re.IGNORECASE)

        while pattern.search(expr):
            match = pattern.search(expr)
            if not match:
                break

            start = match.end()
            args = self._extract_function_args(expr, start - 1)

            if len(args) >= 2:
                condition = args[0]
                true_val = args[1]
                false_val = args[2] if len(args) > 2 else "NULL"

                sql_expr = f"CASE WHEN {condition} THEN {true_val} ELSE {false_val} END"

                full_match = expr[match.start():self._find_closing_paren(expr, match.end() - 1) + 1]
                expr = expr.replace(full_match, sql_expr, 1)
            else:
                break

        return expr

    def _translate_decode(self, expr: str) -> str:
        pattern = re.compile(r'DECODE\s*\(', re.IGNORECASE)

        while pattern.search(expr):
            match = pattern.search(expr)
            if not match:
                break

            start = match.end()
            args = self._extract_function_args(expr, start - 1)

            if len(args) >= 3:
                test_expr = args[0]
                pairs = args[1:]

                when_clauses = []
                default_val = "NULL"

                i = 0
                while i < len(pairs) - 1:
                    search_val = pairs[i]
                    result_val = pairs[i + 1]
                    # DECODE(TRUE, cond1, val1, cond2, val2, ...) is a
                    # searched-case pattern: return the first val whose
                    # condition evaluates to TRUE.  Generate WHEN cond THEN val
                    # (without the TRUE = prefix) so that operator precedence
                    # is correct (e.g. TRUE = x > 10 would parse wrongly).
                    if test_expr.strip().upper() == 'TRUE':
                        when_clauses.append(f"WHEN {search_val} THEN {result_val}")
                    else:
                        when_clauses.append(f"WHEN {test_expr} = {search_val} THEN {result_val}")
                    i += 2

                if len(pairs) % 2 == 1:
                    default_val = pairs[-1]

                sql_expr = "CASE " + " ".join(when_clauses) + f" ELSE {default_val} END"

                full_match = expr[match.start():self._find_closing_paren(expr, match.end() - 1) + 1]
                expr = expr.replace(full_match, sql_expr, 1)
            else:
                break

        return expr

    def _translate_functions(self, expr: str) -> str:
        result = expr

        # Handle bare keyword functions that may appear without parentheses
        # (e.g. bare "SYSDATE" without "SYSDATE()").
        _bare_funcs = {
            'SYSDATE': 'current_timestamp()',
            'SYSTIMESTAMP': 'current_timestamp()',
        }
        for _bf_name, _bf_replacement in _bare_funcs.items():
            result = re.sub(r'\b' + _bf_name + r'\b(?!\s*\()', _bf_replacement, result, flags=re.IGNORECASE)

        for infa_func, spark_func in self.FUNCTION_MAP.items():
            if spark_func is None:
                continue

            pattern = re.compile(rf'\b{infa_func}\s*\(', re.IGNORECASE)

            if '{}' in spark_func:
                while pattern.search(result):
                    match = pattern.search(result)
                    if not match:
                        break

                    args = self._extract_function_args(result, match.end() - 1)
                    if args:
                        new_func = spark_func.format(args[0])
                        full_match = result[match.start():self._find_closing_paren(result, match.end() - 1) + 1]
                        result = result.replace(full_match, new_func, 1)
                    else:
                        break
            else:
                result = pattern.sub(f'{spark_func}(', result)

        return result

    def _translate_date_trunc(self, expr: str) -> str:
        """Translate Informatica TRUNC(date, 'format') and FLOOR(date, 'format')
        to Spark date_trunc('format', date).  Only fires when the second argument
        is a quoted date-part string (e.g. 'MONTH', 'YEAR'); single-argument
        TRUNC/FLOOR calls are left for the generic _translate_functions pass.

        Uses parenthesis-depth tracking to ensure the comma we split on is at
        the top level of the TRUNC/FLOOR call, not inside a nested function.
        """
        _date_parts = {
            'YEAR': 'YEAR', 'YYYY': 'YEAR', 'YY': 'YEAR',
            'MONTH': 'MONTH', 'MM': 'MONTH', 'MON': 'MONTH',
            'DAY': 'DAY', 'DD': 'DAY', 'D': 'DAY',
            'HOUR': 'HOUR', 'HH': 'HOUR', 'HH24': 'HOUR',
            'MINUTE': 'MINUTE', 'MI': 'MINUTE',
            'SECOND': 'SECOND', 'SS': 'SECOND',
            'QUARTER': 'QUARTER', 'Q': 'QUARTER',
            'WEEK': 'WEEK', 'W': 'WEEK',
        }

        # Find TRUNC( or FLOOR( positions
        _func_pat = re.compile(r'\b(TRUNC|FLOOR)\s*\(', re.IGNORECASE)
        _result = expr
        # Process from end to start so positions stay valid
        for _m in reversed(list(_func_pat.finditer(_result))):
            _func_name = _m.group(1).upper()
            _args_start = _m.end() - 1  # position of '('
            _args = self._extract_function_args(_result, _args_start)

            # Single-argument TRUNC(date) — truncate time to start of day
            if len(_args) == 1 and _func_name == 'TRUNC':
                _date_expr = _args[0].strip()
                _replacement = f"date_trunc('day', {_date_expr})"
                _full_match_end = self._find_closing_paren(_result, _args_start) + 1
                _result = _result[:_m.start()] + _replacement + _result[_full_match_end:]
            # Only convert 2-argument calls where arg 2 is a quoted date-part
            elif len(_args) == 2:
                _arg2 = _args[1].strip()
                _part_match = re.match(r"['\"]([A-Za-z0-9]+)['\"]", _arg2)
                if _part_match:
                    _date_expr = _args[0].strip()
                    _part = _part_match.group(1).upper()
                    _spark_part = _date_parts.get(_part, _part)
                    _replacement = f"date_trunc('{_spark_part}', {_date_expr})"
                    _full_match_end = self._find_closing_paren(_result, _args_start) + 1
                    _result = _result[:_m.start()] + _replacement + _result[_full_match_end:]

        return _result

    def _translate_operators(self, expr: str) -> str:
        result = expr

        result = re.sub(r'\|\|', ' || ', result)
        result = re.sub(r'\b(AND)\b', r' AND ', result, flags=re.IGNORECASE)
        result = re.sub(r'\b(OR)\b', r' OR ', result, flags=re.IGNORECASE)
        result = re.sub(r'\b(NOT)\b', r' NOT ', result, flags=re.IGNORECASE)
        result = re.sub(r'<>', '!=', result)
        result = re.sub(r'\s+', ' ', result)

        return result

    def _translate_dd_keywords(self, expr: str) -> str:
        """Convert bare Informatica DD_INSERT/DD_UPDATE/DD_DELETE to string literals.
        
        In Informatica, DD_INSERT, DD_UPDATE, DD_DELETE are built-in constants
        for update strategy operations. In PySpark, they must be quoted strings.
        Uses word-boundary matching to avoid modifying column names that happen
        to contain these substrings.
        """
        return re.sub(
            r'\b(DD_INSERT|DD_UPDATE|DD_DELETE)\b',
            r"'\1'",
            expr
        )

    def _clean_up(self, expr: str) -> str:
        result = expr
        result = re.sub(r'\s+', ' ', result)
        result = result.strip()
        return result

    def _extract_function_args(self, expr: str, start_paren: int) -> List[str]:
        if start_paren >= len(expr) or expr[start_paren] != '(':
            return []

        args = []
        current_arg = ""
        depth = 0

        for i in range(start_paren + 1, len(expr)):
            char = expr[i]

            if char == '(':
                depth += 1
                current_arg += char
            elif char == ')':
                if depth == 0:
                    if current_arg.strip():
                        args.append(current_arg.strip())
                    break
                depth -= 1
                current_arg += char
            elif char == ',' and depth == 0:
                args.append(current_arg.strip())
                current_arg = ""
            else:
                current_arg += char

        return args

    def _find_closing_paren(self, expr: str, start: int) -> int:
        depth = 0
        for i in range(start, len(expr)):
            if expr[i] == '(':
                depth += 1
            elif expr[i] == ')':
                depth -= 1
                if depth == 0:
                    return i
        return len(expr) - 1
