import re
from typing import List, Set
from .models import (
    MappingDefinition, ExpressionAnalysis,
    LookupReference, StoredProcReference
)


class ExpressionScanner:

    LKP_PATTERN = re.compile(r':LKP\.([A-Za-z_][A-Za-z0-9_]*)\s*\(([^)]*)\)', re.IGNORECASE)
    SP_PATTERN = re.compile(r':SP\.([A-Za-z_][A-Za-z0-9_]*)\s*\(([^)]*)\)', re.IGNORECASE)
    PM_PATTERN = re.compile(r'\$PM([A-Za-z_][A-Za-z0-9_]*)', re.IGNORECASE)

    def __init__(self, mapping: MappingDefinition):
        self.mapping = mapping

    def scan(self) -> ExpressionAnalysis:
        analysis = ExpressionAnalysis()

        all_expressions = self._collect_expressions()

        lookups_found: Set[str] = set()
        sps_found: Set[str] = set()
        pm_vars_found: Set[str] = set()

        for expr in all_expressions:
            for match in self.LKP_PATTERN.finditer(expr):
                lookup_name = match.group(1)
                if lookup_name not in lookups_found:
                    lookups_found.add(lookup_name)
                    lkp_ref = LookupReference(
                        lookup_name=lookup_name,
                        input_ports=self._parse_params(match.group(2))
                    )
                    analysis.lookups.append(lkp_ref)

            for match in self.SP_PATTERN.finditer(expr):
                sp_name = match.group(1)
                if sp_name not in sps_found:
                    sps_found.add(sp_name)
                    sp_ref = StoredProcReference(
                        sp_name=sp_name,
                        input_ports=self._parse_params(match.group(2))
                    )
                    analysis.stored_procs.append(sp_ref)

            for match in self.PM_PATTERN.finditer(expr):
                pm_var = "$PM" + match.group(1)
                if pm_var not in pm_vars_found:
                    pm_vars_found.add(pm_var)
                    analysis.pm_variables.append(pm_var)

        return analysis

    def _collect_expressions(self) -> List[str]:
        expressions = []

        for transform in self.mapping.transformations:
            for field in transform.fields:
                if field.expression:
                    expressions.append(field.expression)
                if field.default_value:
                    expressions.append(field.default_value)

            for attr_name, attr_value in transform.table_attributes.items():
                if "expression" in attr_name.lower() or "condition" in attr_name.lower():
                    expressions.append(attr_value)
                if "sql" in attr_name.lower():
                    expressions.append(attr_value)

        return expressions

    def _parse_params(self, params_str: str) -> List[str]:
        if not params_str:
            return []

        params = []
        current = ""
        paren_depth = 0

        for char in params_str:
            if char == '(':
                paren_depth += 1
                current += char
            elif char == ')':
                paren_depth -= 1
                current += char
            elif char == ',' and paren_depth == 0:
                params.append(current.strip())
                current = ""
            else:
                current += char

        if current.strip():
            params.append(current.strip())

        return params

    def find_column_references(self, expression: str) -> List[str]:
        col_pattern = re.compile(r'\b([A-Z_][A-Z0-9_]*)\b', re.IGNORECASE)

        keywords = {
            'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'TRUE', 'FALSE',
            'LIKE', 'BETWEEN', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
            'IF', 'IIF', 'DECODE', 'NVL', 'ISNULL', 'COALESCE',
            'TO_CHAR', 'TO_DATE', 'TO_NUMBER', 'SUBSTR', 'INSTR',
            'TRIM', 'LTRIM', 'RTRIM', 'UPPER', 'LOWER', 'LENGTH',
            'CONCAT', 'LPAD', 'RPAD', 'REPLACE', 'ROUND', 'TRUNC',
            'ABS', 'MOD', 'POWER', 'SQRT', 'FLOOR', 'CEIL',
            'SYSDATE', 'SYSTIMESTAMP', 'ADD_TO_DATE', 'DATE_DIFF',
            'SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'FIRST', 'LAST'
        }

        matches = col_pattern.findall(expression)
        columns = [m for m in matches if m.upper() not in keywords]

        return list(set(columns))
