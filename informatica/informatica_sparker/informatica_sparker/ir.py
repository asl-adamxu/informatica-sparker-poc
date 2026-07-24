from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from enum import Enum


class IRStepType(str, Enum):
    READ_SQL = "ReadSQL"
    READ_FILE = "ReadFile"
    APPLY_SOURCE_QUALIFIER = "ApplySourceQualifier"
    APPLY_FILTER = "ApplyFilter"
    APPLY_EXPRESSION = "ApplyExpression"
    APPLY_LOOKUP = "ApplyLookup"
    APPLY_JOINER = "ApplyJoiner"
    APPLY_AGGREGATOR = "ApplyAggregator"
    APPLY_SORTER = "ApplySorter"
    APPLY_ROUTER = "ApplyRouter"
    APPLY_UNION = "ApplyUnion"
    APPLY_UPDATE_STRATEGY = "ApplyUpdateStrategy"
    APPLY_SEQUENCE = "ApplySequence"
    CALL_STORED_PROC = "CallStoredProc"
    EXECUTE_SQL = "ExecuteSQL"
    WRITE_TARGET = "WriteTarget"
    WRITE_DELTA = "WriteDelta"
    MERGE_DELTA = "MergeDelta"
    APPLY_NORMALIZER = "ApplyNormalizer"
    APPLY_RANK = "ApplyRank"
    APPLY_TRANSACTION_CONTROL = "ApplyTransactionControl"
    ASSIGN_VARIABLE = "AssignVariable"


class IRStep(BaseModel):
    step_type: IRStepType
    step_name: str
    df_input: Optional[str] = None
    df_output: str = ""
    params: Dict[str, Any] = Field(default_factory=dict)
    comments: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class ReadSQLStep(IRStep):
    step_type: IRStepType = IRStepType.READ_SQL

    def __init__(self, step_name: str, df_output: str,
                 connection_alias: str, table_name: str = "",
                 query: str = "", is_lookup: bool = False,
                 db_type: str = "oracle", **kwargs):
        super().__init__(
            step_name=step_name,
            df_output=df_output,
            params={
                "connection_alias": connection_alias,
                "table_name": table_name,
                "query": query,
                "is_lookup": is_lookup,
                "db_type": db_type
            },
            **kwargs
        )


class ReadFileStep(IRStep):
    step_type: IRStepType = IRStepType.READ_FILE

    def __init__(self, step_name: str, df_output: str,
                 file_format: str, file_path: str,
                 options: Dict[str, Any] = None,
                 table_name: str = "", **kwargs):
        super().__init__(
            step_name=step_name,
            df_output=df_output,
            params={
                "file_format": file_format,
                "file_path": file_path,
                "options": options or {},
                "table_name": table_name,
                "object_name": table_name
            },
            **kwargs
        )


class ApplySourceQualifierStep(IRStep):
    step_type: IRStepType = IRStepType.APPLY_SOURCE_QUALIFIER

    def __init__(self, step_name: str, df_input: str, df_output: str,
                 sql_query: str = "", filter_condition: str = "",
                 distinct: bool = False, select_columns: List[str] = None, **kwargs):
        super().__init__(
            step_name=step_name,
            df_input=df_input,
            df_output=df_output,
            params={
                "sql_query": sql_query,
                "filter_condition": filter_condition,
                "distinct": distinct,
                "select_columns": select_columns or []
            },
            **kwargs
        )


class ApplyFilterStep(IRStep):
    step_type: IRStepType = IRStepType.APPLY_FILTER

    def __init__(self, step_name: str, df_input: str, df_output: str,
                 condition: str, original_condition: str = "", **kwargs):
        super().__init__(
            step_name=step_name,
            df_input=df_input,
            df_output=df_output,
            params={
                "condition": condition,
                "original_condition": original_condition
            },
            **kwargs
        )


class ComputedColumn(BaseModel):
    name: str
    expression: str
    datatype: str = "string"


class ApplyExpressionStep(IRStep):
    step_type: IRStepType = IRStepType.APPLY_EXPRESSION

    def __init__(self, step_name: str, df_input: str, df_output: str,
                 computed_columns: List[ComputedColumn] = None, **kwargs):
        super().__init__(
            step_name=step_name,
            df_input=df_input,
            df_output=df_output,
            params={"computed_columns": [c.model_dump() for c in (computed_columns or [])]},
            **kwargs
        )


class JoinPredicate(BaseModel):
    source_col: str
    lookup_col: str


class ApplyLookupStep(IRStep):
    step_type: IRStepType = IRStepType.APPLY_LOOKUP

    def __init__(self, step_name: str, df_input: str, df_output: str,
                 lookup_df: str,
                 join_predicates: List[Dict[str, str]] = None,
                 join_expr: str = None,
                 output_columns: List[str] = None,
                 lookup_type: str = "left", **kwargs):
        super().__init__(
            step_name=step_name,
            df_input=df_input,
            df_output=df_output,
            params={
                "lookup_df": lookup_df,
                "join_predicates": join_predicates or [],
                "join_expr": join_expr,
                "output_columns": output_columns or [],
                "lookup_type": lookup_type
            },
            **kwargs
        )


class ApplyJoinerStep(IRStep):
    step_type: IRStepType = IRStepType.APPLY_JOINER

    def __init__(self, step_name: str, df_master: str, df_detail: str,
                 df_output: str, join_condition: str,
                 join_type: str = "inner", **kwargs):
        super().__init__(
            step_name=step_name,
            df_input=df_master,
            df_output=df_output,
            params={
                "df_master": df_master,
                "df_detail": df_detail,
                "join_condition": join_condition,
                "join_type": join_type
            },
            **kwargs
        )


class ApplyAggregatorStep(IRStep):
    step_type: IRStepType = IRStepType.APPLY_AGGREGATOR

    def __init__(self, step_name: str, df_input: str, df_output: str,
                 group_by: List[str] = None,
                 aggregations: Dict[str, str] = None, **kwargs):
        super().__init__(
            step_name=step_name,
            df_input=df_input,
            df_output=df_output,
            params={
                "group_by": group_by or [],
                "aggregations": aggregations or {}
            },
            **kwargs
        )


class ApplySorterStep(IRStep):
    step_type: IRStepType = IRStepType.APPLY_SORTER

    def __init__(self, step_name: str, df_input: str, df_output: str,
                 sort_columns: List[Dict[str, str]] = None, **kwargs):
        super().__init__(
            step_name=step_name,
            df_input=df_input,
            df_output=df_output,
            params={"sort_columns": sort_columns or []},
            **kwargs
        )


class ApplyUnionStep(IRStep):
    step_type: IRStepType = IRStepType.APPLY_UNION

    def __init__(self, step_name: str, df_inputs: List[str],
                 df_output: str, union_all: bool = True, **kwargs):
        super().__init__(
            step_name=step_name,
            df_input=df_inputs[0] if df_inputs else None,
            df_output=df_output,
            params={
                "df_inputs": df_inputs,
                "union_all": union_all
            },
            **kwargs
        )


class ApplyUpdateStrategyStep(IRStep):
    step_type: IRStepType = IRStepType.APPLY_UPDATE_STRATEGY

    def __init__(self, step_name: str, df_input: str, df_output: str,
                 strategy_expression: str, **kwargs):
        super().__init__(
            step_name=step_name,
            df_input=df_input,
            df_output=df_output,
            params={"strategy_expression": strategy_expression},
            **kwargs
        )


class WriteTargetStep(IRStep):
    step_type: IRStepType = IRStepType.WRITE_TARGET

    def __init__(self, step_name: str, df_input: str,
                 sink_type: str, path: str = "", table_name: str = "",
                 mode: str = "append", format: str = "delta",
                 partition_by: List[str] = None, **kwargs):
        super().__init__(
            step_name=step_name,
            df_input=df_input,
            df_output="",
            params={
                "sink_type": sink_type,
                "path": path,
                "table_name": table_name,
                "mode": mode,
                "format": format,
                "partition_by": partition_by or []
            },
            **kwargs
        )


class ApplyRouterStep(IRStep):
    step_type: IRStepType = IRStepType.APPLY_ROUTER

    def __init__(self, step_name: str, df_input: str,
                 groups: List[Dict[str, str]] = None, **kwargs):
        super().__init__(
            step_name=step_name,
            df_input=df_input,
            df_output="",
            params={
                "groups": groups or []
            },
            **kwargs
        )


class ExecuteSQLStep(IRStep):
    step_type: IRStepType = IRStepType.EXECUTE_SQL

    def __init__(self, step_name: str, connection_alias: str,
                 sql_statement: str, sql_type: str = "pre", **kwargs):
        super().__init__(
            step_name=step_name,
            df_output="",
            params={
                "connection_alias": connection_alias,
                "sql_statement": sql_statement,
                "sql_type": sql_type
            },
            **kwargs
        )


class MergeDeltaStep(IRStep):
    step_type: IRStepType = IRStepType.MERGE_DELTA

    def __init__(self, step_name: str, df_input: str,
                 target_table: str, merge_keys: List[str],
                 update_columns: List[str] = None,
                 insert_columns: List[str] = None,
                 delete_condition: str = "", **kwargs):
        super().__init__(
            step_name=step_name,
            df_input=df_input,
            df_output="",
            params={
                "target_table": target_table,
                "merge_keys": merge_keys,
                "update_columns": update_columns or [],
                "insert_columns": insert_columns or [],
                "delete_condition": delete_condition
            },
            **kwargs
        )


class ApplySequenceStep(IRStep):
    step_type: IRStepType = IRStepType.APPLY_SEQUENCE

    def __init__(self, step_name: str, df_input: str, df_output: str,
                 sequence_name: str, start_value: int = 1, **kwargs):
        super().__init__(
            step_name=step_name,
            df_input=df_input,
            df_output=df_output,
            params={
                "sequence_name": sequence_name,
                "start_value": start_value
            },
            **kwargs
        )


class ApplyNormalizerStep(IRStep):
    step_type: IRStepType = IRStepType.APPLY_NORMALIZER

    def __init__(self, step_name: str, df_input: str, df_output: str,
                 key_name: str = "GENERATED_KEY",
                 normalizer_fields: List[Dict[str, Any]] = None, **kwargs):
        super().__init__(
            step_name=step_name,
            df_input=df_input,
            df_output=df_output,
            params={
                "key_name": key_name,
                "normalizer_fields": normalizer_fields or [],
            },
            **kwargs
        )


class ApplyRankStep(IRStep):
    step_type: IRStepType = IRStepType.APPLY_RANK

    def __init__(self, step_name: str, df_input: str, df_output: str,
                 group_by: List[str] = None,
                 rank_expression: str = "",
                 rank_port_name: str = "RANKINDEX",
                 top_n: int = 1,
                 sort_direction: str = "DESC",
                 rank_function: str = "row_number",
                 **kwargs):
        super().__init__(
            step_name=step_name,
            df_input=df_input,
            df_output=df_output,
            params={
                "group_by": group_by or [],
                "rank_expression": rank_expression,
                "rank_port_name": rank_port_name,
                "top_n": top_n,
                "sort_direction": sort_direction,
                "rank_function": rank_function,
            },
            **kwargs
        )


class ApplyTransactionControlStep(IRStep):
    step_type: IRStepType = IRStepType.APPLY_TRANSACTION_CONTROL

    def __init__(self, step_name: str, df_input: str, df_output: str,
                 control_expression: str = "", **kwargs):
        super().__init__(
            step_name=step_name,
            df_input=df_input,
            df_output=df_output,
            params={
                "control_expression": control_expression,
            },
            **kwargs
        )


class WorkflowSession(BaseModel):
    name: str
    session_type: str = "session"
    mapping_name: str = ""
    pre_sql: List[str] = Field(default_factory=list)
    post_sql: List[str] = Field(default_factory=list)
    dependencies: List[str] = Field(default_factory=list)
    on_success: List[str] = Field(default_factory=list)
    on_failure: List[str] = Field(default_factory=list)
    parameters: Dict[str, Any] = Field(default_factory=dict)


class WorkflowDAG(BaseModel):
    workflow_name: str
    sessions: List[WorkflowSession] = Field(default_factory=list)
    start_task: str = ""
    execution_order: List[str] = Field(default_factory=list)
    variables: Dict[str, str] = Field(default_factory=dict)

    def add_session(self, session: WorkflowSession):
        self.sessions.append(session)
        if not self.start_task:
            self.start_task = session.name


class IRPlan(BaseModel):
    mapping_name: str
    steps: List[IRStep] = Field(default_factory=list)
    lookup_dfs: Dict[str, str] = Field(default_factory=dict)
    lookup_return_ports: Dict[str, List[str]] = Field(default_factory=dict)
    variables: Dict[str, str] = Field(default_factory=dict)
    router_outputs: Dict[str, List[str]] = Field(default_factory=dict)
    warnings: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    pre_sql: List[str] = Field(default_factory=list)
    post_sql: List[str] = Field(default_factory=list)
    source_db_type: str = "oracle"
    target_db_type: str = "spark"
    mapping_variables: Dict[str, str] = Field(default_factory=dict)
    needs_window_import: bool = False

    def add_step(self, step: IRStep):
        self.steps.append(step)

    def add_warning(self, warning: str):
        self.warnings.append(warning)

    def add_error(self, error: str):
        self.errors.append(error)
