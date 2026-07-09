"""Unit tests for metadata_builder.py.

Tests build_metadata() — the pure-function that constructs a MetadataModel
from parsed converter objects (MappingDefinition, Instance, TargetDefinition).
"""

from informatica_sparker.models import (
    MappingDefinition,
    Instance,
    TargetDefinition,
)
from informatica_sparker.metadata.metadata_builder import (
    build_metadata,
    _resolve_targets_from_instances,
    _make_safe_name,
)


# ── _make_safe_name tests ────────────────────────────────────────────────


class TestMakeSafeName:
    def test_lowercase_underscore(self):
        assert _make_safe_name("WF_CMS_DDS_APLY_MTH") == "wf_cms_dds_aply_mth"

    def test_already_lower(self):
        assert _make_safe_name("wf_emp") == "wf_emp"

    def test_leading_digit(self):
        assert _make_safe_name("1workflow").startswith("_")

    def test_special_chars_replaced(self):
        assert _make_safe_name("wf.emp$test") == "wf_emp_test"


# ── _resolve_targets_from_instances tests ────────────────────────────────


class TestResolveTargetsFromInstances:
    def test_single_target(self):
        mapping = MappingDefinition(
            name="m_test",
            instances=[Instance(name="INS_TGT", type="TARGET",
                                transformation_name="TGT_TEST")],
            targets=[TargetDefinition(name="TGT_TEST", db_name="DB_DEV")],
        )
        result = _resolve_targets_from_instances(mapping)
        assert len(result) == 1
        assert result[0].table == "TGT_TEST"
        assert result[0].connection == "DB_DEV"

    def test_multiple_targets(self):
        mapping = MappingDefinition(
            name="m_test",
            instances=[
                Instance(name="INS_A", type="TARGET",
                         transformation_name="TGT_A"),
                Instance(name="INS_B", type="TARGET",
                         transformation_name="TGT_B"),
            ],
            targets=[
                TargetDefinition(name="TGT_A", db_name="DB_A"),
                TargetDefinition(name="TGT_B", db_name="DB_B"),
            ],
        )
        result = _resolve_targets_from_instances(mapping)
        assert len(result) == 2
        assert result[0].table == "TGT_A"
        assert result[1].table == "TGT_B"

    def test_no_targets(self):
        mapping = MappingDefinition(
            name="m_empty",
            instances=[],
            targets=[],
        )
        result = _resolve_targets_from_instances(mapping)
        assert result == []

    def test_duplicate_instance_same_target(self):
        mapping = MappingDefinition(
            name="m_dup",
            instances=[
                Instance(name="INS_1", type="TARGET",
                         transformation_name="TGT_DUP"),
                Instance(name="INS_2", type="TARGET",
                         transformation_name="TGT_DUP"),
            ],
            targets=[TargetDefinition(name="TGT_DUP", db_name="DB_DUP")],
        )
        result = _resolve_targets_from_instances(mapping)
        assert len(result) == 2
        assert result[0].table == "TGT_DUP"
        assert result[1].table == "TGT_DUP"
        assert result[0].connection == "DB_DUP"

    def test_ignores_non_target_instances(self):
        """Instances referencing transformations (not targets) are skipped."""
        mapping = MappingDefinition(
            name="m_filtered",
            instances=[
                Instance(name="INS_SRC", type="SOURCE",
                         transformation_name="SRC_EMP"),
                Instance(name="INS_EXP", type="TRANSFORMATION",
                         transformation_name="EXPTRANS"),
                Instance(name="INS_TGT", type="TARGET",
                         transformation_name="TGT_EMP"),
            ],
            targets=[TargetDefinition(name="TGT_EMP", db_name="DW_DEV")],
        )
        result = _resolve_targets_from_instances(mapping)
        assert len(result) == 1
        assert result[0].table == "TGT_EMP"

    def test_empty_connection_fallback(self):
        mapping = MappingDefinition(
            name="m_no_conn",
            instances=[Instance(name="INS_TGT", type="TARGET",
                                transformation_name="TGT_NOCONN")],
            targets=[TargetDefinition(name="TGT_NOCONN", db_name="")],
        )
        result = _resolve_targets_from_instances(mapping)
        assert result[0].connection == "target_db"

    def test_instance_without_transformation_name(self):
        """Instance with empty transformation_name is skipped."""
        mapping = MappingDefinition(
            name="m_empty_key",
            instances=[Instance(name="INS_TGT", type="TARGET",
                                transformation_name="")],
            targets=[TargetDefinition(name="TGT_EMP", db_name="DB")],
        )
        result = _resolve_targets_from_instances(mapping)
        assert len(result) == 0


# ── build_metadata integration tests ─────────────────────────────────────


class TestBuildMetadata:
    def test_single_mapping_single_target(self):
        mapping = MappingDefinition(
            name="m_employee",
            instances=[Instance(name="INS_TGT", type="TARGET",
                                transformation_name="TGT_EMP")],
            targets=[TargetDefinition(name="TGT_EMP", db_name="DW_DEV")],
        )
        result = build_metadata([mapping], "WF_EMP", "wf_emp.py")

        assert result.metadata_version == "1.0"
        assert result.workflow == "WF_EMP"
        assert len(result.mappings) == 1
        assert result.mappings[0].mapping == "m_employee"
        assert len(result.mappings[0].targets) == 1
        assert result.mappings[0].targets[0].table == "TGT_EMP"
        assert result.mappings[0].targets[0].connection == "DW_DEV"
        assert result.output.script == "wf_emp.py"

    def test_multiple_mappings(self):
        m1 = MappingDefinition(
            name="m_a",
            instances=[Instance(name="INS_A", type="TARGET",
                                transformation_name="TGT_A")],
            targets=[TargetDefinition(name="TGT_A", db_name="DB_A")],
        )
        m2 = MappingDefinition(
            name="m_b",
            instances=[Instance(name="INS_B", type="TARGET",
                                transformation_name="TGT_B")],
            targets=[TargetDefinition(name="TGT_B", db_name="DB_B")],
        )
        result = build_metadata([m1, m2], "WF_MULTI", "wf_multi.py")

        assert len(result.mappings) == 2
        assert result.mappings[0].mapping == "m_a"
        assert result.mappings[1].mapping == "m_b"
        assert result.mappings[0].targets[0].connection == "DB_A"
        assert result.mappings[1].targets[0].connection == "DB_B"

    def test_mapping_without_targets(self):
        mapping = MappingDefinition(
            name="m_no_target",
            instances=[],
            targets=[],
        )
        result = build_metadata([mapping], "WF_NO_TGT", "wf_no_tgt.py")

        assert len(result.mappings) == 1
        assert len(result.mappings[0].targets) == 0

    def test_workflow_name_and_script(self):
        mapping = MappingDefinition(
            name="m_test",
            instances=[Instance(name="INS_TGT", type="TARGET",
                                transformation_name="TGT_T")],
            targets=[TargetDefinition(name="TGT_T", db_name="DB")],
        )
        result = build_metadata([mapping], "WF_CMS_DDS_APLY_MTH",
                                "wf_cms_dds_aply_mth.py")

        assert result.workflow == "WF_CMS_DDS_APLY_MTH"
        assert result.output.script == "wf_cms_dds_aply_mth.py"

    def test_model_dump_json(self):
        """Verify serialised JSON matches the agreed contract schema."""
        mapping = MappingDefinition(
            name="m_employee",
            instances=[Instance(name="INS_TGT", type="TARGET",
                                transformation_name="TGT_EMP")],
            targets=[TargetDefinition(name="TGT_EMP", db_name="DW_DEV")],
        )
        result = build_metadata([mapping], "WF_EMP", "wf_emp.py")
        dumped = result.model_dump()

        assert dumped == {
            "metadata_version": "1.0",
            "workflow": "WF_EMP",
            "mappings": [
                {
                    "mapping": "m_employee",
                    "targets": [
                        {"table": "TGT_EMP", "connection": "DW_DEV"}
                    ],
                }
            ],
            "output": {"script": "wf_emp.py"},
        }
