"""Tests for lib.write_target. Fixtures (runtime_lib, spark) come from conftest.py."""

import pytest


def test_dual_noop(runtime_lib, spark, monkeypatch):
    monkeypatch.setattr(runtime_lib, "write_table",
                        lambda *a, **k: pytest.fail("write_table called for DUAL"))
    runtime_lib.write_target(
        spark=spark, df=spark.createDataFrame([(1,)], ["V"]),
        conn={}, table="DUAL", mode="append", config={}, name="WT")


def test_field_map_and_target_select(runtime_lib, spark, monkeypatch):
    written = {}
    monkeypatch.setattr(runtime_lib, "write_table",
                        lambda df, conn, table, mode="append": written.update(
                            df=df, table=table, mode=mode))
    df = spark.createDataFrame([(1, "A")], ["SRC", "V"])
    runtime_lib.write_target(
        spark=spark, df=df, conn={}, table="T", mode="append", config={},
        field_map={"TGT": "SRC"}, target_columns=["TGT", "V"],
    )
    assert written["table"] == "T" and written["mode"] == "append"
    assert written["df"].columns == ["TGT", "V"]
    assert written["df"].collect()[0]["TGT"] == 1


def test_static_dd_update_batches_all_rows(runtime_lib, spark, monkeypatch):
    updates, written = [], {}
    monkeypatch.setattr(runtime_lib, "batch_update",
                        lambda s, c, t, set_c, key_c, rows, b=1000: updates.append(
                            (t, set_c, key_c, rows)))
    monkeypatch.setattr(runtime_lib, "write_table",
                        lambda df, conn, table, mode="append": written.update(
                            df=df, table=table))
    df = spark.createDataFrame([(1, "A"), (2, "B")], ["K", "V"])
    runtime_lib.write_target(
        spark=spark, df=df, conn={}, table="T", mode="append", config={},
        has_update_flag=True, static_dd="DD_UPDATE", delete_keys=["K"],
    )
    assert len(updates) == 1
    assert updates[0][1] == ["V"] and updates[0][2] == ["K"]
    assert sorted(r[1] for r in updates[0][3]) == [1, 2]
    assert written["df"].count() == 0  # filter(lit(False))


def test_dynamic_split_insert_only_write(runtime_lib, spark, monkeypatch):
    written, del_calls = {}, []
    monkeypatch.setattr(runtime_lib, "write_table",
                        lambda df, conn, table, mode="append": written.update(
                            df=df, table=table))
    monkeypatch.setattr(runtime_lib, "batch_delete_composite",
                        lambda s, c, t, keys, rows, b=1000: del_calls.append(
                            (t, keys, rows)))
    monkeypatch.setattr(runtime_lib, "batch_update", lambda *a, **k: None)
    df = spark.createDataFrame([(1, "I"), (2, "U"), (3, "D")], ["K", "_update_flag"])
    runtime_lib.write_target(
        spark=spark, df=df, conn={}, table="T", mode="append", config={},
        has_update_flag=True, delete_keys=["K"],
    )
    # I rows flow to the normal write; U/D rows handled via batch
    assert written["df"].count() == 1
    assert del_calls == [("T", ["K"], [(3,)])]
