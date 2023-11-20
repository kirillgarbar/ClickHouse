import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
    macros={"replica": "node1"},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

# Case: merge tree to merge tree
def test_modify_engine_merge_to_merge(started_cluster):
    database_name = "test1"
    ch1.query(
        database="default",
        sql="CREATE DATABASE " + database_name,
    )
    assert ch1.query(
        database=database_name,
        sql="SHOW TABLES",
    ).strip() == ""

    ch1.query(
        database=database_name,
        sql="CREATE TABLE foo ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;",
    )

    ch1.query(
        database=database_name,
        sql="INSERT INTO foo SELECT number, today(), '' FROM numbers(1e2);",
    )
    ch1.query(
        database=database_name,
        sql="INSERT INTO foo SELECT number, today()-60, '' FROM numbers(1e2);",
    )

    ch1.query(
        database=database_name,
        sql="ALTER TABLE foo MODIFY ENGINE MergeTree PARTITION BY toYYYYMM(D) ORDER BY A;",
    )

    #Check tables exists
    assert ch1.query(
        database=database_name,
        sql="SHOW TABLES",
    ).strip() == "foo\nfoo_old"

    #Check engines
    assert ch1.query(
        database=database_name,
        sql=f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND (name = 'foo' OR name = 'foo_old')",
    ).strip() == "foo\tMergeTree\nfoo_old\tMergeTree"

    #Check values
    assert ch1.query(
        database=database_name,
        sql="SELECT count() FROM foo",
    ).strip() == "200"

# Case: replicated to merge tree
def test_modify_engine_replicated_to_merge(started_cluster):
    database_name = "test2"
    ch1.query(
        database="default",
        sql="CREATE DATABASE " + database_name,
    )
    assert ch1.query(
        database=database_name,
        sql="SHOW TABLES",
    ).strip() == ""

    ch1.query(
        database=database_name,
        sql="CREATE TABLE foo ( A Int64, D Date, S String ) ENGINE ReplicatedMergeTree('/clickhouse/cluster/tables/{database}/{table}/{shard}','{replica}') PARTITION BY toYYYYMM(D) ORDER BY A;",
    )

    ch1.query(
        database=database_name,
        sql="INSERT INTO foo SELECT number, today(), '' FROM numbers(1e2);",
    )
    ch1.query(
        database=database_name,
        sql="INSERT INTO foo SELECT number, today()-60, '' FROM numbers(1e2);",
    )

    ch1.query(
        database=database_name,
        sql="ALTER TABLE foo MODIFY ENGINE MergeTree PARTITION BY toYYYYMM(D) ORDER BY A;",
    )

    #Check tables exists
    assert ch1.query(
        database=database_name,
        sql="SHOW TABLES",
    ).strip() == "foo\nfoo_old"

    #Check engines
    assert ch1.query(
        database=database_name,
        sql=f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND (name = 'foo' OR name = 'foo_old')",
    ).strip() == "foo\tMergeTree\nfoo_old\tReplicatedMergeTree"

    #Check values
    assert ch1.query(
        database=database_name,
        sql="SELECT count() FROM foo",
    ).strip() == "200"

# Case: merge tree to replicated
def test_modify_engine_merge_to_replicated(started_cluster):
    database_name = "test"
    ch1.query(
        database="default",
        sql="CREATE DATABASE " + database_name,
    )
    assert ch1.query(
        database=database_name,
        sql="SHOW TABLES",
    ).strip() == ""

    ch1.query(
        database=database_name,
        sql="CREATE TABLE foo ( A Int64, D Date, S String ) ENGINE MergeTree PARTITION BY toYYYYMM(D) ORDER BY A;",
    )

    ch1.query(
        database=database_name,
        sql="INSERT INTO foo SELECT number, today(), '' FROM numbers(1e2);",
    )
    ch1.query(
        database=database_name,
        sql="INSERT INTO foo SELECT number, today()-60, '' FROM numbers(1e2);",
    )

    ch1.query(
        database=database_name,
        sql="ALTER TABLE foo MODIFY ENGINE ReplicatedMergeTree('/clickhouse/cluster/tables/{database}/{table}/{shard}','{replica}') PARTITION BY toYYYYMM(D) ORDER BY A;",
    )

    #Check tables exists
    assert ch1.query(
        database=database_name,
        sql="SHOW TABLES",
    ).strip() == "foo\nfoo_old"

    #Check engines
    assert ch1.query(
        database=database_name,
        sql=f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND (name = 'foo' OR name = 'foo_old')",
    ).strip() == "foo\tReplicatedMergeTree\nfoo_old\tMergeTree"

    #Check values
    assert ch1.query(
        database=database_name,
        sql="SELECT count() FROM foo",
    ).strip() == "200"