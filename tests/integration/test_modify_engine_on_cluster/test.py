import pytest
from helpers.cluster import ClickHouseCluster
from time import sleep

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
ch2 = cluster.add_instance(
    "ch2",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
    macros={"replica": "node2"},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

# Case: 2 merge tree tables with same data on hosts, convert to replicated
def test_modify_engine_on_cluster_to_replicated(started_cluster):
    database_name = "test1"
    ch1.query(
        database="default",
        sql="CREATE DATABASE " + database_name + " ON CLUSTER 'cluster';",
    )
    assert ch1.query(
        database=database_name,
        sql="SHOW TABLES",
    ).strip() == ""

    ch1.query(
        database=database_name,
        sql="CREATE TABLE foo ON CLUSTER 'cluster' ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;",
    )

    for node in [ch1, ch2]:
        node.query(
            database=database_name,
            sql="INSERT INTO foo SELECT number, today(), '' FROM numbers(1e2);",
        )
        node.query(
            database=database_name,
            sql="INSERT INTO foo SELECT number, today()-60, '' FROM numbers(1e2);",
        )

    ch1.query(
        database=database_name,
        sql="ALTER TABLE foo ON CLUSTER 'cluster' MODIFY ENGINE ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{uuid}', '{replica}') PARTITION BY toYYYYMM(D) ORDER BY A;",
    )

    sleep(10)

    for node in [ch1, ch2]:
        #Check tables exists
        assert node.query(
            database=database_name,
            sql="SHOW TABLES",
        ).strip() == "foo\nfoo_old"

        #Check engines
        assert node.query(
            database=database_name,
            sql=f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND (name = 'foo' OR name = 'foo_old')",
        ).strip() == "foo\tReplicatedMergeTree\nfoo_old\tMergeTree"

        #Check values
        assert node.query(
            database=database_name,
            sql="SELECT count() FROM foo",
        ).strip() == "200"
    

# Case: 2 replciated merge tree tables, convert one to not replicated
def test_modify_engine_to_not_replicated(started_cluster):
    database_name = "test2"
    ch1.query(
        database="default",
        sql="CREATE DATABASE " + database_name + " ON CLUSTER 'cluster';",
    )

    assert ch1.query(
        database=database_name,
        sql="SHOW TABLES",
    ).strip() == ""

    ch1.query(
        database=database_name,
        sql="CREATE TABLE foo ON CLUSTER 'cluster' ( A Int64, D Date, S String ) ENGINE ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{uuid}', '{replica}') PARTITION BY toYYYYMM(D) ORDER BY A;",
    )

    ch1.query(
        database=database_name,
        sql="INSERT INTO foo SELECT number, today(), '' FROM numbers(1e2);",
    )
    ch1.query(
        database=database_name,
        sql="INSERT INTO foo SELECT number, today()-60, '' FROM numbers(1e2);",
    )

    sleep(10)

    assert ch2.query(
        database=database_name,
        sql="SELECT count() FROM foo;",
    ).strip() == "200"

    ch1.query(
        database=database_name,
        sql="ALTER TABLE foo MODIFY ENGINE MergeTree PARTITION BY toYYYYMM(D) ORDER BY A;",
    )

    #Check tables exists
    assert ch1.query(
        database=database_name,
        sql="SHOW TABLES",
    ).strip() == "foo\nfoo_old"
    assert ch2.query(
        database=database_name,
        sql="SHOW TABLES",
    ).strip() == "foo"

    #Check engines
    assert ch1.query(
        database=database_name,
        sql=f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND (name = 'foo' OR name = 'foo_old')",
    ).strip() == "foo\tMergeTree\nfoo_old\tReplicatedMergeTree"
    assert ch2.query(
        database=database_name,
        sql=f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND name = 'foo'",
    ).strip() == "foo\tReplicatedMergeTree"

    #Check values
    for node in [ch1, ch2]:
        assert node.query(
            database=database_name,
            sql="SELECT count() FROM foo",
        ).strip() == "200"