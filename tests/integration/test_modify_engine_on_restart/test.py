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
    stay_alive=True,
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

def q(node, database, query):
    return node.query(
                    database=database,
                    sql=query
                    )

def create_tables(database_name, node):
    # MergeTree table that will be converted
    q(
        node,
        database_name,
        "CREATE TABLE foo ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;"
    )

    q(
        node,
        database_name,
        "INSERT INTO foo SELECT number, today(), '' FROM numbers(1e6);"
    )
    q(
        node,
        database_name,
        "INSERT INTO foo SELECT number, today()-60, '' FROM numbers(1e6);"
    )

    # MergeTree table that will not be converted
    q(
        node,
        database_name,
        "CREATE TABLE bar ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;"
    )

    # Not MergeTree table
    q(
        node,
        database_name,
        "CREATE TABLE bar2 ( A Int64, D Date, S String ) ENGINE Log;"
    )

def check_tables_not_converted(database_name, node):
    # Check tables exists
    assert q(
        node,
        database_name,
        "SHOW TABLES",
    ).strip() == "bar\nbar2\nfoo"

    # Check engines
    assert q(
        node,
        database_name,
        f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND (name = 'foo' OR name = 'foo_temp')",
    ).strip() == "foo\tMergeTree"
    assert q(
        node,
        database_name,
        f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND (name = 'bar' OR name = 'bar2')",
    ).strip() == "bar\tMergeTree\nbar2\tLog"

    # Check values
    assert q(
        node,
        database_name,
        "SELECT count() FROM foo",
    ).strip() == "2000000"

def check_tables_converted(database_name, node):
    # Check tables exists
    assert q(
        node,
        database_name,
        "SHOW TABLES",
    ).strip() == "bar\nbar2\nfoo\nfoo_temp"

    # Check engines
    assert q(
        node,
        database_name,
        f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND (name = 'foo' OR name = 'foo_temp')",
    ).strip() == "foo\tReplicatedMergeTree\nfoo_temp\tMergeTree"
    assert q(
        node,
        database_name,
        f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND (name = 'bar' OR name = 'bar2')",
    ).strip() == "bar\tMergeTree\nbar2\tLog"

    # Check values
    assert q(
        node,
        database_name,
        "SELECT count() FROM foo",
    ).strip() == "2000000"
    assert q(
        node,
        database_name,
        "SELECT count() FROM foo_temp",
    ).strip() == "2000000"

def test_modify_engine_on_restart(started_cluster):
    database_name = "test1"
    q(
        ch1,
        "default",
        "CREATE DATABASE " + database_name + " ON CLUSTER cluster",
    )
    assert q(
        ch1,
        database_name,
        "SHOW TABLES"
    ).strip() == ""

    create_tables(database_name, ch1)

    check_tables_not_converted(database_name, ch1)

    # Restart server without flags set
    ch1.restart_clickhouse()

    check_tables_not_converted(database_name, ch1)

    # Set convert flag
    ch1.exec_in_container(
        ["bash", "-c", f"mkdir /var/lib/clickhouse/data/{database_name}/foo/flags"],
        ["bash", "-c", f"touch /var/lib/clickhouse/data/{database_name}/foo/flags/convert_to_replicated"]
    )

    # Set flag to not MergeTree table to check that nothing happens
    ch1.exec_in_container(
        ["bash", "-c", f"mkdir /var/lib/clickhouse/data/{database_name}/bar2/flags"],
        ["bash", "-c", f"touch /var/lib/clickhouse/data/{database_name}/bar2/flags/convert_to_replicated"]
    )

    ch1.restart_clickhouse()

    check_tables_converted(database_name, ch1)