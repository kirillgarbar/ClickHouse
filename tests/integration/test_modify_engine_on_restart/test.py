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

    q(
        ch1,
        database_name,
        "CREATE TABLE foo ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;"
    )

    q(
        ch1,
        database_name,
        "INSERT INTO foo SELECT number, today(), '' FROM numbers(1e6);"
    )
    q(
        ch1,
        database_name,
        "INSERT INTO foo SELECT number, today()-60, '' FROM numbers(1e6);"
    )

    ch1.exec_in_container(
        ["bash", "-c", f"mkdir /var/lib/clickhouse/data/{database_name}/foo/flags"],
        ["bash", "-c", f"touch /var/lib/clickhouse/data/{database_name}/foo/flags/convert_to_replicated"]
    )

    ch1.restart_clickhouse()

    # Check tables exists
    assert q(
        ch1,
        database_name,
        "SHOW TABLES",
    ).strip() == "foo\nfoo_temp"

    # Check engines
    assert q(
        ch1,
        database_name,
        f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND (name = 'foo' OR name = 'foo_temp')",
    ).strip() == "foo\tReplicatedMergeTree\nfoo_temp\tMergeTree"

    # Check values
    assert q(
        ch1,
        database_name,
        "SELECT count() FROM foo",
    ).strip() == "2000000"
    

    # # Add new replica
    # q(
    #     ch2,
    #     database_name,
    #     f"CREATE TABLE foo ( A Int64, D Date, S String ) ENGINE ReplicatedMergeTree(('/clickhouse/cluster/01/tables/{database_name}/foo', '{{replica}}')) PARTITION BY toYYYYMM(D) ORDER BY A;"
    # )

    # sleep(5)

    # # Check data is replicated
    # assert q(
    #     ch2,
    #     database_name,
    #     "SELECT count() FROM test1.foo",
    # ).strip() == "2000000"