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

database_name = "modify_engine"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def q(node, query):
    return node.query(database=database_name, sql=query)


def create_tables():
    # MergeTree table that will be converted
    q(
        ch1,
        "CREATE TABLE rmt ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;",
    )

    q(ch1, "INSERT INTO rmt SELECT number, today(), '' FROM numbers(1e6);")
    q(ch1, "INSERT INTO rmt SELECT number, today()-60, '' FROM numbers(1e5);")

    # ReplacingMergeTree table that will be converted to check unusual engine kinds
    q(
        ch1,
        "CREATE TABLE replacing ( A Int64, D Date, S String ) ENGINE ReplacingMergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;",
    )

    q(ch1, "INSERT INTO replacing SELECT number, today(), '' FROM numbers(1e6);")
    q(ch1, "INSERT INTO replacing SELECT number, today()-60, '' FROM numbers(1e5);")

    # Table with arguments to check that they will not be lost during convertation
    q(
        ch1,
        "CREATE TABLE collapsing_ver ( ID UInt64, Sign Int8, Version UInt8 ) ENGINE = VersionedCollapsingMergeTree(Sign, Version) ORDER BY ID",
    )

    # MergeTree table that will be converted to MergeTree
    q(
        ch1,
        "CREATE TABLE mt ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;",
    )

    # Not MergeTree table to check it will not be converted
    q(ch1, "CREATE TABLE log ( A Int64, D Date, S String ) ENGINE Log;")


def convert_tables():
    q(
        ch1,
        "ALTER TABLE rmt MODIFY ENGINE TO REPLICATED",
    )

    q(
        ch1,
        "ALTER TABLE replacing MODIFY ENGINE TO REPLICATED",
    )

    q(
        ch1,
        "ALTER TABLE collapsing_ver MODIFY ENGINE TO REPLICATED",
    )


def check_tables_converted():
    # Check tables exists
    assert (
        q(
            ch1,
            "SHOW TABLES",
        ).strip()
        == "collapsing_ver\nlog\nmt\nreplacing\nrmt"
    )

    # Check engines
    assert (
        q(
            ch1,
            f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND (name != 'log' AND name != 'mt')",
        ).strip()
        == f"collapsing_ver\tReplicatedVersionedCollapsingMergeTree\nreplacing\tReplicatedReplacingMergeTree\nrmt\tReplicatedMergeTree"
    )
    assert (
        q(
            ch1,
            f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND (name = 'log' OR name = 'mt')",
        ).strip()
        == "log\tLog\nmt\tMergeTree"
    )

    # Check arguments
    collapsing_uuid = q(
        ch1,
        f"SELECT uuid FROM system.tables WHERE database = '{database_name}' and name = 'collapsing_ver'",
    ).strip()
    assert (
        q(
            ch1,
            f"SELECT engine_full FROM system.tables WHERE database = '{database_name}' and name = 'collapsing_ver'",
        )
        .strip()
        .startswith(
            f"ReplicatedVersionedCollapsingMergeTree(\\'/clickhouse/tables/{collapsing_uuid}/{{shard}}\\', \\'{{replica}}\\', Sign, Version)"
        )
    )

    # Check values
    for table in ["rmt", "replacing"]:
        assert (
            q(
                ch1,
                f"SELECT count() FROM {table}",
            ).strip()
            == "1100000"
        )


def check_replica_added():
    # Add replica to check if zookeeper path is correct and consistent with table uuid

    uuid = q(
        ch1,
        f"SELECT uuid FROM system.tables WHERE table = 'rmt' AND database = '{database_name}'",
    ).strip()

    q(
        ch2,
        f"CREATE TABLE rmt ( A Int64, D Date, S String ) ENGINE ReplicatedMergeTree('/clickhouse/tables/{uuid}/{{shard}}', '{{replica}}') PARTITION BY toYYYYMM(D) ORDER BY A",
    )

    ch2.query(database=database_name, sql="SYSTEM SYNC REPLICA rmt", timeout=20)

    # Check values
    assert (
        q(
            ch2,
            f"SELECT count() FROM rmt",
        ).strip()
        == "1100000"
    )


def test_modify_engine(started_cluster):
    ch1.query("CREATE DATABASE " + database_name + " ON CLUSTER cluster")

    create_tables()

    convert_tables()

    check_tables_converted()

    # Restart to check that tables are loaded correctly from new metadata
    ch1.restart_clickhouse()

    check_tables_converted()

    check_replica_added()
