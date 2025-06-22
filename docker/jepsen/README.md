# Dockerized Jepsen

This docker image attempts to simplify the setup required by Jepsen.
It is intended to be used by a CI tool or anyone with Docker who wants to try Jepsen themselves.

It contains all the jepsen dependencies and code. It uses [Docker
Compose](https://github.com/docker/compose) to spin up the five containers used
by Jepsen. A script builds a `docker-compose.yml` file out of fragments in
`template/`, because this is the future, and using `awk` to generate YAML to
generate computers is *cloud native*.

## Quickstart

Assuming you have `docker compose` set up already, run:

```
bin/up
bin/console
```

... which will drop you into a console on the Jepsen control node.

Your DB nodes are `n1`, `n2`, `n3`, `n4`, and `n5`. You can open as many shells
as you like using `bin/console`. If your test includes a web server (try `lein
run serve` on the control node, in your test directory), you can open it
locally by running using `bin/web`. This can be a handy way to browse test
results.


## Run ClickHouse Keeper test

```sh
sudo ./bin/console

cd jepsen.clickhouse

lein run test --nodes n1,n2,n3 --username root --password '' --time-limit 30 --concurrency 50 -r 50 --workload set --nemesis logs-and-snapshots-corruptor  --clickhouse-source '/usr/bin/clickhouse' -q --test-count 1 --reuse-binary
```

## Run ClickHouse Server test

```sh
sudo ./bin/console

cd jepsen.clickhouse

lein run server test --nodes n1,n2,n3 --username root --password '' --time-limit 30 --concurrency 50 -r 50 --workload set --nemesis random-node-killer  --clickhouse-source '/usr/bin/clickhouse' --test-count 1 --reuse-binary
```

## Run ClickHouse Server zero-copy test

```sh
sudo ./bin/console

cd jepsen.clickhouse

lein run server test --nodes n1,n2,n3 --keeper n4 --minio n5 --username root --password '' --time-limit 30 --concurrency 50 -r 50 --workload set --nemesis random-node-killer  --clickhouse-source '/usr/bin/clickhouse' --test-count 1 --reuse-binary
```