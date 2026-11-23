#!/bin/sh
# Registers the JDBC sink with FlattenNested, produces nested orders, and checks the flattened rows in PostgreSQL.
# Expects the stack from docker-compose.yml to be running.
set -eu
cd "$(dirname "$0")/.."

CONNECT=http://localhost:8083
psql() { docker compose exec -T postgres psql -U demo -d warehouse "$@"; }

echo "1. Plugin is visible to the Connect worker"
curl -fs "$CONNECT/connector-plugins?connectorsOnly=false" | grep -q 'FlattenNested$Value' \
  || { echo "FlattenNested not found on plugin.path"; exit 1; }

echo "2. Register JDBC sink with the FlattenNested transform"
curl -fs -X PUT -H 'Content-Type: application/json' --data @demo/orders-sink.json \
  "$CONNECT/connectors/orders-sink/config" > /dev/null

echo "3. Produce nested orders"
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders < demo/orders.jsonl

echo "4. Wait for flattened rows"
tries=60
until [ "$(psql -tAc "select count(*) from orders" 2>/dev/null || echo 0)" = "3" ]; do
  tries=$((tries - 1))
  if [ "$tries" -le 0 ]; then
    curl -s "$CONNECT/connectors/orders-sink/status"; echo
    echo "rows did not arrive"; exit 1
  fi
  sleep 2
done

psql -c 'select * from orders order by id'

row=$(psql -tAc "select customer_name, customer_address_city, tags, items from orders where id = 1")
expected='Ana|Dhaka|new,priority|[{"sku":"A-1","qty":2},{"sku":"B-7","qty":1}]'
[ "$row" = "$expected" ] || { echo "unexpected row: $row"; exit 1; }
[ "$(psql -tAc "select customer_address_city is null from orders where id = 2")" = "t" ] \
  || { echo "null parent struct should give null columns"; exit 1; }
echo "demo passed: nested structs and arrays were flattened into relational columns"
