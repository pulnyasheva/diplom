#!/bin/bash

set -e

host="$1"
shift
cmd="$@"

check_table_and_rows() {
  local table_name="$1"
  local min_rows="$2"

  export PGPASSWORD="$POSTGRES_PASSWORD"
  row_count=$(psql -h "$host" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c "SELECT COUNT(*) FROM "$table_name";")

  if [[ "$row_count" -ge "$min_rows" ]]; then
    return 0
  else
    return 1
  fi
}

table1="example1"
table2="example2"
min_rows=4

until check_table_and_rows "$table1" "$min_rows" && check_table_and_rows "$table2" "$min_rows"; do
  >&2 echo "Waiting for the database tables to be initialized with enough rows..."
  sleep 2
done

>&2 echo "Postgres is up, both tables are ready and contain enough rows - executing command"
exec $cmd
