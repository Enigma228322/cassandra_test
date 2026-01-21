#!/bin/bash

docker cp messenger.cql cassandra:/tmp/
docker exec cassandra cqlsh -f /tmp/messenger.cql
