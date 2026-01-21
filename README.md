#!/bin/bash

# Up cassandra
docker compose up -d
# Go to cassandra cli
docker exec -it cassandra cqlsh

# Up cassandra
# CREATE TABLE test_space.messages (
#     chat_id bigint,
#     bucket int,
#     chat_msg_local_id bigint,
#     author_id bigint,
#     date bigint,
#     deleted_for_all boolean,
#     flags bigint,
#     forwarded boolean,
#     kludges text,
#     mentions text,
#     text text,
#     ttl bigint,
#     update_time bigint,
#     forwarded_message_ids list<bigint>,
#     marked_users list<bigint>,
#     PRIMARY KEY ((chat_id, bucket), chat_msg_local_id)
# ) WITH CLUSTERING ORDER BY (chat_msg_local_id DESC)

# exit from docker
exit
# Gen csv file with data to insert to cassandra
python3 dsbulk_generate.py --count $RECORDS_NUMBER --output $FILE_NAME
# Copy to docker
docker cp $FILE_NAME cassandra:/tmp/

# Go to cassandra docker
docker exec -it cassandra bash
# Install dependencies
apt install nodetool curl
curl -L https://downloads.datastax.com/dsbulk/dsbulk-1.11.0.tar.gz | tar xz -C /opt
# Command in docker to load data in cassandra table
dsbulk load -url /tmp/{$FILE_NAME} -k $KEYSPACE -t $TABLE -header true
# Command in docker flush sstable + compact + write table stats
nodetool flush test_space messages && nodetool compact && nodetool tablestats {$KEYSPACE}.{$TABLE}

# Get "Space used (total)" value and insert to the cassandra_stats.csv like "records_num,size_in_bytes"
# 1000,233561
# 2000,456033
# 4000,919431
# 8000,1828982
# 32000,7302675
# 64000,14629779
# 128000,29331968
# 256000,49986516
# 512000,100415489
# 1024000,202302796

# Make plot and look at regression polynoms formula
python3 total_plotter.py