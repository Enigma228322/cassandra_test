CREATE TABLE IF NOT EXISTS test_space.messages (
    chat_id bigint,
    bucket int,
    chat_msg_local_id bigint,
    author_id bigint,
    date bigint,
    deleted_for_all boolean,
    flags bigint,
    forwarded boolean,
    kludges text,
    mentions text,
    text text,
    ttl bigint,
    update_time bigint,
    forwarded_message_ids list<bigint>,
    marked_users list<bigint>,
    PRIMARY KEY ((chat_id, bucket), chat_msg_local_id)
) WITH ID = 5c0366e0-f6ac-11f0-bb52-05fb7d69dfe7
    AND CLUSTERING ORDER BY (chat_msg_local_id DESC)
    AND additional_write_policy = '99p'
    AND allow_auto_snapshot = true
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND memtable = 'default'
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND extensions = {}
    AND gc_grace_seconds = 864000
    AND incremental_backups = true
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair = 'BLOCKING'
    AND speculative_retry = '99p';
