CREATE TABLE IF NOT EXISTS test_space.peerids (
    user_id bigint,
    chat_id bigint,
    id bigint,
    archived boolean,
    deleted boolean,
    disable_for bigint,
    invite_timestamp bigint,
    inviter_id bigint,
    last_message_id bigint,
    last_read_message_id bigint,
    PRIMARY KEY (user_id, chat_id, id)
) WITH ID = e0636780-f203-11f0-ba20-4ba96dd61616
    AND CLUSTERING ORDER BY (chat_id ASC, id ASC)
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
