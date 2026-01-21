-- Создание ключевого пространства
CREATE KEYSPACE IF NOT EXISTS test_space
WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

USE test_space;

-- Создание таблиц
CREATE TABLE IF NOT EXISTS PeerIds (
    id bigint,
    user_id bigint,
    chat_id bigint,
    invite_timestamp bigint,
    disable_for bigint,
    archived boolean,
    deleted boolean,
    inviter_id bigint,
    last_read_message_id bigint,
    last_message_id bigint,
    PRIMARY KEY ((user_id), chat_id, id)
) WITH CLUSTERING ORDER BY (chat_id ASC, id ASC);

CREATE TABLE IF NOT EXISTS Chats (
    id bigint PRIMARY KEY,
    members list<bigint>,
    pinned_message_ids list<bigint>,
    photo text,
    secret text,
    members_count int,
    description text,
    big_chat_or_channel boolean,
    no_search boolean,
    deleted_for_all boolean
);

CREATE TABLE IF NOT EXISTS Messages (
    chat_id bigint,
    id bigint,
    flags bigint,
    date bigint,
    update_time bigint,
    author_id bigint,
    text text,
    kludges text,
    forwarded boolean,
    forwarded_message_ids list<bigint>,
    mentions text,
    marked_users list<bigint>,
    ttl bigint,
    deleted_for_all boolean,
    PRIMARY KEY ((chat_id), id)
) WITH CLUSTERING ORDER BY (id DESC);

CREATE TABLE IF NOT EXISTS UserToMessage (
    user_id bigint,
    message_id bigint,
    peer_id bigint,
    author_flags bigint,
    unread boolean,
    deleted boolean,
    PRIMARY KEY ((user_id, peer_id), message_id)
) WITH CLUSTERING ORDER BY (message_id DESC);

CREATE TABLE IF NOT EXISTS MessageReadStatus (
    message_id bigint,
    chat_id bigint,
    user_id bigint,
    read_at timestamp,
    PRIMARY KEY ((message_id, chat_id), user_id)
);

-- Наполнение данными

-- Чат 1: Личный чат между двумя пользователями
INSERT INTO Chats (id, members, pinned_message_ids, photo, secret, members_count, description, big_chat_or_channel, no_search, deleted_for_all)
VALUES (
    1001,
    [1001, 1002],
    [5001],
    'https://cdn.example.com/photo1.jpg',
    'secret1234567890',
    2,
    'Личный чат',
    false,
    false,
    false
);

-- Чат 2: Групповой чат
INSERT INTO Chats (id, members, pinned_message_ids, photo, secret, members_count, description, big_chat_or_channel, no_search, deleted_for_all)
VALUES (
    1002,
    [1001, 1002, 1003, 1004, 1005],
    [5002, 5003],
    'https://cdn.example.com/photo2.jpg',
    'secret0987654321',
    5,
    'Группа друзей',
    false,
    false,
    false
);

-- Чат 3: Канал (большой чат)
INSERT INTO Chats (id, members, pinned_message_ids, photo, secret, members_count, description, big_chat_or_channel, no_search, deleted_for_all)
VALUES (
    1003,
    [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008],
    [],
    'https://cdn.example.com/photo3.jpg',
    'secret1122334455',
    8,
    'Новостной канал',
    true,
    false,
    false
);

-- Пользователь 1001 в чате 1001
INSERT INTO PeerIds (id, user_id, chat_id, invite_timestamp, disable_for, archived, deleted, inviter_id, last_read_message_id, last_message_id)
VALUES (
    1,
    1001,
    1001,
    1737036000,
    0,
    false,
    false,
    null,
    5005,
    5010
);

-- Пользователь 1002 в чате 1001
INSERT INTO PeerIds (id, user_id, chat_id, invite_timestamp, disable_for, archived, deleted, inviter_id, last_read_message_id, last_message_id)
VALUES (
    2,
    1002,
    1001,
    1737036100,
    0,
    false,
    false,
    1001,
    5003,
    5010
);

-- Пользователь 1001 в чате 1002
INSERT INTO PeerIds (id, user_id, chat_id, invite_timestamp, disable_for, archived, deleted, inviter_id, last_read_message_id, last_message_id)
VALUES (
    3,
    1001,
    1002,
    1737036200,
    0,
    false,
    false,
    null,
    5008,
    5015
);

-- Пользователь 1003 в чате 1002
INSERT INTO PeerIds (id, user_id, chat_id, invite_timestamp, disable_for, archived, deleted, inviter_id, last_read_message_id, last_message_id)
VALUES (
    4,
    1003,
    1002,
    1737036300,
    3600,
    false,
    false,
    1001,
    5007,
    5015
);

-- Сообщение 1 в чате 1001
INSERT INTO Messages (chat_id, id, flags, date, update_time, author_id, text, kludges, forwarded, forwarded_message_ids, mentions, marked_users, ttl, deleted_for_all)
VALUES (
    1001,
    5001,
    1,
    1737037000,
    1737037010,
    1001,
    'Привет! Как дела?',
    'compressed_data_1',
    false,
    [],
    'none',
    [],
    0,
    false
);

-- Сообщение 2 в чате 1001
INSERT INTO Messages (chat_id, id, flags, date, update_time, author_id, text, kludges, forwarded, forwarded_message_ids, mentions, marked_users, ttl, deleted_for_all)
VALUES (
    1001,
    5002,
    2,
    1737037100,
    1737037110,
    1002,
    'Привет! Все хорошо, а у тебя?',
    'compressed_data_2',
    false,
    [],
    'none',
    [],
    0,
    false
);

-- Сообщение в чате 1002
INSERT INTO Messages (chat_id, id, flags, date, update_time, author_id, text, kludges, forwarded, forwarded_message_ids, mentions, marked_users, ttl, deleted_for_all)
VALUES (
    1002,
    5003,
    4,
    1737037200,
    1737037210,
    1001,
    '@all Всем привет! Собрание в 15:00',
    'compressed_data_3',
    false,
    [],
    'all',
    [1001, 1002, 1003, 1004, 1005],
    0,
    false
);

-- Пересланное сообщение
INSERT INTO Messages (chat_id, id, flags, date, update_time, author_id, text, kludges, forwarded, forwarded_message_ids, mentions, marked_users, ttl, deleted_for_all)
VALUES (
    1002,
    5004,
    8,
    1737037300,
    1737037310,
    1002,
    'Смотри, что прислали',
    'compressed_data_4',
    true,
    [3001, 3002, 3003],
    'none',
    [],
    0,
    false
);

-- Пользователь 1001 читает свои сообщения в чате 1001
INSERT INTO UserToMessage (user_id, peer_id, message_id, author_flags, unread, deleted)
VALUES (
    1001,
    1001,
    5001,
    1,
    false,
    false
);

-- Пользователь 1002 читает сообщение 1001 в чате 1001
INSERT INTO UserToMessage (user_id, peer_id, message_id, author_flags, unread, deleted)
VALUES (
    1002,
    1001,
    5001,
    1,
    false,
    false
);

-- Пользователь 1002 не прочитал свое сообщение
INSERT INTO UserToMessage (user_id, peer_id, message_id, author_flags, unread, deleted)
VALUES (
    1002,
    1001,
    5002,
    2,
    true,
    false
);

-- Пользователь 1001 видит сообщение в групповом чате
INSERT INTO UserToMessage (user_id, peer_id, message_id, author_flags, unread, deleted)
VALUES (
    1001,
    1002,
    5003,
    4,
    false,
    false
);

-- Пользователь 1002 прочитал сообщение 5001 в чате 1001
INSERT INTO MessageReadStatus (message_id, chat_id, user_id, read_at)
VALUES (
    5001,
    1001,
    1002,
    '2026-01-15 15:30:00'
);

-- Пользователь 1003 прочитал сообщение 5003 в чате 1002
INSERT INTO MessageReadStatus (message_id, chat_id, user_id, read_at)
VALUES (
    5003,
    1002,
    1003,
    '2026-01-15 15:30:00'
);

-- Пользователь 1001 прочитал свое сообщение
INSERT INTO MessageReadStatus (message_id, chat_id, user_id, read_at)
VALUES (
    5001,
    1001,
    1001,
    '2026-01-15 15:25:00'
);

-- Дополнительные данные (массовая вставка)
BEGIN BATCH
INSERT INTO Messages (chat_id, id, flags, date, author_id, text, forwarded, mentions, deleted_for_all) VALUES (1001, 5010, 1, 1737038000, 1001, 'Сообщение 1', false, 'none', false);
INSERT INTO Messages (chat_id, id, flags, date, author_id, text, forwarded, mentions, deleted_for_all) VALUES (1001, 5011, 1, 1737038100, 1001, 'Сообщение 2', false, 'none', false);
INSERT INTO Messages (chat_id, id, flags, date, author_id, text, forwarded, mentions, deleted_for_all) VALUES (1001, 5012, 1, 1737038200, 1002, 'Сообщение 3', false, 'none', false);
APPLY BATCH;

BEGIN BATCH
INSERT INTO UserToMessage (user_id, peer_id, message_id, author_flags, unread, deleted) VALUES (1001, 1001, 5010, 1, false, false);
INSERT INTO UserToMessage (user_id, peer_id, message_id, author_flags, unread, deleted) VALUES (1002, 1001, 5010, 1, false, false);
INSERT INTO UserToMessage (user_id, peer_id, message_id, author_flags, unread, deleted) VALUES (1001, 1001, 5011, 1, false, false);
APPLY BATCH;

-- Создание индексов
CREATE INDEX IF NOT EXISTS chats_secret_idx ON Chats (secret);

-- Проверка создания таблиц
SELECT keyspace_name, table_name FROM system_schema.tables WHERE keyspace_name = 'test_space';