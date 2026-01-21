#!/usr/bin/env python3
"""
Генератор INSERT-запросов для таблицы Cassandra Messages
Использование: python generate_messages.py --count 1000 --output messages_inserts.cql
"""

import argparse
import random
import uuid
import time
from datetime import datetime, timedelta
import json
import hashlib
from typing import List, Dict, Any

class MessageGenerator:
    def __init__(self, seed: int = 42):
        """Инициализация генератора с сидом для воспроизводимости"""
        random.seed(seed)

        # Статистика для правдоподобных данных
        self.users = list(range(1000, 1000000))  # 1M пользователей
        self.chats = list(range(1000, 500000))   # 500K чатов
        self.common_words = [
            "привет", "как", "дела", "нормально", "спасибо", "пока", "что", "где",
            "когда", "почему", "сегодня", "завтра", "вчера", "работа", "дом", "друзья",
            "встреча", "совещание", "проект", "задача", "срочно", "важно", "файл", "ссылка"
        ]

    def generate_message_id(self, chat_id: int, local_id: int) -> int:
        """Генерация chat_msg_local_id"""
        return local_id

    def generate_bucket(self, message_id: int) -> int:
        """Вычисление bucket = floor(chat_msg_local_id/1000)"""
        return message_id // 1000

    def generate_flags(self) -> int:
        """Генерация флагов сообщения"""
        # Флаги: 1=прочитано, 2=отредактировано, 4=удалено, 8=переслано, 16=ответ
        flags = 0
        if random.random() < 0.8:  # 80% прочитано
            flags |= 1
        if random.random() < 0.1:  # 10% отредактировано
            flags |= 2
        if random.random() < 0.02:  # 2% удалено
            flags |= 4
        if random.random() < 0.15:  # 15% переслано
            flags |= 8
        if random.random() < 0.3:  # 30% ответ
            flags |= 16
        return flags

    def generate_timestamp(self, base_date: datetime = None) -> int:
        """Генерация timestamp в секундах"""
        if base_date is None:
            base_date = datetime(2020, 1, 1)

        # Случайная дата за последние 3 года
        days_ago = random.randint(0, 3 * 365)
        random_date = base_date - timedelta(days=days_ago)

        # Добавляем случайное время
        random_time = timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )

        final_date = random_date + random_time
        return int(final_date.timestamp())

    def generate_text(self, min_words: int = 3, max_words: int = 50) -> str:
        """Генерация текста сообщения"""
        words_count = random.randint(min_words, max_words)

        # Случайный текст из общих слов
        words = []
        for _ in range(words_count):
            word = random.choice(self.common_words)
            # Случайно делаем некоторые слова с заглавной буквы
            if random.random() < 0.3:
                word = word.capitalize()
            words.append(word)

        # Добавляем знаки препинания
        text = ' '.join(words)
        if random.random() < 0.7:
            text += random.choice(['.', '!', '?'])

        return text

    def generate_kludges(self) -> str:
        """Генерация kludges (сжатых аттачей)"""
        # Имитируем JSON с медиа-данными
        kludge_types = ['photo', 'video', 'document', 'audio', 'voice', 'sticker']

        if random.random() < 0.3:  # 30% сообщений с медиа
            media_type = random.choice(kludge_types)
            kludge_data = {
                "type": media_type,
                "id": str(uuid.uuid4()),
                "size": random.randint(1024, 50 * 1024 * 1024),
                "url": f"https://cdn.example.com/{media_type}/{hashlib.md5(str(random.random()).encode()).hexdigest()[:8]}",
                "width": random.choice([1280, 1920, 2560]) if media_type in ['photo', 'video'] else None,
                "height": random.choice([720, 1080, 1440]) if media_type in ['photo', 'video'] else None,
                "duration": random.randint(1, 300) if media_type in ['video', 'audio', 'voice'] else None
            }
            return json.dumps(kludge_data, ensure_ascii=False)

        return "{}"

    def generate_forwarded_message_ids(self) -> List[int]:
        """Генерация списка пересланных сообщений"""
        if random.random() < 0.15:  # 15% сообщений пересланы
            count = random.randint(1, 3)
            return [random.randint(1000000, 9999999) for _ in range(count)]
        return []

    def generate_mentions(self) -> str:
        """Генерация типа упоминаний"""
        types = ['none', 'all', 'online', 'user']
        weights = [0.7, 0.1, 0.1, 0.1]  # 70% без упоминаний

        return random.choices(types, weights=weights, k=1)[0]

    def generate_marked_users(self, author_id: int) -> List[int]:
        """Генерация списка упомянутых пользователей"""
        if random.random() < 0.2:  # 20% сообщений с упоминаниями
            # Убедимся, что author_id не упоминает сам себя
            available_users = [u for u in random.sample(self.users, 10) if u != author_id]
            count = random.randint(1, min(5, len(available_users)))
            return random.sample(available_users, count)
        return []

    def generate_ttl(self) -> int:
        """Генерация TTL (в секундах)"""
        # 0 - без TTL, иначе срок от 1 часа до 30 дней
        if random.random() < 0.05:  # 5% сообщений с TTL
            return random.choice([
                3600,       # 1 час
                86400,      # 1 день
                604800,     # 1 неделя
                2592000     # 30 дней
            ])
        return 0

    def generate_message(self, message_idx: int, chat_id: int = None) -> Dict[str, Any]:
        """Генерация одного сообщения"""

        # Если chat_id не указан, выбираем случайный
        if chat_id is None:
            chat_id = random.choice(self.chats)

        # Генерируем message_id (локальный для чата)
        message_id = self.generate_message_id(chat_id, message_idx)

        # Автор сообщения
        author_id = random.choice(self.users)

        # Временные метки
        date = self.generate_timestamp()
        update_time = date
        if random.random() < 0.1:  # 10% сообщений были отредактированы
            update_time = date + random.randint(60, 3600)  # через 1 мин - 1 час

        # Генерируем данные
        text = self.generate_text()
        kludges = self.generate_kludges()
        forwarded = len(self.generate_forwarded_message_ids()) > 0
        forwarded_message_ids = self.generate_forwarded_message_ids()
        mentions = self.generate_mentions()
        marked_users = self.generate_marked_users(author_id)
        ttl = self.generate_ttl()
        deleted_for_all = random.random() < 0.01  # 1% сообщений удалены для всех
        flags = self.generate_flags()

        return {
            "chat_id": chat_id,
            "bucket": self.generate_bucket(message_id),
            "chat_msg_local_id": message_id,
            "flags": flags,
            "date": date,
            "update_time": update_time,
            "author_id": author_id,
            "text": text,
            "kludges": kludges,
            "forwarded": forwarded,
            "forwarded_message_ids": forwarded_message_ids,
            "mentions": mentions,
            "marked_users": marked_users,
            "ttl": ttl,
            "deleted_for_all": deleted_for_all
        }

    def generate_insert_statement(self, message_data: Dict[str, Any]) -> str:
        """Формирование INSERT-запроса для одного сообщения"""

        # Экранирование специальных символов в тексте
        text_escaped = message_data['text'].replace("'", "''")
        kludges_escaped = message_data['kludges'].replace("'", "''")
        mentions_escaped = message_data['mentions'].replace("'", "''")

        # Формирование списков для Cassandra
        forwarded_ids_str = '[]'
        if message_data['forwarded_message_ids']:
            ids = ', '.join(str(id) for id in message_data['forwarded_message_ids'])
            forwarded_ids_str = f'[{ids}]'

        marked_users_str = '[]'
        if message_data['marked_users']:
            users = ', '.join(str(user) for user in message_data['marked_users'])
            marked_users_str = f'[{users}]'

        insert = f"""
INSERT INTO Messages (
    chat_id, bucket, chat_msg_local_id, flags, date, update_time,
    author_id, text, kludges, forwarded, forwarded_message_ids,
    mentions, marked_users, ttl, deleted_for_all
) VALUES (
    {message_data['chat_id']},
    {message_data['bucket']},
    {message_data['chat_msg_local_id']},
    {message_data['flags']},
    {message_data['date']},
    {message_data['update_time']},
    {message_data['author_id']},
    '{text_escaped}',
    '{kludges_escaped}',
    {str(message_data['forwarded']).lower()},
    {forwarded_ids_str},
    '{mentions_escaped}',
    {marked_users_str},
    {message_data['ttl']},
    {str(message_data['deleted_for_all']).lower()}
);"""

        return insert.strip()

    def generate_batch_insert(self, messages_data: List[Dict[str, Any]]) -> str:
        """Формирование BATCH-запроса для нескольких сообщений"""
        if not messages_data:
            return ""

        batch = "BEGIN BATCH\n"

        for msg in messages_data:
            insert = self.generate_insert_statement(msg)
            # Убираем точку с запятой из отдельных INSERT, так как она будет в конце BATCH
            insert = insert.rstrip(';')
            batch += f"    {insert}\n"

        batch += "APPLY BATCH;"
        return batch

    def generate_file(self, count: int, output_file: str,
                     batch_size: int = 100, use_batch: bool = True,
                     chat_id: int = None) -> None:
        """Генерация файла с INSERT-запросами"""

        print(f"Генерация {count} сообщений в файл {output_file}")
        print(f"Режим: {'BATCH (размер батча: ' + str(batch_size) + ')' if use_batch else 'отдельные INSERT'}")

        start_time = time.time()
        messages_generated = 0

        with open(output_file, 'w', encoding='utf-8') as f:
            # Записываем заголовок
            f.write("-- Генерация тестовых данных для таблицы Messages\n")
            f.write(f"-- Количество сообщений: {count}\n")
            f.write(f"-- Время генерации: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("USE test_space;\n\n")

            if use_batch:
                # Генерация батчами
                for batch_start in range(0, count, batch_size):
                    batch_end = min(batch_start + batch_size, count)
                    batch_count = batch_end - batch_start

                    # Генерируем сообщения для батча
                    batch_messages = []
                    for i in range(batch_count):
                        msg_idx = messages_generated + i
                        message = self.generate_message(msg_idx, chat_id)
                        batch_messages.append(message)

                    # Формируем BATCH запрос
                    batch_query = self.generate_batch_insert(batch_messages)
                    f.write(batch_query + "\n\n")

                    messages_generated += batch_count

                    # Прогресс
                    if batch_end % (batch_size * 10) == 0 or batch_end == count:
                        elapsed = time.time() - start_time
                        rate = messages_generated / elapsed if elapsed > 0 else 0
                        print(f"  Сгенерировано: {messages_generated}/{count} ({messages_generated/count*100:.1f}%), "
                              f"скорость: {rate:.1f} msg/sec")
            else:
                # Генерация отдельных INSERT
                for i in range(count):
                    message = self.generate_message(i, chat_id)
                    insert_query = self.generate_insert_statement(message)
                    f.write(insert_query + "\n")

                    messages_generated += 1

                    # Прогресс
                    if i % 1000 == 0 or i == count - 1:
                        elapsed = time.time() - start_time
                        rate = messages_generated / elapsed if elapsed > 0 else 0
                        print(f"  Сгенерировано: {messages_generated}/{count} ({messages_generated/count*100:.1f}%), "
                              f"скорость: {rate:.1f} msg/sec")

        elapsed = time.time() - start_time
        print(f"\n✓ Генерация завершена!")
        print(f"  Всего сообщений: {messages_generated}")
        print(f"  Затраченное время: {elapsed:.2f} сек")
        print(f"  Средняя скорость: {messages_generated/elapsed:.1f} msg/sec")
        print(f"  Файл сохранен: {output_file}")

        # Создаем файл с примерами для проверки
        self.create_sample_file(output_file)

    def create_sample_file(self, source_file: str) -> None:
        """Создание файла с примерами первых запросов"""
        sample_file = source_file.replace('.cql', '_sample.cql')

        try:
            with open(source_file, 'r', encoding='utf-8') as f:
                lines = []
                for i, line in enumerate(f):
                    if i < 20:  # Первые 20 строк
                        lines.append(line)
                    else:
                        break

            with open(sample_file, 'w', encoding='utf-8') as f:
                f.writelines(lines)
                f.write("\n-- ... и так далее ...\n")

            print(f"  Примеры запросов: {sample_file}")
        except Exception as e:
            print(f"  Не удалось создать файл с примерами: {e}")

def main():
    parser = argparse.ArgumentParser(description='Генератор INSERT-запросов для Cassandra Messages')
    parser.add_argument('--count', type=int, default=1000,
                       help='Количество сообщений для генерации (по умолчанию: 1000)')
    parser.add_argument('--output', type=str, default='messages_inserts.cql',
                       help='Имя выходного файла (по умолчанию: messages_inserts.cql)')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Размер батча для BATCH запросов (по умолчанию: 100)')
    parser.add_argument('--no-batch', action='store_true',
                       help='Использовать отдельные INSERT вместо BATCH')
    parser.add_argument('--chat-id', type=int,
                       help='Фиксированный chat_id для всех сообщений')
    parser.add_argument('--seed', type=int, default=42,
                       help='Seed для случайного генератора (по умолчанию: 42)')
    parser.add_argument('--create-table', action='store_true',
                       help='Добавить CREATE TABLE в начало файла')

    args = parser.parse_args()

    # Создаем генератор
    generator = MessageGenerator(seed=args.seed)

    # Добавляем CREATE TABLE если нужно
    if args.create_table:
        create_table_sql = """
CREATE TABLE IF NOT EXISTS Messages (
    chat_id bigint,
    bucket int,
    chat_msg_local_id bigint,
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
    PRIMARY KEY ((chat_id, bucket), chat_msg_local_id)
) WITH CLUSTERING ORDER BY (chat_msg_local_id DESC);

"""
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(create_table_sql)

    # Генерируем данные
    generator.generate_file(
        count=args.count,
        output_file=args.output,
        batch_size=args.batch_size,
        use_batch=not args.no_batch,
        chat_id=args.chat_id
    )

if __name__ == "__main__":
    main()