#!/usr/bin/env python3
"""
Генератор INSERT-запросов для Cassandra БЕЗ использования BATCH
Оптимизирован для больших объемов данных
"""

import argparse
import random
import uuid
import time
from datetime import datetime, timedelta
import json
import hashlib
import os
from typing import List, Dict, Any

class NoBatchMessageGenerator:
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

        # Метрики для мониторинга
        self.metrics = {
            'messages_generated': 0,
            'files_created': 0,
            'total_size_bytes': 0,
            'start_time': None,
            'end_time': None
        }

    def generate_message_id(self, chat_id: int, local_id: int) -> int:
        """Генерация chat_msg_local_id"""
        return local_id

    def generate_bucket(self, message_id: int) -> int:
        """Вычисление bucket = floor(chat_msg_local_id/1000)"""
        return message_id // 1000

    def generate_flags(self) -> int:
        """Генерация флагов сообщения"""
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

        # Случайная дата за последние 3 лет
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

        words = []
        for _ in range(words_count):
            word = random.choice(self.common_words)
            if random.random() < 0.3:
                word = word.capitalize()
            words.append(word)

        text = ' '.join(words)
        if random.random() < 0.7:
            text += random.choice(['.', '!', '?'])

        return text

    def generate_kludges(self) -> str:
        """Генерация kludges (сжатых аттачей)"""
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
        weights = [0.7, 0.1, 0.1, 0.1]

        return random.choices(types, weights=weights, k=1)[0]

    def generate_marked_users(self, author_id: int) -> List[int]:
        """Генерация списка упомянутых пользователей"""
        if random.random() < 0.2:  # 20% сообщений с упоминаниями
            available_users = [u for u in random.sample(self.users, 10) if u != author_id]
            count = random.randint(1, min(5, len(available_users)))
            return random.sample(available_users, count)
        return []

    def generate_ttl(self) -> int:
        """Генерация TTL (в секундах)"""
        if random.random() < 0.05:  # 5% сообщений с TTL
            return random.choice([3600, 86400, 604800, 2592000])
        return 0

    def generate_message(self, message_idx: int, chat_id: int = None) -> Dict[str, Any]:
        """Генерация одного сообщения"""
        if chat_id is None:
            chat_id = random.choice(self.chats)

        message_id = self.generate_message_id(chat_id, message_idx)
        author_id = random.choice(self.users)

        date = self.generate_timestamp()
        update_time = date
        if random.random() < 0.1:
            update_time = date + random.randint(60, 3600)

        text = self.generate_text()
        kludges = self.generate_kludges()
        forwarded = len(self.generate_forwarded_message_ids()) > 0
        forwarded_message_ids = self.generate_forwarded_message_ids()
        mentions = self.generate_mentions()
        marked_users = self.generate_marked_users(author_id)
        ttl = self.generate_ttl()
        deleted_for_all = random.random() < 0.01
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

    def escape_sql_value(self, value: Any) -> str:
        """Экранирование значения для SQL"""
        if value is None:
            return 'NULL'
        elif isinstance(value, bool):
            return str(value).lower()
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, str):
            # Экранируем одинарные кавычки
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, list):
            # Для списков (Cassandra list)
            items = [str(item) for item in value]
            return f"[{', '.join(items)}]"
        else:
            return f"'{str(value)}'"

    def generate_insert_statement(self, message_data: Dict[str, Any]) -> str:
        """Формирование отдельного INSERT-запроса"""

        # Экранируем все значения
        values = {
            'chat_id': message_data['chat_id'],
            'bucket': message_data['bucket'],
            'chat_msg_local_id': message_data['chat_msg_local_id'],
            'flags': message_data['flags'],
            'date': message_data['date'],
            'update_time': message_data['update_time'],
            'author_id': message_data['author_id'],
            'text': self.escape_sql_value(message_data['text']),
            'kludges': self.escape_sql_value(message_data['kludges']),
            'forwarded': message_data['forwarded'],
            'forwarded_message_ids': message_data['forwarded_message_ids'],
            'mentions': self.escape_sql_value(message_data['mentions']),
            'marked_users': message_data['marked_users'],
            'ttl': message_data['ttl'],
            'deleted_for_all': message_data['deleted_for_all']
        }

        # Формируем INSERT
        columns = ', '.join(values.keys())

        # Форматируем значения
        values_list = []
        for key, value in values.items():
            if key in ['forwarded_message_ids', 'marked_users']:
                # Списки нужно специально форматировать
                if value:
                    values_list.append(f'[{", ".join(str(v) for v in value)}]')
                else:
                    values_list.append('[]')
            else:
                values_list.append(str(value) if isinstance(value, (int, bool)) else value)

        values_str = ', '.join(values_list)

        insert = f"INSERT INTO Messages ({columns}) VALUES ({values_str});"
        return insert

    def generate_file(self, count: int, output_file: str,
                     chunk_size: int = 10000, chat_id: int = None) -> None:
        """Генерация файла с отдельными INSERT-запросами"""

        self.metrics['start_time'] = time.time()

        print(f"Генерация {count} сообщений в файл {output_file}")
        print(f"Режим: отдельные INSERT-запросы")
        print(f"Размер чанка для отслеживания: {chunk_size}")

        # Если файл существует, спрашиваем подтверждение
        if os.path.exists(output_file):
            response = input(f"Файл {output_file} уже существует. Перезаписать? (y/N): ")
            if response.lower() != 'y':
                print("Отменено пользователем")
                return

        total_generated = 0
        chunk_count = 0

        # Открываем файл для записи
        with open(output_file, 'w', encoding='utf-8') as f:
            # Записываем заголовок
            f.write("-- Генерация тестовых данных для таблицы Messages (без BATCH)\n")
            f.write(f"-- Количество сообщений: {count}\n")
            f.write(f"-- Время генерации: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("USE test_space;\n\n")

            # Генерация сообщений
            for i in range(count):
                message = self.generate_message(i, chat_id)
                insert_query = self.generate_insert_statement(message)

                f.write(insert_query + "\n")
                total_generated += 1

                # Периодически показываем прогресс
                if total_generated % chunk_size == 0:
                    chunk_count += 1
                    elapsed = time.time() - self.metrics['start_time']
                    rate = total_generated / elapsed if elapsed > 0 else 0

                    # Оцениваем размер файла
                    current_pos = f.tell()
                    estimated_total = current_pos * (count / total_generated)

                    print(f"  Прогресс: {total_generated:,}/{count:,} "
                          f"({total_generated/count*100:.1f}%), "
                          f"скорость: {rate:.1f} msg/sec, "
                          f"размер файла: {current_pos/1024/1024:.1f} MB")

        self.metrics['end_time'] = time.time()
        self.metrics['messages_generated'] = total_generated

        # Получаем финальный размер файла
        file_size = os.path.getsize(output_file)
        self.metrics['total_size_bytes'] = file_size

        self.print_summary(file_size)

    def generate_multiple_files(self, count: int, output_dir: str,
                               records_per_file: int = 100000,
                               chat_id: int = None) -> None:
        """Генерация нескольких файлов для больших объемов"""

        print(f"Генерация {count} сообщений в директорию {output_dir}")
        print(f"Сообщений на файл: {records_per_file}")
        print(f"Количество файлов: {count // records_per_file + (1 if count % records_per_file else 0)}")

        # Создаем директорию если не существует
        os.makedirs(output_dir, exist_ok=True)

        total_generated = 0
        file_count = 0

        while total_generated < count:
            file_count += 1
            records_in_file = min(records_per_file, count - total_generated)
            output_file = os.path.join(output_dir, f"messages_part_{file_count:04d}.cql")

            print(f"\nГенерация файла {file_count}: {output_file}")
            print(f"  Записей в файле: {records_in_file}")

            start_time = time.time()

            with open(output_file, 'w', encoding='utf-8') as f:
                # Записываем заголовок
                f.write(f"-- Файл {file_count}: {records_in_file} сообщений\n")
                f.write(f"-- Время генерации: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("-- USE your_keyspace;\n\n")

                for i in range(records_in_file):
                    message = self.generate_message(total_generated + i, chat_id)
                    insert_query = self.generate_insert_statement(message)
                    f.write(insert_query + "\n")

            elapsed = time.time() - start_time
            file_size = os.path.getsize(output_file)

            print(f"  ✓ Файл создан: {file_size/1024/1024:.1f} MB, "
                  f"время: {elapsed:.1f} сек, "
                  f"скорость: {records_in_file/elapsed:.1f} msg/sec")

            total_generated += records_in_file

        print(f"\n✓ Все файлы созданы!")
        print(f"  Всего файлов: {file_count}")
        print(f"  Всего сообщений: {total_generated}")
        print(f"  Директория: {output_dir}")

    def generate_optimized_inserts(self, count: int, output_file: str,
                                  chat_id: int = None,
                                  progress_interval: int = 1000) -> None:
        """Оптимизированная генерация с буферизацией"""

        print(f"Оптимизированная генерация {count} сообщений")
        print(f"Буферизация запросов в памяти")

        BUFFER_SIZE = 1000  # Записей в буфере
        buffer = []

        start_time = time.time()

        with open(output_file, 'w', encoding='utf-8') as f:
            # Заголовок
            f.write("-- Оптимизированная генерация данных\n")
            f.write(f"-- Количество: {count}\n\n")

            for i in range(count):
                message = self.generate_message(i, chat_id)
                insert_query = self.generate_insert_statement(message)
                buffer.append(insert_query)

                # Когда буфер заполнен, записываем в файл
                if len(buffer) >= BUFFER_SIZE:
                    f.write('\n'.join(buffer) + '\n')
                    buffer.clear()

                # Показываем прогресс
                if (i + 1) % progress_interval == 0:
                    elapsed = time.time() - start_time
                    rate = (i + 1) / elapsed
                    print(f"  Сгенерировано: {i + 1:,}/{count:,} "
                          f"({(i + 1)/count*100:.1f}%), "
                          f"скорость: {rate:.1f} msg/sec")

            # Записываем остатки из буфера
            if buffer:
                f.write('\n'.join(buffer) + '\n')

        elapsed = time.time() - start_time
        file_size = os.path.getsize(output_file)

        print(f"\n✓ Генерация завершена!")
        print(f"  Время: {elapsed:.1f} сек")
        print(f"  Средняя скорость: {count/elapsed:.1f} msg/sec")
        print(f"  Размер файла: {file_size/1024/1024:.1f} MB")

    def print_summary(self, file_size_bytes: int = None):
        """Вывод сводки генерации"""
        elapsed = self.metrics['end_time'] - self.metrics['start_time']

        print("\n" + "="*60)
        print("СВОДКА ГЕНЕРАЦИИ")
        print("="*60)
        print(f"Сообщений сгенерировано: {self.metrics['messages_generated']:,}")
        print(f"Затраченное время: {elapsed:.2f} секунд")
        print(f"Средняя скорость: {self.metrics['messages_generated']/elapsed:.1f} msg/sec")

        if file_size_bytes:
            size_mb = file_size_bytes / (1024 * 1024)
            size_per_record = file_size_bytes / self.metrics['messages_generated']
            print(f"Размер файла: {size_mb:.2f} MB")
            print(f"Средний размер записи: {size_per_record:.0f} байт")

        print("\n✓ РЕКОМЕНДАЦИИ ПО ИСПОЛЬЗОВАНИЮ:")
        print("  Для загрузки в Cassandra используйте:")
        print("  1. DSBulk (рекомендуется):")
        print("     dsbulk load -url data.csv -k keyspace -t table")
        print("  2. COPY команду в cqlsh (для небольших объемов):")
        print("     COPY keyspace.table FROM 'data.csv' WITH HEADER=true")
        print("  3. Apache Spark для распределенной загрузки")

def main():
    parser = argparse.ArgumentParser(
        description='Генератор INSERT-запросов для Cassandra (без BATCH)'
    )
    parser.add_argument('--count', type=int, default=1000,
                       help='Количество сообщений для генерации')
    parser.add_argument('--output', type=str, default='messages_no_batch.cql',
                       help='Имя выходного файла')
    parser.add_argument('--output-dir', type=str,
                       help='Директория для выходных файлов (многопоточная генерация)')
    parser.add_argument('--records-per-file', type=int, default=100000,
                       help='Сообщений на файл при многопоточной генерации')
    parser.add_argument('--chat-id', type=int,
                       help='Фиксированный chat_id для всех сообщений')
    parser.add_argument('--seed', type=int, default=42,
                       help='Seed для случайного генератора')
    parser.add_argument('--create-table', action='store_true',
                       help='Добавить CREATE TABLE в начало файла')
    parser.add_argument('--chunk-size', type=int, default=10000,
                       help='Частота вывода прогресса')
    parser.add_argument('--optimized', action='store_true',
                       help='Использовать оптимизированный режим с буферизацией')

    args = parser.parse_args()

    # Создаем генератор
    generator = NoBatchMessageGenerator(seed=args.seed)

    # Добавляем CREATE TABLE если нужно
    if args.create_table and not args.output_dir:
        create_table_sql = """CREATE TABLE IF NOT EXISTS Messages (
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

    # Выбираем режим генерации
    if args.output_dir:
        # Многопоточная генерация в несколько файлов
        generator.generate_multiple_files(
            count=args.count,
            output_dir=args.output_dir,
            records_per_file=args.records_per_file,
            chat_id=args.chat_id
        )
    elif args.optimized:
        # Оптимизированная генерация
        generator.generate_optimized_inserts(
            count=args.count,
            output_file=args.output,
            chat_id=args.chat_id,
            progress_interval=args.chunk_size
        )
    else:
        # Стандартная генерация
        generator.generate_file(
            count=args.count,
            output_file=args.output,
            chunk_size=args.chunk_size,
            chat_id=args.chat_id
        )

if __name__ == "__main__":
    main()