#!/usr/bin/env python3
"""
Генератор CSV файлов для загрузки в Cassandra через DSBulk
"""

import argparse
import random
import uuid
import time
from datetime import datetime, timedelta
import json
import hashlib
import os
import csv
from typing import List, Dict, Any

class CSVDsbulkGenerator:
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

    def generate_text(self, min_words: int = 1, max_words: int = 2) -> str:
        """Генерация текста сообщения"""
        words_count = 1 # = random.randint(min_words, max_words)

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
        return ""
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

    def generate_forwarded_message_ids(self) -> str:
        """Генерация списка пересланных сообщений для CSV"""
        if random.random() < 0.15:  # 15% сообщений пересланы
            count = random.randint(1, 3)
            ids = [str(random.randint(1000000, 9999999)) for _ in range(count)]
            return '[' + ','.join(ids) + ']'
        return '[]'

    def generate_mentions(self) -> str:
        """Генерация типа упоминаний"""
        types = ['none', 'all', 'online', 'user']
        weights = [0.7, 0.1, 0.1, 0.1]

        return random.choices(types, weights=weights, k=1)[0]

    def generate_marked_users(self, author_id: int) -> str:
        """Генерация списка упомянутых пользователей для CSV"""
        if random.random() < 0.2:  # 20% сообщений с упоминаниями
            available_users = [u for u in random.sample(self.users, 10) if u != author_id]
            count = random.randint(1, min(5, len(available_users)))
            users = random.sample(available_users, count)
            return '[' + ','.join(str(u) for u in users) + ']'
        return '[]'

    def generate_ttl(self) -> int:
        """Генерация TTL (в секундах)"""
        if random.random() < 0.05:  # 5% сообщений с TTL
            return random.choice([3600, 86400, 604800, 2592000])
        return 0

    def escape_csv_value(self, value: Any) -> str:
        """Экранирование значения для CSV с правильной обработкой JSON"""
        if value is None:
            return ''
        elif isinstance(value, bool):
            return str(value).lower()
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, str):
            # Специальная обработка для строк, которые уже являются JSON
            # Определяем по начальным символам
            if value.strip().startswith('{') and value.strip().endswith('}'):
                # Это JSON: удваиваем все кавычки внутри строки
                escaped = value.replace('"', '""')
                return f'"{escaped}"'
            # Обычная строка: экранируем только если есть спецсимволы
            if ',' in value or '\n' in value or '"' in value:
                # Удваиваем существующие кавычки
                escaped = value.replace('"', '""')
                return f'"{escaped}"'
            return value
        else:
            # Для списков и других типов возвращаем строковое представление
            str_value = str(value)
            # Экранируем кавычки в строковом представлении
            if '"' in str_value or ',' in str_value or '\n' in str_value:
                escaped = str_value.replace('"', '""')
                return f'"{escaped}"'
            return str_value

    def generate_message_row(self, message_idx: int, chat_id: int = None) -> Dict[str, Any]:
        """Генерация одной строки данных для CSV"""
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
        forwarded = random.random() < 0.15  # 15% пересланы
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
            "text": self.escape_csv_value(text),
            "kludges": self.escape_csv_value(kludges),
            "forwarded": str(forwarded).lower(),
            "forwarded_message_ids": forwarded_message_ids,
            "mentions": mentions,
            "marked_users": marked_users,
            "ttl": ttl,
            "deleted_for_all": str(deleted_for_all).lower()
        }

    def generate_csv_file(self, count: int, output_file: str,
                         chunk_size: int = 10000, chat_id: int = None) -> None:
        """Генерация одного CSV файла"""

        self.metrics['start_time'] = time.time()

        print(f"Генерация {count} сообщений в CSV файл {output_file}")
        print(f"Формат: DSBulk compatible CSV")
        print(f"Размер чанка для отслеживания: {chunk_size}")

        # Если файл существует, спрашиваем подтверждение
        if os.path.exists(output_file):
            response = input(f"Файл {output_file} уже существует. Перезаписать? (y/N): ")
            if response.lower() != 'y':
                print("Отменено пользователем")
                return

        total_generated = 0
        chunk_count = 0

        # Определяем порядок колонок для CSV
        fieldnames = [
            "chat_id", "bucket", "chat_msg_local_id", "flags", "date",
            "update_time", "author_id", "text", "kludges", "forwarded",
            "forwarded_message_ids", "mentions", "marked_users", "ttl",
            "deleted_for_all"
        ]

        # Открываем файл для записи CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Записываем заголовок
            writer.writeheader()

            # Генерация сообщений
            for i in range(count):
                row = self.generate_message_row(i, chat_id)
                writer.writerow(row)
                total_generated += 1

                # Периодически показываем прогресс
                if total_generated % chunk_size == 0:
                    chunk_count += 1
                    elapsed = time.time() - self.metrics['start_time']
                    rate = total_generated / elapsed if elapsed > 0 else 0

                    # Оцениваем размер файла
                    current_pos = csvfile.tell()
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

    def generate_multiple_csv_files(self, count: int, output_dir: str,
                                   records_per_file: int = 100000,
                                   chat_id: int = None) -> None:
        """Генерация нескольких CSV файлов для больших объемов"""

        print(f"Генерация {count} сообщений в директорию {output_dir}")
        print(f"Сообщений на файл: {records_per_file}")
        print(f"Количество файлов: {count // records_per_file + (1 if count % records_per_file else 0)}")

        # Создаем директорию если не существует
        os.makedirs(output_dir, exist_ok=True)

        total_generated = 0
        file_count = 0

        # Определяем порядок колонок для CSV
        fieldnames = [
            "chat_id", "bucket", "chat_msg_local_id", "flags", "date",
            "update_time", "author_id", "text", "kludges", "forwarded",
            "forwarded_message_ids", "mentions", "marked_users", "ttl",
            "deleted_for_all"
        ]

        while total_generated < count:
            file_count += 1
            records_in_file = min(records_per_file, count - total_generated)
            output_file = os.path.join(output_dir, f"messages_part_{file_count:04d}.csv")

            print(f"\nГенерация файла {file_count}: {output_file}")
            print(f"  Записей в файле: {records_in_file}")

            start_time = time.time()

            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                for i in range(records_in_file):
                    row = self.generate_message_row(total_generated + i, chat_id)
                    writer.writerow(row)

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

    def generate_optimized_csv(self, count: int, output_file: str,
                              chat_id: int = None,
                              progress_interval: int = 1000) -> None:
        """Оптимизированная генерация CSV с буферизацией"""

        print(f"Оптимизированная генерация {count} сообщений в CSV")
        print(f"Буферизация записей в памяти")

        BUFFER_SIZE = 1000  # Записей в буфере
        buffer = []

        # Определяем порядок колонок для CSV
        fieldnames = [
            "chat_id", "bucket", "chat_msg_local_id", "flags", "date",
            "update_time", "author_id", "text", "kludges", "forwarded",
            "forwarded_message_ids", "mentions", "marked_users", "ttl",
            "deleted_for_all"
        ]

        start_time = time.time()

        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(count):
                row = self.generate_message_row(i, chat_id)
                buffer.append(row)

                # Когда буфер заполнен, записываем в файл
                if len(buffer) >= BUFFER_SIZE:
                    writer.writerows(buffer)
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
                writer.writerows(buffer)

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
        print("СВОДКА ГЕНЕРАЦИИ CSV")
        print("="*60)
        print(f"Сообщений сгенерировано: {self.metrics['messages_generated']:,}")
        print(f"Затраченное время: {elapsed:.2f} секунд")
        print(f"Средняя скорость: {self.metrics['messages_generated']/elapsed:.1f} msg/sec")

        if file_size_bytes:
            size_mb = file_size_bytes / (1024 * 1024)
            size_per_record = file_size_bytes / self.metrics['messages_generated']
            print(f"Размер файла: {size_mb:.2f} MB")
            print(f"Средний размер записи: {size_per_record:.0f} байт")

        print("\n✓ РЕКОМЕНДАЦИИ ПО ИСПОЛЬЗОВАНИЮ С DSBULK:")
        print("  1. Базовая загрузка:")
        print("     dsbulk load -url data.csv -k keyspace -t table")
        print("  2. С дополнительными параметрами:")
        print("     dsbulk load -url data.csv \\")
        print("       -k keyspace \\")
        print("       -t table \\")
        print("       -header true \\")
        print("       -delim ',' \\")
        print("       -quote '\"' \\")
        print("       -maxConcurrentQueries 32 \\")
        print("       -maxRecords 1000000")
        print("  3. Для директории с несколькими файлами:")
        print("     dsbulk load -url directory/ -k keyspace -t table")
        print("\n  Подсказка: используйте -dryRun true для тестирования")

def main():
    parser = argparse.ArgumentParser(
        description='Генератор CSV файлов для загрузки в Cassandra через DSBulk'
    )
    parser.add_argument('--count', type=int, default=1000,
                       help='Количество сообщений для генерации')
    parser.add_argument('--output', type=str, default='messages.csv',
                       help='Имя выходного CSV файла')
    parser.add_argument('--output-dir', type=str,
                       help='Директория для выходных CSV файлов (многопоточная генерация)')
    parser.add_argument('--records-per-file', type=int, default=100000,
                       help='Сообщений на файл при многопоточной генерации')
    parser.add_argument('--chat-id', type=int,
                       help='Фиксированный chat_id для всех сообщений')
    parser.add_argument('--seed', type=int, default=42,
                       help='Seed для случайного генератора')
    parser.add_argument('--chunk-size', type=int, default=10000,
                       help='Частота вывода прогресса')
    parser.add_argument('--optimized', action='store_true',
                       help='Использовать оптимизированный режим с буферизацией')
    parser.add_argument('--no-header', action='store_true',
                       help='Не добавлять заголовок в CSV (по умолчанию с заголовком)')

    args = parser.parse_args()

    # Создаем генератор
    generator = CSVDsbulkGenerator(seed=args.seed)

    # Выбираем режим генерации
    if args.output_dir:
        # Генерация нескольких CSV файлов
        generator.generate_multiple_csv_files(
            count=args.count,
            output_dir=args.output_dir,
            records_per_file=args.records_per_file,
            chat_id=args.chat_id
        )
    elif args.optimized:
        # Оптимизированная генерация
        generator.generate_optimized_csv(
            count=args.count,
            output_file=args.output,
            chat_id=args.chat_id,
            progress_interval=args.chunk_size
        )
    else:
        # Стандартная генерация
        generator.generate_csv_file(
            count=args.count,
            output_file=args.output,
            chunk_size=args.chunk_size,
            chat_id=args.chat_id
        )

if __name__ == "__main__":
    main()