#!/usr/bin/env python3
"""
Генератор CSV файлов для загрузки в Cassandra через DSBulk
Таблица: UserToMessage
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
from typing import List, Dict, Any, Set

class UserToMessageCSVGenerator:
    def __init__(self, seed: int = 42):
        """Инициализация генератора с сидом для воспроизводимости"""
        random.seed(seed)

        # Статистика для правдоподобных данных
        self.users = list(range(1000, 1000000))  # 1M пользователей
        self.peers = list(range(1000, 500000))   # 500K чатов (peer_id)

        # Для отслеживания chat_local_id для каждого (user_id, peer_id)
        self.chat_local_counter = {}

        # Кэш для уже сгенерированных сообщений (чтобы избежать дубликатов)
        self.generated_messages = set()

        # Метрики для мониторинга
        self.metrics = {
            'records_generated': 0,
            'files_created': 0,
            'total_size_bytes': 0,
            'start_time': None,
            'end_time': None
        }

    def get_next_chat_local_id(self, user_id: int, peer_id: int) -> int:
        """Получение следующего chat_local_id для пары (user_id, peer_id)"""
        key = (user_id, peer_id)
        if key not in self.chat_local_counter:
            self.chat_local_counter[key] = 0
        self.chat_local_counter[key] += 1
        return self.chat_local_counter[key]

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
        if random.random() < 0.05:  # 5% с пометкой
            flags |= 32
        if random.random() < 0.2:  # 20% упоминание
            flags |= 64
        if random.random() < 0.01:  # 1% системное сообщение
            flags |= 128
        return flags

    def generate_unique_message_key(self) -> tuple:
        """Генерация уникального ключа сообщения для избежания дубликатов"""
        while True:
            user_id = random.choice(self.users)
            peer_id = random.choice(self.peers)
            key = (user_id, peer_id)

            # Если для этой пары еще нет записей, создаем новую
            if key not in self.chat_local_counter:
                return (user_id, peer_id, 1)

            # Иначе проверяем, не превысили ли лимит сообщений для этой пары
            if self.chat_local_counter[key] < 1000:  # Максимум 1000 сообщений на пару
                return (user_id, peer_id, self.chat_local_counter[key] + 1)

            # Если превысили, ищем другую пару

    def escape_csv_value(self, value: Any) -> str:
        """Экранирование значения для CSV"""
        if value is None:
            return ''
        elif isinstance(value, bool):
            return str(value).lower()
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, str):
            # Экранируем только если есть спецсимволы
            if ',' in value or '\n' in value or '"' in value:
                escaped = value.replace('"', '""')
                return f'"{escaped}"'
            return value
        else:
            str_value = str(value)
            if '"' in str_value or ',' in str_value or '\n' in str_value:
                escaped = str_value.replace('"', '""')
                return f'"{escaped}"'
            return str_value

    def generate_record_row(self, record_idx: int,
                          user_id: int = None,
                          peer_id: int = None) -> Dict[str, Any]:
        """Генерация одной строки данных для CSV"""

        # Генерируем или используем переданные user_id и peer_id
        if user_id is None or peer_id is None:
            user_id, peer_id, chat_local_id = self.generate_unique_message_key()
        else:
            # Для фиксированных user_id и peer_id генерируем последовательный chat_local_id
            chat_local_id = self.get_next_chat_local_id(user_id, peer_id)

        # Обновляем счетчик
        key = (user_id, peer_id)
        if key not in self.chat_local_counter:
            self.chat_local_counter[key] = 0
        self.chat_local_counter[key] = chat_local_id

        flags = self.generate_flags()

        # Уникальный ключ для проверки дубликатов
        message_key = (user_id, peer_id, chat_local_id)
        if message_key in self.generated_messages:
            # Если такой ключ уже есть, увеличиваем chat_local_id
            chat_local_id += 1
            self.chat_local_counter[key] = chat_local_id
            message_key = (user_id, peer_id, chat_local_id)

        self.generated_messages.add(message_key)

        return {
            "user_id": user_id,
            "peer_id": peer_id,
            "chat_local_id": chat_local_id,
            "flags": flags
        }

    def generate_csv_file(self, count: int, output_file: str,
                         chunk_size: int = 10000,
                         user_id: int = None,
                         peer_id: int = None) -> None:
        """Генерация одного CSV файла"""

        self.metrics['start_time'] = time.time()

        print(f"Генерация {count} записей в CSV файл {output_file}")
        print(f"Таблица: UserToMessage")
        print(f"Размер чанка для отслеживания: {chunk_size}")

        if user_id is not None:
            print(f"Фиксированный user_id: {user_id}")
        if peer_id is not None:
            print(f"Фиксированный peer_id: {peer_id}")

        # Если файл существует, спрашиваем подтверждение
        if os.path.exists(output_file):
            response = input(f"Файл {output_file} уже существует. Перезаписать? (y/N): ")
            if response.lower() != 'y':
                print("Отменено пользователем")
                return

        total_generated = 0
        chunk_count = 0

        # Определяем порядок колонок для CSV (соответствует порядку в таблице)
        fieldnames = [
            "user_id",
            "peer_id",
            "chat_local_id",
            "flags"
        ]

        # Открываем файл для записи CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Записываем заголовок
            writer.writeheader()

            # Генерация записей
            for i in range(count):
                row = self.generate_record_row(i, user_id, peer_id)
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
                          f"скорость: {rate:.1f} records/sec, "
                          f"размер файла: {current_pos/1024/1024:.1f} MB")

        self.metrics['end_time'] = time.time()
        self.metrics['records_generated'] = total_generated

        # Получаем финальный размер файла
        file_size = os.path.getsize(output_file)
        self.metrics['total_size_bytes'] = file_size

        self.print_summary(file_size)

    def generate_multiple_csv_files(self, count: int, output_dir: str,
                                   records_per_file: int = 100000,
                                   user_id: int = None,
                                   peer_id: int = None) -> None:
        """Генерация нескольких CSV файлов для больших объемов"""

        print(f"Генерация {count} записей в директорию {output_dir}")
        print(f"Таблица: UserToMessage")
        print(f"Записей на файл: {records_per_file}")
        print(f"Количество файлов: {count // records_per_file + (1 if count % records_per_file else 0)}")

        if user_id is not None:
            print(f"Фиксированный user_id: {user_id}")
        if peer_id is not None:
            print(f"Фиксированный peer_id: {peer_id}")

        # Создаем директорию если не существует
        os.makedirs(output_dir, exist_ok=True)

        total_generated = 0
        file_count = 0

        # Определяем порядок колонок для CSV
        fieldnames = [
            "user_id",
            "peer_id",
            "chat_local_id",
            "flags"
        ]

        while total_generated < count:
            file_count += 1
            records_in_file = min(records_per_file, count - total_generated)
            output_file = os.path.join(output_dir, f"usertomessage_part_{file_count:04d}.csv")

            print(f"\nГенерация файла {file_count}: {output_file}")
            print(f"  Записей в файле: {records_in_file}")

            start_time = time.time()

            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                for i in range(records_in_file):
                    row = self.generate_record_row(total_generated + i, user_id, peer_id)
                    writer.writerow(row)

            elapsed = time.time() - start_time
            file_size = os.path.getsize(output_file)

            print(f"  ✓ Файл создан: {file_size/1024/1024:.1f} MB, "
                  f"время: {elapsed:.1f} сек, "
                  f"скорость: {records_in_file/elapsed:.1f} records/sec")

            total_generated += records_in_file

        print(f"\n✓ Все файлы созданы!")
        print(f"  Всего файлов: {file_count}")
        print(f"  Всего записей: {total_generated}")
        print(f"  Директория: {output_dir}")

    def generate_optimized_csv(self, count: int, output_file: str,
                              user_id: int = None,
                              peer_id: int = None,
                              progress_interval: int = 1000) -> None:
        """Оптимизированная генерация CSV с буферизацией"""

        print(f"Оптимизированная генерация {count} записей в CSV")
        print(f"Таблица: UserToMessage")
        print(f"Буферизация записей в памяти")

        if user_id is not None:
            print(f"Фиксированный user_id: {user_id}")
        if peer_id is not None:
            print(f"Фиксированный peer_id: {peer_id}")

        BUFFER_SIZE = 1000  # Записей в буфере
        buffer = []

        # Определяем порядок колонок для CSV
        fieldnames = [
            "user_id",
            "peer_id",
            "chat_local_id",
            "flags"
        ]

        start_time = time.time()

        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(count):
                row = self.generate_record_row(i, user_id, peer_id)
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
                          f"скорость: {rate:.1f} records/sec")

            # Записываем остатки из буфера
            if buffer:
                writer.writerows(buffer)

        elapsed = time.time() - start_time
        file_size = os.path.getsize(output_file)

        print(f"\n✓ Генерация завершена!")
        print(f"  Время: {elapsed:.1f} сек")
        print(f"  Средняя скорость: {count/elapsed:.1f} records/sec")
        print(f"  Размер файла: {file_size/1024/1024:.1f} MB")

    def print_summary(self, file_size_bytes: int = None):
        """Вывод сводки генерации"""
        elapsed = self.metrics['end_time'] - self.metrics['start_time']

        print("\n" + "="*60)
        print("СВОДКА ГЕНЕРАЦИИ CSV ДЛЯ ТАБЛИЦЫ UserToMessage")
        print("="*60)
        print(f"Записей сгенерировано: {self.metrics['records_generated']:,}")
        print(f"Уникальных пар (user_id, peer_id): {len(self.chat_local_counter):,}")
        print(f"Затраченное время: {elapsed:.2f} секунд")
        print(f"Средняя скорость: {self.metrics['records_generated']/elapsed:.1f} records/sec")

        if file_size_bytes:
            size_mb = file_size_bytes / (1024 * 1024)
            size_per_record = file_size_bytes / self.metrics['records_generated']
            print(f"Размер файла: {size_mb:.2f} MB")
            print(f"Средний размер записи: {size_per_record:.0f} байт")

        # Статистика по распределению данных
        if self.chat_local_counter:
            max_msgs = max(self.chat_local_counter.values())
            avg_msgs = sum(self.chat_local_counter.values()) / len(self.chat_local_counter)
            print(f"\nСтатистика распределения:")
            print(f"  Макс сообщений на пару (user_id, peer_id): {max_msgs}")
            print(f"  Среднее сообщений на пару: {avg_msgs:.1f}")

        print("\n✓ РЕКОМЕНДАЦИИ ПО ИСПОЛЬЗОВАНИЮ С DSBULK:")
        print("  1. Базовая загрузка:")
        print("     dsbulk load -url data.csv -k keyspace -t usertomessage")
        print("  2. С указанием ключевых колонок (оптимизация):")
        print("     dsbulk load -url data.csv \\")
        print("       -k keyspace \\")
        print("       -t usertomessage \\")
        print("       -m 'user_id=user_id,peer_id=peer_id,chat_local_id=chat_local_id,flags=flags' \\")
        print("       -header true")
        print("  3. Для директории с несколькими файлами:")
        print("     dsbulk load -url directory/ -k keyspace -t usertomessage")
        print("  4. С ограничением скорости (рекомендуется для больших объемов):")
        print("     dsbulk load -url data.csv \\")
        print("       -k keyspace \\")
        print("       -t usertomessage \\")
        print("       -maxConcurrentQueries 16 \\")
        print("       -maxRecords 500000")
        print("\n  Подсказка: используйте -dryRun true для тестирования конфигурации")

def main():
    parser = argparse.ArgumentParser(
        description='Генератор CSV файлов для загрузки в таблицу UserToMessage через DSBulk'
    )
    parser.add_argument('--count', type=int, default=1000,
                       help='Количество записей для генерации')
    parser.add_argument('--output', type=str, default='usertomessage.csv',
                       help='Имя выходного CSV файла')
    parser.add_argument('--output-dir', type=str,
                       help='Директория для выходных CSV файлов (многопоточная генерация)')
    parser.add_argument('--records-per-file', type=int, default=100000,
                       help='Записей на файл при многопоточной генерации')
    parser.add_argument('--user-id', type=int,
                       help='Фиксированный user_id для всех записей')
    parser.add_argument('--peer-id', type=int,
                       help='Фиксированный peer_id для всех записей')
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
    generator = UserToMessageCSVGenerator(seed=args.seed)

    # Выбираем режим генерации
    if args.output_dir:
        # Генерация нескольких CSV файлов
        generator.generate_multiple_csv_files(
            count=args.count,
            output_dir=args.output_dir,
            records_per_file=args.records_per_file,
            user_id=args.user_id,
            peer_id=args.peer_id
        )
    elif args.optimized:
        # Оптимизированная генерация
        generator.generate_optimized_csv(
            count=args.count,
            output_file=args.output,
            user_id=args.user_id,
            peer_id=args.peer_id,
            progress_interval=args.chunk_size
        )
    else:
        # Стандартная генерация
        generator.generate_csv_file(
            count=args.count,
            output_file=args.output,
            chunk_size=args.chunk_size,
            user_id=args.user_id,
            peer_id=args.peer_id
        )

if __name__ == "__main__":
    main()