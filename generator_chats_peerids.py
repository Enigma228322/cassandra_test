#!/usr/bin/env python3
"""
Генератор CSV файлов для загрузки в Cassandra через DSBulk
Таблицы: Chats и PeerIds
"""

import argparse
import random
import string
import time
from datetime import datetime, timedelta
import csv
import os
import math
from typing import List, Dict, Any, Tuple

class CassandraDataGenerator:
    def __init__(self, seed: int = 42):
        """Инициализация генератора с сидом для воспроизводимости"""
        random.seed(seed)

        # Диапазоны ID
        self.user_ids = list(range(1000, 1000000))  # 1M пользователей
        self.chat_ids = list(range(1000, 500000))   # 500K чатов
        self.message_ids = list(range(1000, 10000000))  # 10M сообщений

        # Метрики
        self.metrics = {
            'chats_generated': 0,
            'peerids_generated': 0,
            'start_time': None,
            'end_time': None
        }

    def generate_string(self, length: int) -> str:
        """Генерация случайной строки заданной длины"""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    def generate_name(self) -> str:
        """Генерация имени чата (10 байт)"""
        return self.generate_string(10)

    def generate_secret(self) -> str:
        """Генерация секрета (8 байт)"""
        return self.generate_string(8)

    def generate_photo(self) -> str:
        """Генерация фото в формате {album_id}_{photo_id} (9 байт)"""
        album_id = random.randint(1000, 9999)
        photo_id = random.randint(1000, 9999)
        return f"{album_id}_{photo_id}"

    def generate_description(self, length: int = 100) -> str:
        """Генерация описания чата"""
        words = [
            "группа", "обсуждение", "проект", "команда", "работа",
            "друзья", "семья", "коллеги", "сообщество", "чат",
            "активный", "полезный", "интересный", "важный", "закрытый",
            "открытый", "официальный", "неофициальный", "веселый", "серьезный"
        ]
        word_count = random.randint(5, 15)
        selected_words = random.choices(words, k=word_count)
        description = ' '.join(selected_words).capitalize()

        # Обрезаем до нужной длины
        if len(description) > length:
            description = description[:length-3] + "..."
        return description

    def generate_pinned_message_ids(self, max_count: int = 3) -> str:
        """Генерация списка закрепленных сообщений"""
        count = random.randint(1, max_count)
        ids = random.sample(self.message_ids, count)
        return '[' + ','.join(str(msg_id) for msg_id in ids) + ']'

    def generate_members_count(self) -> int:
        """Генерация количества участников"""
        # Распределение: большинство чатов маленькие, некоторые большие
        if random.random() < 0.8:  # 80% чатов: 2-50 участников
            return random.randint(2, 50)
        elif random.random() < 0.9:  # 18% чатов: 51-200 участников
            return random.randint(51, 200)
        else:  # 2% чатов: 201-1000 участников
            return random.randint(201, 1000)

    def generate_chat_flags(self) -> int:
        """Генерация флагов чата"""
        flags = 0
        if random.random() < 0.7:  # 70% чатов активны
            flags |= 1
        if random.random() < 0.3:  # 30% чатов публичные
            flags |= 2
        if random.random() < 0.2:  # 20% чатов верифицированы
            flags |= 4
        if random.random() < 0.1:  # 10% чатов скрыты
            flags |= 8
        if random.random() < 0.4:  # 40% чатов разрешают медиа
            flags |= 16
        return flags

    def generate_timestamp(self, years_back: int = 3) -> int:
        """Генерация timestamp в секундах"""
        base_date = datetime.now()

        # Случайная дата за последние years_back лет
        days_ago = random.randint(0, years_back * 365)
        random_date = base_date - timedelta(days=days_ago)

        # Добавляем случайное время
        random_time = timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )

        final_date = random_date - random_time
        return int(final_date.timestamp())

    def generate_peer_flags(self) -> int:
        """Генерация флагов для PeerIds"""
        flags = 0
        if random.random() < 0.8:  # 80% уведомления включены
            flags |= 1
        if random.random() < 0.5:  # 50% пользователь администратор
            flags |= 2
        if random.random() < 0.1:  # 10% пользователь создатель
            flags |= 4
        if random.random() < 0.3:  # 30% пользователь покинул чат
            flags |= 8
        if random.random() < 0.2:  # 20% пользователь забанен
            flags |= 16
        return flags

    def escape_csv_value(self, value: Any) -> str:
        """Экранирование значения для CSV"""
        if value is None:
            return ''
        elif isinstance(value, bool):
            return str(value).lower()
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, str):
            # Экранируем кавычки
            if '"' in value:
                value = value.replace('"', '""')
            if ',' in value or '\n' in value or '"' in value:
                return f'"{value}"'
            return value
        else:
            return str(value)

    def generate_chat_row(self, chat_id: int, prob_description: float = 0.05,
                         prob_pinned: float = 0.1) -> Dict[str, Any]:
        """Генерация строки для таблицы Chats"""
        has_description = random.random() < prob_description
        has_pinned = random.random() < prob_pinned

        return {
            "id": chat_id,
            "name": self.escape_csv_value(self.generate_name()),
            "pinned_message_ids": self.generate_pinned_message_ids() if has_pinned else "[]",
            "secret": self.escape_csv_value(self.generate_secret()),
            "photo": self.escape_csv_value(self.generate_photo()),
            "members_count": self.generate_members_count(),
            "description": self.escape_csv_value(self.generate_description()) if has_description else "",
            "flags": self.generate_chat_flags()
        }

    def generate_peerid_row(self, user_id: int, chat_id: int,
                          base_timestamp: int) -> Dict[str, Any]:
        """Генерация строки для таблицы PeerIds"""
        invite_timestamp = base_timestamp
        last_message_ts = invite_timestamp + random.randint(0, 30 * 24 * 3600)  # до 30 дней после приглашения

        return {
            "user_id": user_id,
            "chat_id": chat_id,
            "invite_timestamp": invite_timestamp,
            "disable_for": random.randint(0, 100) if random.random() < 0.1 else 0,
            "flags": self.generate_peer_flags(),
            "inviter_id": random.choice(self.user_ids),
            "last_read_message_id": random.randint(0, 10000),
            "last_message_id": random.randint(0, 10000),
            "last_message_ts": last_message_ts
        }

    def generate_chats_csv(self, count: int, output_file: str,
                          prob_description: float = 0.05,
                          prob_pinned: float = 0.1,
                          chunk_size: int = 10000) -> List[int]:
        """Генерация CSV файла для таблицы Chats"""

        print(f"Генерация {count} чатов в файл {output_file}")
        self.metrics['start_time'] = time.time()

        # Если файл существует, спрашиваем подтверждение
        if os.path.exists(output_file):
            response = input(f"Файл {output_file} уже существует. Перезаписать? (y/N): ")
            if response.lower() != 'y':
                print("Отменено пользователем")
                return []

        generated_chat_ids = []
        fieldnames = ["id", "name", "pinned_message_ids", "secret", "photo",
                     "members_count", "description", "flags"]

        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(count):
                chat_id = self.chat_ids[i] if i < len(self.chat_ids) else 1000 + i
                row = self.generate_chat_row(chat_id, prob_description, prob_pinned)
                writer.writerow(row)
                generated_chat_ids.append(chat_id)

                if (i + 1) % chunk_size == 0:
                    elapsed = time.time() - self.metrics['start_time']
                    rate = (i + 1) / elapsed if elapsed > 0 else 0
                    print(f"  Чатов: {i + 1:,}/{count:,} ({rate:.1f} chats/sec)")

        self.metrics['chats_generated'] = count
        print(f"✓ Сгенерировано {count} чатов")
        return generated_chat_ids

    def generate_peerids_csv(self, count: int, output_file: str,
                            chat_ids: List[int],
                            chunk_size: int = 10000) -> None:
        """Генерация CSV файла для таблицы PeerIds"""

        print(f"Генерация {count} записей PeerIds в файл {output_file}")

        if os.path.exists(output_file):
            response = input(f"Файл {output_file} уже существует. Перезаписать? (y/N): ")
            if response.lower() != 'y':
                print("Отменено пользователем")
                return

        fieldnames = ["user_id", "chat_id", "invite_timestamp", "disable_for",
                     "flags", "inviter_id", "last_read_message_id",
                     "last_message_id", "last_message_ts"]

        # Для обеспечения уникальности (user_id, last_message_ts)
        used_pairs = set()

        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(count):
                # Выбираем случайного пользователя и чат
                user_id = random.choice(self.user_ids)
                chat_id = random.choice(chat_ids)

                # Генерируем метку времени приглашения (за последние 3 года)
                invite_ts = self.generate_timestamp(3)

                # Генерируем last_message_ts (после invite_ts)
                max_offset = 180 * 24 * 3600  # 180 дней в секундах
                last_message_ts = invite_ts + random.randint(0, max_offset)

                # Убеждаемся в уникальности пары (user_id, last_message_ts)
                pair = (user_id, last_message_ts)
                attempts = 0
                while pair in used_pairs and attempts < 10:
                    last_message_ts += random.randint(1, 10)
                    pair = (user_id, last_message_ts)
                    attempts += 1

                used_pairs.add(pair)

                row = self.generate_peerid_row(user_id, chat_id, invite_ts)
                row["last_message_ts"] = last_message_ts
                writer.writerow(row)

                if (i + 1) % chunk_size == 0:
                    elapsed = time.time() - self.metrics['start_time']
                    rate = (i + 1) / elapsed if elapsed > 0 else 0
                    print(f"  PeerIds: {i + 1:,}/{count:,} ({rate:.1f} records/sec)")

        self.metrics['peerids_generated'] = count
        print(f"✓ Сгенерировано {count} записей PeerIds")

    def generate_optimized_peerids(self, count: int, output_file: str,
                                 chat_ids: List[int],
                                 users_per_chat: int = 5) -> None:
        """Оптимизированная генерация PeerIds с реалистичным распределением"""

        print(f"Оптимизированная генерация {count} записей PeerIds")
        print(f"Среднее количество пользователей на чат: {users_per_chat}")

        fieldnames = ["user_id", "chat_id", "invite_timestamp", "disable_for",
                     "flags", "inviter_id", "last_read_message_id",
                     "last_message_id", "last_message_ts"]

        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            # Распределяем записи по чатам
            records_per_chat = max(1, count // len(chat_ids))

            buffer = []
            total_generated = 0

            for chat_idx, chat_id in enumerate(chat_ids):
                if total_generated >= count:
                    break

                # Количество пользователей в этом чате
                users_in_chat = min(records_per_chat, count - total_generated)
                if chat_idx < len(chat_ids) - 1:
                    users_in_chat = random.randint(1, records_per_chat * 2)

                # Базовое время для этого чата
                base_ts = self.generate_timestamp(3)

                for user_idx in range(users_in_chat):
                    if total_generated >= count:
                        break

                    user_id = random.choice(self.user_ids)
                    invite_ts = base_ts + random.randint(0, 7 * 24 * 3600)  # В течение недели

                    # Last message ts после invite
                    last_message_ts = invite_ts + random.randint(0, 180 * 24 * 3600)

                    row = self.generate_peerid_row(user_id, chat_id, invite_ts)
                    row["last_message_ts"] = last_message_ts
                    buffer.append(row)
                    total_generated += 1

                    # Периодически сбрасываем буфер
                    if len(buffer) >= 1000:
                        writer.writerows(buffer)
                        buffer.clear()

                        if total_generated % 10000 == 0:
                            print(f"  Прогресс: {total_generated:,}/{count:,}")

            # Записываем оставшиеся данные
            if buffer:
                writer.writerows(buffer)

        print(f"✓ Сгенерировано {total_generated} записей PeerIds")

def main():
    parser = argparse.ArgumentParser(
        description='Генератор CSV файлов для таблиц Chats и PeerIds в Cassandra'
    )

    # Общие параметры
    parser.add_argument('--seed', type=int, default=42,
                       help='Seed для случайного генератора')

    # Параметры для таблицы Chats
    parser.add_argument('--count-chats', type=int, default=1000,
                       help='Количество чатов для генерации')
    parser.add_argument('--output-chats', type=str, default='chats.csv',
                       help='Имя выходного CSV файла для таблицы Chats')
    parser.add_argument('--prob-description', type=float, default=0.05,
                       help='Вероятность наличия описания у чата (0.0-1.0)')
    parser.add_argument('--prob-pinned', type=float, default=0.1,
                       help='Вероятность наличия закрепленных сообщений (0.0-1.0)')

    # Параметры для таблицы PeerIds
    parser.add_argument('--count-peerids', type=int, default=5000,
                       help='Количество записей PeerIds для генерации')
    parser.add_argument('--output-peerids', type=str, default='peerids.csv',
                       help='Имя выходного CSV файла для таблицы PeerIds')
    parser.add_argument('--optimized', action='store_true',
                       help='Использовать оптимизированную генерацию PeerIds')
    parser.add_argument('--users-per-chat', type=int, default=5,
                       help='Среднее количество пользователей на чат (для оптимизированной генерации)')

    args = parser.parse_args()

    # Создаем генератор
    generator = CassandraDataGenerator(seed=args.seed)

    print("="*60)
    print("ГЕНЕРАЦИЯ ДАННЫХ ДЛЯ CASSANDRA")
    print("="*60)

    # Генерация данных для таблицы Chats
    print(f"\n1. Генерация таблицы Chats:")
    print(f"   - Количество чатов: {args.count_chats:,}")
    print(f"   - Вероятность описания: {args.prob_description*100}%")
    print(f"   - Вероятность закрепленных сообщений: {args.prob_pinned*100}%")
    print(f"   - Выходной файл: {args.output_chats}")

    chat_ids = generator.generate_chats_csv(
        count=args.count_chats,
        output_file=args.output_chats,
        prob_description=args.prob_description,
        prob_pinned=args.prob_pinned
    )

    # Генерация данных для таблицы PeerIds
    print(f"\n2. Генерация таблицы PeerIds:")
    print(f"   - Количество записей: {args.count_peerids:,}")
    print(f"   - Выходной файл: {args.output_peerids}")

    if args.optimized:
        generator.generate_optimized_peerids(
            count=args.count_peerids,
            output_file=args.output_peerids,
            chat_ids=chat_ids,
            users_per_chat=args.users_per_chat
        )
    else:
        generator.generate_peerids_csv(
            count=args.count_peerids,
            output_file=args.output_peerids,
            chat_ids=chat_ids
        )

    # Сводка
    print("\n" + "="*60)
    print("СВОДКА ГЕНЕРАЦИИ")
    print("="*60)
    print(f"Чатов сгенерировано: {generator.metrics['chats_generated']:,}")
    print(f"Записей PeerIds сгенерировано: {generator.metrics['peerids_generated']:,}")

    print("\n✓ КОМАНДЫ ДЛЯ ЗАГРУЗКИ В CASSANDRA:")
    print(f"  1. Загрузка таблицы Chats:")
    print(f"     dsbulk load -url {args.output_chats} -k test_space -t Chats -header true")
    print(f"  2. Загрузка таблицы PeerIds:")
    print(f"     dsbulk load -url {args.output_peerids} -k test_space -t PeerIds -header true")
    print("\n  Примечание: перед загрузкой убедитесь, что таблицы созданы в keyspace test_space")

if __name__ == "__main__":
    main()

#  python generator_chats_peerids.py \
#   --count-chats 10000 \
#   --count-peerids 100000 \
#   --prob-description 0.1 \
#   --prob-pinned 0.15 \
#   --optimized \
#   --output-chats /tmp/chats_10k.csv \
#   --output-peerids /tmp/peerids_100k.csv
