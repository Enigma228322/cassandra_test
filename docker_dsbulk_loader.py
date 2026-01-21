#!/usr/bin/env python3
# docker_dsbulk_loader_fixed.py
"""
Исправленный скрипт для загрузки через DSBulk в Docker
"""

import subprocess
import tempfile
import csv
import re
import os
import time

class DockerDSBulkLoader:
    def __init__(self, container_name="cassandra", keyspace="test_space"):
        self.container_name = container_name
        self.keyspace = keyspace

    def copy_to_container(self, local_file, container_path="/tmp/"):
        """Копирует файл в контейнер"""
        cmd = f"docker cp {local_file} {self.container_name}:{container_path}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Ошибка копирования: {result.stderr}")
            return None
        return f"{container_path}{os.path.basename(local_file)}"

    def install_dsbulk(self):
        """Устанавливает DSBulk в контейнер если нет"""
        check_cmd = "docker exec cassandra bash -c 'which dsbulk 2>/dev/null || echo \"not found\"'"
        result = subprocess.run(check_cmd, shell=True, capture_output=True, text=True)

        if "not found" in result.stdout:
            print("Устанавливаем DSBulk в контейнер...")
            install_cmd = """
            docker exec cassandra bash -c '
                cd /tmp && \
                wget -q https://downloads.datastax.com/dsbulk/dsbulk-1.11.0.tar.gz -O dsbulk.tar.gz && \
                tar xzf dsbulk.tar.gz && \
                echo "DSBulk установлен"
            '
            """
            result = subprocess.run(install_cmd, shell=True, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"Ошибка установки DSBulk: {result.stderr}")
                return False
        return True

    def convert_sql_to_csv(self, sql_file_path, csv_file_path):
        """Конвертирует SQL файл в CSV"""
        print("Чтение SQL файла...")
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        print("Парсинг INSERT запросов...")
        # Находим все INSERT запросы
        pattern = r'INSERT INTO Messages\s*\((.*?)\)\s*VALUES\s*\((.*?)\);'
        inserts = re.findall(pattern, content, re.DOTALL | re.IGNORECASE)

        print(f"Найдено {len(inserts)} INSERT запросов")

        with open(csv_file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)

            # Заголовок
            writer.writerow([
                'chat_id', 'bucket', 'chat_msg_local_id', 'flags', 'date',
                'update_time', 'author_id', 'text', 'kludges', 'forwarded',
                'forwarded_message_ids', 'mentions', 'marked_users', 'ttl',
                'deleted_for_all'
            ])

            processed = 0
            for columns, values_str in inserts:
                # Парсим значения
                values = self.parse_sql_values(values_str)
                writer.writerow(values)

                processed += 1
                if processed % 10000 == 0:
                    print(f"  Обработано {processed} записей")

        print(f"✓ CSV файл создан: {csv_file_path}")
        print(f"  Всего записей: {len(inserts)}")
        return len(inserts)

    def parse_sql_values(self, values_str):
        """Парсит строку значений SQL в список Python"""
        values = []
        current = ''
        in_quotes = False
        in_list = False
        escape_next = False

        # Очищаем лишние пробелы и переносы строк
        values_str = values_str.replace('\n', ' ').strip()

        i = 0
        while i < len(values_str):
            char = values_str[i]

            if escape_next:
                current += char
                escape_next = False
            elif char == '\\':
                escape_next = True
            elif char == "'" and not in_quotes:
                in_quotes = True
            elif char == "'" and in_quotes:
                # Проверяем, не двойная ли кавычка
                if i + 1 < len(values_str) and values_str[i + 1] == "'":
                    current += "'"
                    i += 1  # Пропускаем следующую кавычку
                else:
                    in_quotes = False
            elif char == '[' and not in_quotes:
                in_list = True
                current += char
            elif char == ']' and in_list:
                in_list = False
                current += char
            elif char == ',' and not in_quotes and not in_list:
                values.append(self.clean_value(current.strip()))
                current = ''
            else:
                current += char

            i += 1

        # Добавляем последнее значение
        if current:
            values.append(self.clean_value(current.strip()))

        # Проверяем количество значений
        if len(values) != 15:
            print(f"Предупреждение: ожидается 15 значений, получено {len(values)}")
            print(f"Строка: {values_str[:100]}...")

        return values

    def clean_value(self, value):
        """Очищает значение от SQL-форматирования"""
        if not value:
            return ''

        # Boolean значения
        if value.upper() == 'TRUE':
            return 'true'
        elif value.upper() == 'FALSE':
            return 'false'

        # NULL
        if value.upper() == 'NULL':
            return ''

        # Строка в кавычках
        if value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
            # Заменяем экранированные кавычки
            value = value.replace("''", "'")

        # Списки оставляем как есть (Cassandra поймет)
        if value.startswith('[') and value.endswith(']'):
            return value

        # Числа оставляем как есть
        return value

    def load_with_dsbulk(self, csv_file, table="messages"):
        """Загружает CSV через DSBulk с правильными параметрами"""
        # Копируем CSV в контейнер
        print(f"Копируем CSV в контейнер...")
        container_csv = self.copy_to_container(csv_file)
        if not container_csv:
            return False

        # Команда для DSBulk с правильными параметрами
        dsbulk_cmd = f"""
docker exec {self.container_name} bash -c '
    # Определяем путь к DSBulk
    if [ -f /tmp/dsbulk-1.11.0/bin/dsbulk ]; then
        DSBULK=/tmp/dsbulk-1.11.0/bin/dsbulk
    elif [ -f /opt/dsbulk-1.11.0/bin/dsbulk ]; then
        DSBULK=/opt/dsbulk-1.11.0/bin/dsbulk
    else
        # Пробуем найти в PATH
        DSBULK=$(which dsbulk 2>/dev/null || echo "dsbulk")
    fi

    # echo "Используем DSBulk: \$DSBULK"

    # Проверяем версию DSBulk
    dsbulk --version

    echo "Начинаем загрузку..."
    echo "CSV файл: {container_csv}"
    echo "Keyspace: {self.keyspace}"
    echo "Таблица: {table}"

    # Запускаем DSBulk с правильными параметрами
    time dsbulk load \\
        --url file://{container_csv} \\
        --keyspace {self.keyspace} \\
        --table {table} \\
        --header true \\
        --delim "," \\
        --quote "\\\"" \\
        --escape "\\\\\\\\" \\
        --batch.mode PARTITION_KEY \\
        --batch.size 100 \\
        --executor.maxPerSecond 10000 \\
        --executor.maxInFlight 100 \\
        --executor.continuousPaging.enabled false \\
        --schema.allowMissingFields true \\
        --connector.csv.maxCharsPerColumn -1 \\
        --driver.advanced.retry-policy.fixed.retries 10 \\
        --log.directory /tmp/dsbulk_logs \\
        --log.verbosity quiet
'
"""

        print("Запуск DSBulk...")
        print("=" * 60)

        start_time = time.time()
        result = subprocess.run(dsbulk_cmd, shell=True, capture_output=True, text=True)

        elapsed = time.time() - start_time

        print("Вывод DSBulk:")
        print(result.stdout)
        if result.stderr:
            print("Ошибки:")
            print(result.stderr)

        if result.returncode == 0:
            print("=" * 60)
            print(f"✓ DSBulk успешно завершил загрузку за {elapsed:.2f} сек")
            return True
        else:
            print("=" * 60)
            print(f"✗ DSBulk завершился с ошибкой (код: {result.returncode})")
            print(f"Время выполнения: {elapsed:.2f} сек")
            return False

    def simple_load_with_dsbulk(self, csv_file, table="messages"):
        """Простая загрузка с минимальными параметрами"""
        container_csv = self.copy_to_container(csv_file)
        if not container_csv:
            return False

        # Минимальная команда для тестирования
        dsbulk_cmd = f"""
docker exec {self.container_name} bash -c '
    if [ -f /tmp/dsbulk-1.11.0/bin/dsbulk ]; then
        DSBULK=/tmp/dsbulk-1.11.0/bin/dsbulk
    else
        DSBULK=dsbulk
    fi

    echo "Простая загрузка с минимальными параметрами..."
    \$DSBULK load \\
        --url file://{container_csv} \\
        --keyspace {self.keyspace} \\
        --table {table} \\
        --header true
'
"""

        print("Запуск DSBulk (простой режим)...")
        result = subprocess.run(dsbulk_cmd, shell=True, capture_output=True, text=True)

        print(result.stdout)
        if result.stderr:
            print("Ошибки:", result.stderr)

        return result.returncode == 0

    def load_sql_file(self, sql_file_path, table="messages"):
        """Основной метод: загружает SQL файл через DSBulk"""
        print(f"Обработка файла: {sql_file_path}")
        print("=" * 60)

        # Проверяем существование файла
        if not os.path.exists(sql_file_path):
            print(f"✗ Файл не найден: {sql_file_path}")
            return False

        # # Устанавливаем DSBulk если нужно
        # if not self.install_dsbulk():
        #     print("✗ Не удалось установить DSBulk")
        #     return False

        # Создаем временный CSV файл
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_csv:
            csv_file_path = tmp_csv.name

        try:
            # Конвертируем в CSV
            print("\n1. Конвертация SQL в CSV...")
            record_count = self.convert_sql_to_csv(sql_file_path, csv_file_path)

            if record_count == 0:
                print("✗ Нет данных для загрузки")
                return False

            # Загружаем через DSBulk
            print(f"\n2. Загрузка {record_count:,} записей через DSBulk...")

            # Сначала пробуем простую загрузку
            success = self.simple_load_with_dsbulk(csv_file_path, table)

            if not success:
                print("\nПробуем оптимизированную загрузку...")
                success = self.load_with_dsbulk(csv_file_path, table)

            if success:
                print("\n" + "=" * 60)
                print("✓ ЗАГРУЗКА УСПЕШНО ЗАВЕРШЕНА!")
                print("=" * 60)
            else:
                print("\n" + "=" * 60)
                print("✗ ЗАГРУЗКА НЕ УДАЛАСЬ")
                print("=" * 60)

            return success

        finally:
            # Удаляем временный CSV файл
            if os.path.exists(csv_file_path):
                os.unlink(csv_file_path)

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Загрузка SQL в Cassandra через DSBulk в Docker')
    parser.add_argument('sql_file', help='SQL файл для загрузки')
    parser.add_argument('--container', default='cassandra', help='Имя контейнера')
    parser.add_argument('--keyspace', default='test_space', help='Keyspace')
    parser.add_argument('--table', default='messages', help='Таблица')

    args = parser.parse_args()

    loader = DockerDSBulkLoader(args.container, args.keyspace)
    success = loader.load_sql_file(args.sql_file, args.table)

    if not success:
        print("\nСОВЕТЫ ПО УСТРАНЕНИЮ ПРОБЛЕМ:")
        print("1. Проверьте, что контейнер Cassandra запущен: docker ps")
        print("2. Проверьте, что keyspace и таблица существуют")
        print("3. Попробуйте загрузить вручную:")
        print("   docker exec -it cassandra bash")
        print("   cd /tmp")
        print("   /tmp/dsbulk-1.11.0/bin/dsbulk load --help")
        print("4. Используйте альтернативный метод загрузки через cqlsh:")
        print(f"   cat {args.sql_file} | docker exec -i cassandra cqlsh -k {args.keyspace}")

    sys.exit(0 if success else 1)

if __name__ == "__main__":
    import sys
    main()