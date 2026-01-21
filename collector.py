#!/usr/bin/env python3
import subprocess
import re
import csv
import sys

CONTAINER_ID = "b9d6dc985663"
KEYSPACE = "test_space"
TABLE = "messages"
CSV_FILE = "cassandra_stats.csv"

def docker_cqlsh(query):
    """Выполняет CQL запрос в Docker"""
    cmd = [
        'docker', 'exec', CONTAINER_ID, 'cqlsh',
        '-e', query, 'localhost', '9042'
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True,
                              check=True, timeout=60)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"❌ CQL ошибка: {e.stderr}")
        return None

def docker_nodetool(command_args):
    """Выполняет nodetool в Docker"""
    cmd = ['docker', 'exec', CONTAINER_ID] + command_args
    try:
        result = subprocess.run(cmd, capture_output=True, text=True,
                              check=True, timeout=30)
        return result.stdout
    except:
        return None

def parse_count_result(output):
    """Парсит результат SELECT COUNT(*)"""
    match = re.search(r'count\s*\|\s*(\d+)', output)
    return int(match.group(1)) if match else 0

def parse_nodetool_stats(output):
    """Парсит только размер из nodetool tablestats"""
    if not output:
        return 0

    # Только Space used (live)
    space_match = re.search(r'Space used \(live\):\s*([\d.]+)\s*([KMGT]?)B?', output)
    space_kb = 0
    if space_match:
        num, unit = space_match.groups()
        multipliers = {'K':1024, 'M':1024**2, 'G':1024**3, 'T':1024**4}
        space_kb = float(num) * multipliers.get(unit, 1)

    return int(space_kb)

def main():
    print("📊 Собираем статистику таблицы...")

    # 1. SELECT COUNT(*)
    print("🔢 Подсчёт записей...")
    count_result = docker_cqlsh(f"SELECT COUNT(*) FROM {KEYSPACE}.{TABLE};")
    records = parse_count_result(count_result) if count_result else 0
    print(f"   Записей: {records:,}")

    # 2. nodetool tablestats (только размер)
    print("💾 Измеряем размер...")
    stats_output = docker_nodetool(['nodetool', 'tablestats', f'{KEYSPACE}.{TABLE}'])
    disk_kb = parse_nodetool_stats(stats_output)
    print(f"   Диск: {disk_kb/1024:.1f} MB")

    # Сохраняем в CSV: только records,disk_kb
    with open(CSV_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([records, disk_kb])

    print(f"\n✅ Данные сохранены: {CSV_FILE}")
    print(f"📈 Запустите: python3 plotter.py")

if __name__ == "__main__":
    main()
