#!/usr/bin/env python3
import subprocess
import re
import matplotlib.pyplot as plt
import time
import sys

CONTAINER_ID = "b9d6dc985663"
KEYSPACE = "test_space"
TABLE = "messages"

def docker_nodetool_stats(command_args):
    """Выполняет nodetool внутри Docker-контейнера"""
    cmd = ['docker', 'exec', CONTAINER_ID] + command_args
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=30)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Ошибка nodetool: {e.stderr}")
        return None

def get_table_stats():
    """Получает статистику таблицы из nodetool в Docker"""
    output = docker_nodetool_stats(['nodetool', 'tablestats', f'{KEYSPACE}.{TABLE}'])
    if not output:
        return {'space_kb': 0, 'partitions': 0, 'sstables': 0, 'memtable_kb': 0}

    # Парсим ключевые метрики
    space_live = re.search(r'Space used \(live\):\s*([\d.]+)\s*([KMGT]?)B?', output)
    partitions = re.search(r'Number of partitions \(estimate\):\s*([\d,]+)', output)
    sstables = re.search(r'SSTable count:\s*(\d+)', output)
    memtable_size = re.search(r'Memtable data size:\s*([\d,]+)', output)

    space_kb = 0
    if space_live:
        num, unit = space_live.groups()
        multipliers = {'K':1024, 'M':1024**2, 'G':1024**3, 'T':1024**4}
        space_kb = float(num) * multipliers.get(unit, 1)

    partitions_num = int(partitions.group(1).replace(',', '')) if partitions else 0

    return {
        'space_kb': int(space_kb),
        'partitions': partitions_num,
        'sstables': int(sstables.group(1)) if sstables else 0,
        'memtable_kb': int(memtable_size.group(1).replace(',', '')) // 1024 if memtable_size else 0
    }

def main():
    print(f"Анализируем таблицу {KEYSPACE}.{TABLE} в контейнере {CONTAINER_ID}")
    print("Собираем данные...\n")

    measurements = []

    # 1. Текущее состояние (memtable)
    print("📊 1. До flush...")
    stats1 = get_table_stats()
    measurements.append({
        'records': stats1['partitions'],
        'disk_kb': stats1['space_kb'],
        'memtable_kb': stats1['memtable_kb'],
        'label': 'До flush'
    })
    print(f"   Записей: {stats1['partitions']:,} | Диск: {stats1['space_kb']/1024:.1f} MB | Memtable: {stats1['memtable_kb']} KB")

    # 2. Flush в Docker
    print("\n🔄 2. Выполняем flush...")
    docker_nodetool_stats(['nodetool', 'flush', f'{KEYSPACE}.{TABLE}'])
    time.sleep(3)  # Ждём SSTable

    stats2 = get_table_stats()
    measurements.append({
        'records': stats2['partitions'],
        'disk_kb': stats2['space_kb'],
        'memtable_kb': stats2['memtable_kb'],
        'label': 'После flush'
    })
    print(f"   Записей: {stats2['partitions']:,} | Диск: {stats2['space_kb']/1024:.1f} MB | Memtable: {stats2['memtable_kb']} KB")

    # 3. Прогноз роста
    current_records = stats2['partitions']
    current_size = stats2['space_kb']
    avg_size_per_record = current_size / current_records if current_records > 0 else 1

    print("\n📈 3. Прогноз роста...")
    for i, records in enumerate(range(2000, 11000, 2000)):
        size_kb = records * avg_size_per_record * 1.15  # +15% overhead SSTables
        measurements.append({
            'records': records,
            'disk_kb': int(size_kb),
            'memtable_kb': 0,
            'label': f'{records:,} записей'
        })
        print(f"   Прогноз: {records:,} записей → {size_kb/1024:.1f} MB")

    # График
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

    records = [m['records'] for m in measurements]
    disk_kb = [m['disk_kb'] for m in measurements]
    memtable_kb = [m['memtable_kb'] for m in measurements]

    # График 1: Диск vs Memtable
    ax1.plot(records, disk_kb, 'o-', linewidth=3, markersize=8, label='Диск (Space used live)', color='darkblue')
    ax1.plot(records, memtable_kb, 's--', linewidth=2, markersize=6, label='Memtable (RAM)', color='orange')
    ax1.set_xlabel('Количество записей')
    ax1.set_ylabel('Размер (KB)')
    ax1.set_title(f'Рост таблицы {KEYSPACE}.{TABLE} (контейнер {CONTAINER_ID[:12]}...)', fontweight='bold')
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # Аннотации
    ax1.annotate(f'{measurements[0]["label"]}\n{disk_kb[0]} KB',
                xy=(records[0], disk_kb[0]), xytext=(records[0]+100, disk_kb[0]+100),
                arrowprops=dict(arrowstyle='->', color='red'), fontsize=10, ha='left')
    ax1.annotate(f'{measurements[1]["label"]}\n{disk_kb[1]} KB',
                xy=(records[1], disk_kb[1]), xytext=(records[1]-400, disk_kb[1]+200),
                arrowprops=dict(arrowstyle='->', color='green'), fontsize=10, ha='right')

    # График 2: Байт на запись
    bytes_per_record = [d/r if r > 0 else 0 for d, r in zip(disk_kb, records)]
    ax2.plot(records, bytes_per_record, '^-', linewidth=2, markersize=6, color='purple')
    ax2.set_xlabel('Количество записей')
    ax2.set_ylabel('Байт на запись')
    ax2.set_title('Эффективность хранения')
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('cassandra_docker_growth.png', dpi=300, bbox_inches='tight')
    plt.show()

    # Итоги
    print(f"\n✅ График: cassandra_docker_growth.png")
    print(f"📏 Средний размер записи: {avg_size_per_record:.1f} байт")
    print(f"💾 10k записей займут: {measurements[-1]['disk_kb']/1024:.1f} MB")
    print(f"🔍 Проверить: docker exec {CONTAINER_ID} nodetool tablestats test_space.messages")

if __name__ == "__main__":
    main()
