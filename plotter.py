#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

CSV_FILE = "cassandra_stats.csv"

def load_data():
    """Загружает данные из CSV: records,disk_kb"""
    try:
        df = pd.read_csv(CSV_FILE, names=['records', 'disk_kb'], header=None)
        df['bytes_per_record'] = df['disk_kb'] / df['records'].replace(0, np.nan)
        df = df.dropna().sort_values('records')
        return df
    except FileNotFoundError:
        print("❌ Файл не найден. Запустите сначала collector.py")
        return None

def plot_growth(df):
    """Строит графики роста"""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))

    # График 1: Записи vs Размер
    ax1.scatter(df['records'], df['disk_kb']/1024, s=200, alpha=0.8, color='darkblue')
    z = np.polyfit(df['records'], df['disk_kb'], 1)
    p = np.poly1d(z)
    ax1.plot(df['records'], p(df['records']), "r--", linewidth=3, alpha=0.9)

    ax1.set_xlabel('Количество записей')
    ax1.set_ylabel('Размер (MB)')
    ax1.set_title('Размер vs Записи (линейная регрессия)')
    ax1.grid(True, alpha=0.3)

    # График 2: Байт на запись
    ax2.plot(df['records'], df['bytes_per_record'], 'go-', linewidth=3, markersize=12)
    ax2.axhline(y=df['bytes_per_record'].iloc[-1], color='red', linestyle=':', alpha=0.7)
    ax2.set_xlabel('Количество записей')
    ax2.set_ylabel('Байт на запись')
    ax2.set_title('Эффективность хранения')
    ax2.grid(True, alpha=0.3)

    # График 3: Линейная зависимость (zoom)
    ax3.scatter(df['records'], df['disk_kb']/1024, s=150, color='green', alpha=0.8)
    ax3.plot(df['records'], p(df['records'])/1024, "r-", linewidth=4)
    ax3.set_xlabel('Количество записей')
    ax3.set_ylabel('Размер (MB)')
    ax3.set_title(f'Регрессия: y = {z[0]:.2e}x + {z[1]:.0f}')
    ax3.grid(True, alpha=0.3)

    # График 4: Прогноз 11 трлн
    bytes_per_record = df['bytes_per_record'].iloc[-1]
    records_now = df['records'].iloc[-1]

    forecast_records = np.logspace(np.log10(1000), np.log10(11e12), 100)
    forecast_size_tb = forecast_records * bytes_per_record / 1024**4

    ax4.loglog(forecast_records/1e12, forecast_size_tb, 'purple', linewidth=4)
    ax4.scatter([records_now/1e12], [df['disk_kb'].iloc[-1]/1024**4],
                s=400, color='red', zorder=5, label=f'Сейчас: {records_now:,}')
    ax4.set_xlabel('Триллионы записей')
    ax4.set_ylabel('Размер (TB)')
    ax4.set_title(f'Прогноз 11 трлн записей\n({bytes_per_record:.0f} байт/запись)')
    ax4.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('cassandra_growth.png', dpi=300, bbox_inches='tight')
    plt.show()

    # Статистика
    print("📊 АНАЛИЗ")
    print(f"🔢 Записей: {df['records'].iloc[-1]:,}")
    print(f"💾 Размер: {df['disk_kb'].iloc[-1]/1024:.1f} MB")
    print(f"📏 Байт/запись: {df['bytes_per_record'].iloc[-1]:.0f}")
    print(f"🌌 11 трлн записей: {11e12 * df['bytes_per_record'].iloc[-1] / 1024**5 :.1f} PB")
    print(f"📈 Коэффициент регрессии R²: {np.corrcoef(df['records'], df['disk_kb'])[0,1]:.3f}")

def main():
    df = load_data()
    if df is not None and not df.empty:
        plot_growth(df)
    else:
        print("Нет данных для графика")

if __name__ == "__main__":
    main()
