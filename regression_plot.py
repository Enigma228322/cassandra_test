#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import argparse

CSV_CHAT_FILE = "chats_stats.csv"  # chats, disk_kb
CSV_PEER_FILE = "peerids_stats.csv"  # peerids, disk_kb


def load_data(csv_file):
    """Загружает данные из CSV: count,disk_kb"""
    try:
        df = pd.read_csv(csv_file, names=['count', 'disk_kb'], header=None)
        df['bytes_per_record'] = df['disk_kb'] / df['count'].replace(0, np.nan)
        df = df.dropna().sort_values('count')
        return df
    except FileNotFoundError:
        print(f"❌ Файл {csv_file} не найден")
        return None


def plot_regression(df, title, forecast_count):
    """Строит 4 графика как в оригинальном скрипте"""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))

    # График 1: Записи vs Размер
    ax1.scatter(df['count'], df['disk_kb']/1024, s=200, alpha=0.8, color='darkblue')
    z = np.polyfit(df['count'], df['disk_kb'], 1)
    p = np.poly1d(z)
    ax1.plot(df['count'], p(df['count']), "r--", linewidth=3, alpha=0.9)
    ax1.set_xlabel('Количество записей')
    ax1.set_ylabel('Размер (MB)')
    ax1.set_title('Размер vs Записи (линейная регрессия)')
    ax1.grid(True, alpha=0.3)

    # График 2: Байт на запись
    ax2.plot(df['count'], df['bytes_per_record'], 'go-', linewidth=3, markersize=12)
    ax2.axhline(y=df['bytes_per_record'].iloc[-1], color='red', linestyle=':', alpha=0.7)
    ax2.set_xlabel('Количество записей')
    ax2.set_ylabel('Байт на запись')
    ax2.set_title('Эффективность хранения')
    ax2.grid(True, alpha=0.3)

    # График 3: Линейная зависимость (zoom) - ТОЧНО как в оригинале
    ax3.scatter(df['count'], df['disk_kb']/1024, s=150, color='green', alpha=0.8)
    ax3.plot(df['count'], p(df['count'])/1024, "r-", linewidth=4)
    ax3.set_xlabel('Количество записей')
    ax3.set_ylabel('Размер (MB)')
    ax3.set_title(f'Регрессия: y = {z[0]:.2e}x + {z[1]:.0f}')
    ax3.grid(True, alpha=0.3)

    # График 4: Прогноз на заданное количество записей
    bytes_per_record = df['bytes_per_record'].iloc[-1]
    count_now = df['count'].iloc[-1]

    forecast_range = np.logspace(np.log10(df['count'].min()), np.log10(forecast_count), 100)
    forecast_size_tb = forecast_range * bytes_per_record / 1024**4

    ax4.loglog(forecast_range/max(1e9, forecast_count/10), forecast_size_tb, 'purple', linewidth=4)
    ax4.scatter([count_now/max(1e9, forecast_count/10)], [df['disk_kb'].iloc[-1]/1024**4],
                s=400, color='red', zorder=5, label=f'Сейчас: {count_now:,}')
    ax4.set_xlabel('Миллиарды записей' if forecast_count > 1e9 else 'Миллионы записей')
    ax4.set_ylabel('Размер (TB)')
    unit = 'млрд' if forecast_count > 1e9 else 'млн'
    ax4.set_title(f'Прогноз {forecast_count/1e6:.0f} {unit} записей\n({bytes_per_record:.0f} байт/запись)')
    ax4.grid(True, alpha=0.3)

    plt.tight_layout()
    return z, df['bytes_per_record'].iloc[-1], p


def print_stats(df, z, bytes_per_rec, forecast_count, title, p):
    """Выводит статистику"""
    current_count = df['count'].iloc[-1]
    current_size_mb = df['disk_kb'].iloc[-1] / 1024
    forecast_size_kb = p(forecast_count)
    forecast_size_mb = forecast_size_kb / 1024
    forecast_size_tb = forecast_size_mb / 1024**2

    print(f"\n📊 {title}")
    print(f"🔢 Записей сейчас: {current_count:,}")
    print(f"💾 Размер сейчас: {current_size_mb:.1f} MB")
    print(f"📏 Байт/запись: {bytes_per_rec:.0f}")
    print(f"📈 Коэффициент регрессии: a={z[0]:.2e}, b={z[1]:.0f}")
    print(f"R²: {np.corrcoef(df['count'], df['disk_kb'])[0,1]:.3f}")
    print(f"🎯 Прогноз {forecast_count:,} записей: {forecast_size_mb:.1f} MB ({forecast_size_tb:.3f} TB)")


def main():
    parser = argparse.ArgumentParser(description='Линейная регрессия размера таблиц (4 графика)')
    parser.add_argument('--chats-forecast', type=int, default=100_000_000,
                       help='Прогноз количества чатов (по умолчанию 100M)')
    parser.add_argument('--peerids-forecast', type=int, default=1_000_000_000,
                       help='Прогноз количества peerids (по умолчанию 1B)')

    args = parser.parse_args()

    plt.ion()  # Интерактивный режим

    # Чаты
    df_chats = load_data(CSV_CHAT_FILE)
    if df_chats is not None and not df_chats.empty:
        z_chats, bytes_chats, p_chats = plot_regression(df_chats, 'Чаты', args.chats_forecast)
        print_stats(df_chats, z_chats, bytes_chats, args.chats_forecast, 'ЧАТЫ', p_chats)
        plt.savefig('chats_regression.png', dpi=300, bbox_inches='tight')
        plt.draw()
        plt.pause(0.1)

    # PeerIDs
    df_peerids = load_data(CSV_PEER_FILE)
    if df_peerids is not None and not df_peerids.empty:
        z_peerids, bytes_peerids, p_peerids = plot_regression(df_peerids, 'PeerIDs', args.peerids_forecast)
        print_stats(df_peerids, z_peerids, bytes_peerids, args.peerids_forecast, 'PEERIDS', p_peerids)
        plt.savefig('peerids_regression.png', dpi=300, bbox_inches='tight')
        plt.draw()
        plt.pause(0.1)

    plt.ioff()
    plt.show()


if __name__ == "__main__":
    main()

# python3 regression_plot.py --chats-forecast 700000000 --peerids-forecast 1400000000