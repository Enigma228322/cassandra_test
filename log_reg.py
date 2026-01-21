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


def log_regression_fit(x, y):
    """Подгоняет логарифмическую регрессию y = a*ln(x) + b"""
    # Логарифмическая регрессия: ln(y) = ln(a) + b*ln(x)
    log_x = np.log(x)
    log_y = np.log(y)
    coeffs = np.polyfit(log_x, log_y, 1)
    a = np.exp(coeffs[0])  # exp(ln(a))
    b = coeffs[1]           # ln(a)
    return a, b, coeffs


def plot_regression(df, title, forecast_count):
    """Строит 6 графиков: линейная + логарифмическая регрессия"""
    fig = plt.figure(figsize=(20, 15))

    # Линейная регрессия (первые 4 графика)
    z_lin = np.polyfit(df['count'], df['disk_kb'], 1)
    p_lin = np.poly1d(z_lin)

    # Логарифмическая регрессия
    a_log, b_log, z_log = log_regression_fit(df['count'], df['disk_kb'])
    p_log = lambda x: a_log * np.log(x) + b_log

    # График 1: Линейная - Записи vs Размер
    ax1 = plt.subplot(3, 2, 1)
    ax1.scatter(df['count'], df['disk_kb']/1024, s=200, alpha=0.8, color='darkblue')
    ax1.plot(df['count'], p_lin(df['count'])/1024, "r--", linewidth=3, alpha=0.9)
    ax1.set_xlabel('Количество записей')
    ax1.set_ylabel('Размер (MB)')
    ax1.set_title('Линейная: Размер vs Записи')
    ax1.grid(True, alpha=0.3)

    # График 2: Линейная - Байт на запись
    ax2 = plt.subplot(3, 2, 2)
    ax2.plot(df['count'], df['bytes_per_record'], 'go-', linewidth=3, markersize=12)
    ax2.axhline(y=df['bytes_per_record'].iloc[-1], color='red', linestyle=':', alpha=0.7)
    ax2.set_xlabel('Количество записей')
    ax2.set_ylabel('Байт на запись')
    ax2.set_title('Эффективность хранения')
    ax2.grid(True, alpha=0.3)

    # График 3: Линейная - Zoom
    ax3 = plt.subplot(3, 2, 3)
    ax3.scatter(df['count'], df['disk_kb']/1024, s=150, color='green', alpha=0.8)
    ax3.plot(df['count'], p_lin(df['count'])/1024, "r-", linewidth=4)
    ax3.set_title(f'Линейная: y = {z_lin[0]:.2e}x + {z_lin[1]:.0f}')
    ax3.set_xlabel('Количество записей')
    ax3.set_ylabel('Размер (MB)')
    ax3.grid(True, alpha=0.3)

    # График 4: Линейная - Прогноз
    ax4 = plt.subplot(3, 2, 4)
    bytes_per_record = df['bytes_per_record'].iloc[-1]
    count_now = df['count'].iloc[-1]
    forecast_range = np.logspace(np.log10(df['count'].min()), np.log10(forecast_count), 100)
    forecast_size_tb = forecast_range * bytes_per_record / 1024**4
    scale = max(1e9, forecast_count/10)
    ax4.loglog(forecast_range/scale, forecast_size_tb, 'purple', linewidth=4)
    ax4.scatter([count_now/scale], [df['disk_kb'].iloc[-1]/1024**4], s=400, color='red', zorder=5)
    unit = 'млрд' if forecast_count > 1e9 else 'млн'
    ax4.set_xlabel(f'{unit} записей')
    ax4.set_ylabel('Размер (TB)')
    ax4.set_title(f'Линейная прогноз {forecast_count/1e6:.0f} {unit}')
    ax4.grid(True, alpha=0.3)

    # График 5: Логарифмическая - Сравнение
    ax5 = plt.subplot(3, 2, 5)
    ax5.scatter(df['count'], df['disk_kb']/1024, s=200, alpha=0.8, color='darkblue', label='Данные')
    x_range = np.linspace(df['count'].min(), df['count'].max()*1.1, 100)
    ax5.plot(x_range, p_lin(x_range)/1024, "r--", linewidth=3, alpha=0.9, label='Линейная')
    ax5.plot(x_range, p_log(x_range)/1024, "orange", linewidth=3, label=f'Логарифмическая: y = {a_log:.2e}*ln(x) + {b_log:.0f}')
    ax5.legend()
    ax5.set_xlabel('Количество записей')
    ax5.set_ylabel('Размер (MB)')
    ax5.set_title('Сравнение регрессий')
    ax5.grid(True, alpha=0.3)

    # График 6: Логарифмическая - Остатки
    ax6 = plt.subplot(3, 2, 6)
    residuals_lin = df['disk_kb'] - p_lin(df['count'])
    residuals_log = df['disk_kb'] - p_log(df['count'])
    x_pos = np.arange(len(df))
    width = 0.35
    ax6.bar(x_pos - width/2, residuals_lin/1024, width, label='Линейная', alpha=0.7, color='red')
    ax6.bar(x_pos + width/2, residuals_log/1024, width, label='Логарифмическая', alpha=0.7, color='orange')
    ax6.axhline(0, color='black', linestyle='-', alpha=0.5)
    ax6.set_xlabel('Точки данных')
    ax6.set_ylabel('Остатки (MB)')
    ax6.set_title('Остатки регрессий')
    ax6.legend()
    ax6.grid(True, alpha=0.3)

    plt.tight_layout()
    return z_lin, z_log, df['bytes_per_record'].iloc[-1], p_lin, p_log


def print_stats(df, z_lin, z_log, bytes_per_rec, forecast_count, title, p_lin, p_log):
    """Выводит статистику"""
    current_count = df['count'].iloc[-1]
    current_size_mb = df['disk_kb'].iloc[-1] / 1024

    # Линейная
    forecast_size_lin_kb = p_lin(forecast_count)
    forecast_size_lin_mb = forecast_size_lin_kb / 1024
    forecast_size_lin_tb = forecast_size_lin_mb / 1024**2
    r2_lin = np.corrcoef(df['count'], df['disk_kb'])[0,1]**2

    # Логарифмическая
    forecast_size_log_kb = p_log(forecast_count)
    forecast_size_log_mb = forecast_size_log_kb / 1024
    forecast_size_log_tb = forecast_size_log_mb / 1024**2

    print(f"\n📊 {title}")
    print(f"🔢 Записей сейчас: {current_count:,}")
    print(f"💾 Размер сейчас: {current_size_mb:.1f} MB")
    print(f"📏 Байт/запись: {bytes_per_rec:.0f}")
    print(f"\n📈 ЛИНЕЙНАЯ регрессия:")
    print(f"   y = {z_lin[0]:.2e}x + {z_lin[1]:.0f}   R² = {r2_lin:.3f}")
    print(f"   Прогноз {forecast_count:,}: {forecast_size_lin_mb:.1f} MB ({forecast_size_lin_tb:.3f} TB)")
    print(f"\n📈 ЛОГАРИФМИЧЕСКАЯ регрессия:")
    print(f"   y = {np.exp(z_log[0]):.2e}*ln(x) + {z_log[1]:.0f}")
    print(f"   Прогноз {forecast_count:,}: {forecast_size_log_mb:.1f} MB ({forecast_size_log_tb:.3f} TB)")


def main():
    parser = argparse.ArgumentParser(description='Линейная + Логарифмическая регрессия (6 графиков)')
    parser.add_argument('--chats-forecast', type=int, default=100_000_000,
                       help='Прогноз количества чатов (по умолчанию 100M)')
    parser.add_argument('--peerids-forecast', type=int, default=1_000_000_000,
                       help='Прогноз количества peerids (по умолчанию 1B)')

    args = parser.parse_args()

    plt.ion()

    # Чаты
    df_chats = load_data(CSV_CHAT_FILE)
    if df_chats is not None and not df_chats.empty:
        z_lin_chats, z_log_chats, bytes_chats, p_lin_chats, p_log_chats = plot_regression(
            df_chats, 'Чаты', args.chats_forecast)
        print_stats(df_chats, z_lin_chats, z_log_chats, bytes_chats,
                   args.chats_forecast, 'ЧАТЫ', p_lin_chats, p_log_chats)
        plt.savefig('chats_regression.png', dpi=300, bbox_inches='tight')
        plt.draw()
        plt.pause(0.1)

    # PeerIDs
    df_peerids = load_data(CSV_PEER_FILE)
    if df_peerids is not None and not df_peerids.empty:
        z_lin_peer, z_log_peer, bytes_peer, p_lin_peer, p_log_peer = plot_regression(
            df_peerids, 'PeerIDs', args.peerids_forecast)
        print_stats(df_peerids, z_lin_peer, z_log_peer, bytes_peer,
                   args.peerids_forecast, 'PEERIDS', p_lin_peer, p_log_peer)
        plt.savefig('peerids_regression.png', dpi=300, bbox_inches='tight')
        plt.draw()
        plt.pause(0.1)

    plt.ioff()
    plt.show()


if __name__ == "__main__":
    main()
