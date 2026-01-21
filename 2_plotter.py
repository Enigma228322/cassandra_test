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
        df = df.dropna().sort_values('records').reset_index(drop=True)
        return df
    except FileNotFoundError:
        print("❌ Файл не найден. Запустите сначала collector.py")
        return None

def quadratic_regression_numpy(x, y):
    """Квадратичная регрессия y = ax² + bx + c БЕЗ sklearn"""
    # Матрица Вандермонда для полинома 2-й степени
    X = np.vstack([x**2, x, np.ones(len(x))]).T

    # Метод наименьших квадратов: a,b,c = (X^T X)^(-1) X^T y
    coeffs = np.linalg.lstsq(X, y, rcond=None)[0]
    a, b, c = coeffs

    # Предсказания и R²
    y_pred = a*x**2 + b*x + c
    ss_res = np.sum((y - y_pred)**2)
    ss_tot = np.sum((y - np.mean(y))**2)
    r2 = 1 - (ss_res / ss_tot)

    return a, b, c, r2, y_pred

def plot_quadratic_growth(df):
    """Строит графики с квадратичной регрессией"""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

    X = df['records'].values
    y = df['disk_kb'].values

    # ✅ КВАДРАТИЧНАЯ РЕГРЕССИЯ степени 2 (только numpy!)
    a, b, c, r2, y_pred = quadratic_regression_numpy(X, y)

    # График 1: Данные + парабола
    x_smooth = np.linspace(X.min(), X.max()*1.2, 1000)
    y_smooth = a*x_smooth**2 + b*x_smooth + c

    ax1.scatter(X, y/1024, s=250, alpha=0.85, color='darkblue', zorder=5, label='Данные')
    ax1.plot(x_smooth, y_smooth/1024, 'r-', linewidth=5, label=f'y={a:.2e}x²+{b:.2e}x+{c:.0f}\nR²={r2:.4f}')
    ax1.set_xlabel('Количество записей', fontsize=12)
    ax1.set_ylabel('Размер (MB)', fontsize=12)
    ax1.set_title('Квадратичная регрессия размера Cassandra таблицы', fontsize=14, fontweight='bold')
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # График 2: Остатки
    residuals = y - y_pred
    ax2.scatter(X, residuals/1024, s=200, color='green', alpha=0.8)
    ax2.axhline(y=0, color='red', linestyle='-', linewidth=3)
    ax2.set_xlabel('Количество записей')
    ax2.set_ylabel('Остатки (MB)')
    ax2.set_title(f'Качество модели (R²={r2:.4f})')
    ax2.grid(True, alpha=0.3)

    # График 3: Байт на запись
    ax3.plot(X, df['bytes_per_record'], 'go-', linewidth=4, markersize=15)
    ax3.axhline(y=df['bytes_per_record'].iloc[-1], color='red', linestyle=':', linewidth=3)
    ax3.set_xlabel('Количество записей')
    ax3.set_ylabel('Байт на запись')
    ax3.set_title('Эффективность хранения')
    ax3.grid(True, alpha=0.3)

    # График 4: Прогноз 11 трлн
    bytes_per_record = df['bytes_per_record'].iloc[-1]
    records_now = X[-1]

    forecast_records = np.logspace(np.log10(1000), np.log10(11e12), 100)
    forecast_size_tb = (a*forecast_records**2 + b*forecast_records + c) / 1024**4

    ax4.loglog(forecast_records/1e12, forecast_size_tb, 'purple', linewidth=5,
               label=f'Квадратичный прогноз R²={r2:.3f}')
    ax4.scatter([records_now/1e12], [y[-1]/1024**4], s=500, color='red', zorder=10,
                label=f'Текущее: {records_now:,.0f} записей')
    ax4.set_xlabel('Триллионы записей', fontsize=12)
    ax4.set_ylabel('Размер (TB)', fontsize=12)
    ax4.set_title(f'Прогноз на 11 000 000 000 000 записей\n({bytes_per_record:.0f} байт/запись)')
    ax4.legend()
    ax4.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('cassandra_quadratic_regression.png', dpi=300, bbox_inches='tight')
    plt.show()

    # Детальная статистика
    size_11trln_tb = (a*(11e12)**2 + b*(11e12) + c) / 1024**4
    print("\n" + "="*70)
    print("📊 КВАДРАТИЧНАЯ РЕГРЕССИЯ (numpy)")
    print("="*70)
    print(f"📈 Формула:  y = {a:.2e}x² + {b:.2e}x + {c:.0f}")
    print(f"📏 R²       = {r2:.4f}")
    print(f"🔢 Записей = {records_now:>12,}")
    print(f"💾 Размер  = {y[-1]/1024:>8.1f} MB")
    print(f"📏 Байт/запись = {bytes_per_record:>6.0f}")
    print(f"🌌 11 ТРЛН = {size_11trln_tb:>8.1f} TB ({size_11trln_tb/1024:.3f} PB)")
    print("="*70)

def main():
    df = load_data()
    if df is not None and len(df) >= 3:
        plot_quadratic_growth(df)
    elif df is not None:
        print(f"⚠️  Точек данных: {len(df)}. Нужно минимум 3 для квадратичной регрессии!")
    else:
        print("❌ Нет данных. Запустите collector.py 3+ раза!")

if __name__ == "__main__":
    main()
