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

def linear_regression_numpy(x, y):
    """Линейная регрессия y = kx + b"""
    X = np.vstack([x, np.ones(len(x))]).T
    k, b = np.linalg.lstsq(X, y, rcond=None)[0]
    y_pred = k*x + b
    r2 = 1 - np.sum((y - y_pred)**2) / np.sum((y - np.mean(y))**2)
    return k, b, r2, y_pred

def quadratic_regression_numpy(x, y):
    """Квадратичная регрессия y = ax² + bx + c"""
    X = np.vstack([x**2, x, np.ones(len(x))]).T
    coeffs = np.linalg.lstsq(X, y, rcond=None)[0]
    a, b, c = coeffs
    y_pred = a*x**2 + b*x + c
    r2 = 1 - np.sum((y - y_pred)**2) / np.sum((y - np.mean(y))**2)
    return a, b, c, r2, y_pred

def log_regression_numpy(x, y):
    """Логарифмическая регрессия y = a*ln(x) + b"""
    X = np.vstack([np.log(x), np.ones(len(x))]).T
    coeffs = np.linalg.lstsq(X, y, rcond=None)[0]
    a, b = coeffs
    y_pred = a*np.log(x) + b
    r2 = 1 - np.sum((y - y_pred)**2) / np.sum((y - np.mean(y))**2)
    return a, b, r2, y_pred

def calculate_11trillion_linear(k, b):
    """Расчёт 11 трлн для ЛИНЕЙНОЙ регрессии"""
    x = 11e12
    size_kb = k * x + b
    size_tb = size_kb / 1024**4
    size_pb = size_tb / 1024
    nodes_1pb = int(np.ceil(size_pb))
    return size_tb, size_pb, nodes_1pb

def calculate_11trillion_quadratic(a, b, c):
    """Расчёт 11 трлн для КВАДРАТИЧНОЙ регрессии"""
    x = 11e12
    size_kb = a * x**2 + b * x + c
    size_tb = size_kb / 1024**4
    size_pb = size_tb / 1024
    nodes_1pb = int(np.ceil(size_pb))
    return size_tb, size_pb, nodes_1pb

def plot_regression_analysis(df):
    """Строит обе регрессии + прогноз 11 трлн"""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

    X = df['records'].values
    y = df['disk_kb'].values

    # ЛИНЕЙНАЯ регрессия
    k_lin, b_lin, r2_lin, y_pred_lin = linear_regression_numpy(X, y)

    # КВАДРАТИЧНАЯ регрессия
    a_quad, b_quad, c_quad, r2_quad, y_pred_quad = quadratic_regression_numpy(X, y)

    # ✅ РАСЧЁТ 11 ТРЛН — ОТДЕЛЬНО ДЛЯ КАЖДОЙ МОДЕЛИ
    size_lin_tb, size_lin_pb, nodes_lin = calculate_11trillion_linear(k_lin, b_lin)
    size_quad_tb, size_quad_pb, nodes_quad = calculate_11trillion_quadratic(a_quad, b_quad, c_quad)

    # График 1: Сравнение моделей
    x_smooth = np.linspace(X.min(), X.max()*1.2, 1000)
    ax1.scatter(X, y/1024, s=250, alpha=0.8, color='black', zorder=5, label='Данные')
    ax1.plot(x_smooth, (k_lin*x_smooth + b_lin)/1024, 'blue', linewidth=4,
             label=f'Линейная\ny={k_lin:.6f}x+{b_lin:.0f}\nR²={r2_lin:.4f}')
    ax1.plot(x_smooth, (a_quad*x_smooth**2 + b_quad*x_smooth + c_quad)/1024, 'red', linewidth=4,
             label=f'Квадратичная\ny={a_quad:.2e}x²+{b_quad:.2e}x+{c_quad:.0f}\nR²={r2_quad:.4f}')
    ax1.set_xlabel('Количество записей')
    ax1.set_ylabel('Размер (MB)')
    ax1.set_title('Сравнение регрессий')
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # График 2: Байт на запись
    ax2.plot(X, df['bytes_per_record'], 'go-', linewidth=4, markersize=15)
    ax2.axhline(y=df['bytes_per_record'].iloc[-1], color='red', linestyle=':', linewidth=3)
    ax2.set_xlabel('Количество записей')
    ax2.set_ylabel('Байт на запись')
    ax2.set_title(f'Эффективность: {df["bytes_per_record"].iloc[-1]:.0f} байт/запись')
    ax2.grid(True, alpha=0.3)

    # График 3: Прогноз 11 трлн
    forecast_records = np.logspace(np.log10(1000), np.log10(11e12), 100)
    forecast_size_lin_tb = (k_lin*forecast_records + b_lin) / 1024**4
    forecast_size_quad_tb = (a_quad*forecast_records**2 + b_quad*forecast_records + c_quad) / 1024**4

    ax3.loglog(forecast_records/1e12, forecast_size_lin_tb, 'blue', linewidth=5,
               label=f'Линейная: {size_lin_pb:.1f} PB')
    ax3.loglog(forecast_records/1e12, forecast_size_quad_tb, 'red', linewidth=5,
               label=f'Квадратичная: {size_quad_pb:.0f} PB')
    ax3.scatter([X[-1]/1e12], [y[-1]/1024**4], s=500, color='black', zorder=10)
    ax3.set_xlabel('Триллионы записей')
    ax3.set_ylabel('Размер (TB)')
    ax3.set_title('ПРОГНОЗ 11 ТРЛН ЗАПИСЕЙ')
    ax3.legend()
    ax3.grid(True, alpha=0.3)

    # График 4: Сравнение моделей по R²
    models = ['Линейная', 'Квадратичная']
    r2_scores = [r2_lin, r2_quad]
    colors = ['blue', 'red']
    bars = ax4.bar(models, r2_scores, color=colors, alpha=0.7, edgecolor='black', linewidth=2)
    ax4.set_ylabel('R² (качество модели)')
    ax4.set_title('Какая модель лучше?')
    ax4.set_ylim(0, 1)

    # Подписи на столбцах
    for bar, r2 in zip(bars, r2_scores):
        height = bar.get_height()
        ax4.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                f'{r2:.4f}', ha='center', va='bottom', fontweight='bold')

    ax4.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('cassandra_regression_comparison.png', dpi=300, bbox_inches='tight')
    plt.show()

    # ✅ ПОДРОБНЫЙ ВЫВОД
    print("\n" + "="*80)
    print("📊 РЕЗУЛЬТАТЫ РЕГРЕССИЙ ДЛЯ 11 ТРЛН ЗАПИСЕЙ")
    print("="*80)
    print(f"🔢 Точек данных: {len(df)}")
    print(f"📏 Байт/запись: {df['bytes_per_record'].iloc[-1]:.0f}")
    print()
    print("📈 ЛИНЕЙНАЯ РЕГРЕССИЯ (рекомендуется для Cassandra)")
    print(f"   Формула: y = {k_lin:.6f}x + {b_lin:.0f} KB")
    print(f"   R²      = {r2_lin:.4f}")
    print(f"   11 трлн = {size_lin_tb:>10.1f} TB ({size_lin_pb:>7.1f} PB)")
    print(f"   Узлов   = {nodes_lin:>8,} × 1 PB (RF=1)")
    print()
    print("📈 КВАДРАТИЧНАЯ РЕГРЕССИЯ")
    print(f"   Формула: y = {a_quad:.2e}x² + {b_quad:.2e}x + {c_quad:.0f} KB")
    print(f"   R²      = {r2_quad:.4f}")
    print(f"   11 трлн = {size_quad_tb:>10.0f} TB ({size_quad_pb:>7.0f} PB)")
    print(f"   Узлов   = {nodes_quad:>8,} × 1 PB (RF=1)")
    print()
    print("🎯 ЛУЧШАЯ МОДЕЛЬ:", "ЛИНЕЙНАЯ" if r2_lin > 0.999 else "КВАДРАТИЧНАЯ")
    print("="*80)

def main():
    df = load_data()
    if df is not None and len(df) >= 2:
        plot_regression_analysis(df)
    else:
        print("❌ Нужно минимум 2 точки данных! Запустите collector.py 2+ раза.")

if __name__ == "__main__":
    main()
