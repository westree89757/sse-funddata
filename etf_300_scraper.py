#!/usr/bin/env python3
"""
沪深300ETF专项数据爬虫
只获取指定的6只沪深300ETF数据
"""

import os
import re
import json
import time
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
import matplotlib.pyplot as plt
import requests

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'STHeiti']
plt.rcParams['axes.unicode_minus'] = False

# 目标ETF列表
TARGET_ETFS = {
    "510300": "沪深300ETF华泰柏瑞",
    "510310": "沪深300ETF易方达",
    "510320": "沪深300ETF中金",
    "510330": "沪深300ETF华夏",
    "510350": "沪深300ETF工银",
    "510360": "沪深300ETF广发",
}

# API 配置
ETF_API = "https://query.sse.com.cn/commonQuery.do"
HEADERS = {
    "Referer": "https://www.sse.com.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
}

OUTPUT_DIR = "./output"
DATA_DIR = "./data"


def fetch_all_etf_data() -> Optional[pd.DataFrame]:
    """
    获取上交所全部ETF数据
    """
    print("正在获取上交所ETF数据...")

    params = {
        "jsonCallBack": f"jsonpCallback_{int(time.time())}",
        "isPagination": "true",
        "pageHelp.pageSize": "1000",
        "pageHelp.pageNo": "1",
        "pageHelp.beginPage": "1",
        "pageHelp.cacheSize": "1",
        "pageHelp.endPage": "100",
        "sqlId": "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L",
        "STAT_DATE": "",
        "_": str(int(time.time() * 1000)),
    }

    for attempt in range(2):
        try:
            response = requests.get(ETF_API, params=params, headers=HEADERS, timeout=30)
            response.raise_for_status()

            text = response.text
            match = re.search(r'jsonpCallback_\d+\((.+)\)', text)
            if not match:
                print("  [ERROR] 无法解析JSONP格式")
                return None

            data = json.loads(match.group(1))
            records = data.get("pageHelp", {}).get("data", [])

            if records:
                df = pd.DataFrame(records)
                print(f"  [SUCCESS] 获取到 {len(records)} 条ETF记录")
                return df

        except requests.RequestException as e:
            print(f"  [ERROR] 请求失败: {e}")
            if attempt < 1:
                time.sleep(2)

    return None


def filter_target_etfs(df: pd.DataFrame) -> pd.DataFrame:
    """
    筛选目标ETF
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # 筛选目标ETF代码
    target_codes = list(TARGET_ETFS.keys())
    filtered = df[df["SEC_CODE"].isin(target_codes)].copy()

    # 转换数值字段
    if "TOT_VOL" in filtered.columns:
        filtered["TOT_VOL"] = pd.to_numeric(filtered["TOT_VOL"], errors="coerce")

    # 添加基金名称（确保一致性）
    filtered["FUND_NAME"] = filtered["SEC_CODE"].map(TARGET_ETFS)

    # 确保日期格式
    if "STAT_DATE" in filtered.columns:
        filtered["STAT_DATE"] = pd.to_datetime(filtered["STAT_DATE"])

    print(f"\n筛选到 {len(filtered)} 条目标ETF记录:")
    for _, row in filtered.iterrows():
        print(f"  {row['SEC_CODE']} - {row['FUND_NAME']}: {row['TOT_VOL']:,.2f}万份")

    return filtered


def save_to_excel(df: pd.DataFrame, filename: str):
    """
    保存到Excel（多个sheet）
    """
    if df is None or df.empty:
        print("[WARN] 无数据可保存")
        return

    os.makedirs(DATA_DIR, exist_ok=True)
    filepath = os.path.join(DATA_DIR, filename)

    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
        # Sheet1: 各ETF明细数据
        detail_df = df[["STAT_DATE", "SEC_CODE", "FUND_NAME", "ETF_TYPE", "TOT_VOL"]].copy()
        detail_df = detail_df.sort_values(["SEC_CODE", "STAT_DATE"])
        detail_df.to_excel(writer, sheet_name="各ETF明细", index=False)

        # Sheet2: 汇总数据（每日所有ETF汇总）
        if "STAT_DATE" in df.columns:
            daily_summary = df.groupby("STAT_DATE").agg({
                "TOT_VOL": ["sum", "mean", "count"]
            }).reset_index()
            daily_summary.columns = ["日期", "总份额(万份)", "平均份额(万份)", "ETF数量"]
            daily_summary.to_excel(writer, sheet_name="每日汇总", index=False)

        # Sheet3: 各ETF汇总
            fund_summary = df.groupby(["SEC_CODE", "FUND_NAME"]).agg({
                "TOT_VOL": ["first", "last", "mean"]
            }).reset_index()
            fund_summary.columns = ["基金代码", "基金名称", "期初份额", "期末份额", "平均份额"]
            fund_summary.to_excel(writer, sheet_name="各ETF汇总", index=False)

    print(f"[INFO] Excel已保存: {filepath}")
    return filepath


def create_visualizations(df: pd.DataFrame):
    """
    创建可视化图表
    """
    if df is None or df.empty:
        print("[WARN] 无数据可绘图")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 图1: 柱状图 - 各ETF份额对比
    fig, ax = plt.subplots(figsize=(12, 6))

    fund_summary = df.groupby(["SEC_CODE", "FUND_NAME"])["TOT_VOL"].last().reset_index()
    fund_summary = fund_summary.sort_values("TOT_VOL", ascending=True)

    colors = plt.cm.Blues(np.linspace(0.4, 0.9, len(fund_summary)))
    bars = ax.barh(fund_summary["FUND_NAME"], fund_summary["TOT_VOL"], color=colors)

    for bar, val in zip(bars, fund_summary["TOT_VOL"]):
        ax.text(val + fund_summary["TOT_VOL"].max() * 0.01, bar.get_y() + bar.get_height()/2,
                f'{val:,.0f}', va='center', fontsize=10)

    ax.set_xlabel('基金份额 (万份)', fontsize=11)
    ax.set_title('沪深300ETF份额对比', fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.3, linestyle='--')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, 'etf300_bar_comparison.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("[INFO] 柱状图已保存: output/etf300_bar_comparison.png")

    # 图2: 柱状图+折线图 - 日期×基金 复合图
    if len(df) > 0:
        create_combined_chart(df)


def create_combined_chart(df: pd.DataFrame):
    """
    创建复合图表：柱状图(各ETF份额) + 折线图(总份额)
    """
    fig, ax1 = plt.subplots(figsize=(14, 7))

    # 准备数据
    pivot_df = df.pivot_table(index="STAT_DATE", columns="FUND_NAME", values="TOT_VOL", aggfunc='first')
    pivot_df = pivot_df.sort_index()

    # 设置日期格式
    dates = pivot_df.index.strftime('%Y-%m-%d') if hasattr(pivot_df.index, 'strftime') else [str(d) for d in pivot_df.index]
    x = np.arange(len(dates))
    width = 0.12

    # 绘制各ETF柱状图
    funds = list(pivot_df.columns)
    n_funds = len(funds)
    colors = plt.cm.Set3(np.linspace(0, 1, n_funds))

    for i, (fund, color) in enumerate(zip(funds, colors)):
        values = pivot_df[fund].fillna(0).values
        offset = (i - n_funds/2 + 0.5) * width
        bars = ax1.bar(x + offset, values, width, label=fund, color=color, edgecolor='white')

    # 设置主轴
    ax1.set_xlabel('日期', fontsize=11)
    ax1.set_ylabel('基金份额 (万份)', fontsize=11, color='steelblue')
    ax1.set_xticks(x)
    ax1.set_xticklabels(dates, rotation=45, ha='right')
    ax1.tick_params(axis='y', labelcolor='steelblue')
    ax1.legend(loc='upper left', bbox_to_anchor=(0, 1.15), ncol=3, fontsize=9)

    # 创建第二个Y轴（折线图-总份额）
    ax2 = ax1.twinx()
    total_shares = pivot_df.sum(axis=1).values
    line = ax2.plot(x, total_shares, color='red', linewidth=2.5, marker='o', markersize=6, label='总份额')
    ax2.set_ylabel('总份额 (万份)', fontsize=11, color='red')
    ax2.tick_params(axis='y', labelcolor='red')

    # 添加总份额数值标签
    for i, val in enumerate(total_shares):
        ax2.annotate(f'{val:,.0f}', (x[i], val), textcoords="offset points",
                     xytext=(0, 10), ha='center', fontsize=8, color='red')

    ax2.legend(loc='upper right', bbox_to_anchor=(1, 1.15), fontsize=9)

    plt.title('沪深300ETF每日份额：柱状图(各ETF) + 折线图(总份额)', fontsize=14, fontweight='bold', pad=20)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, 'etf300_combined_chart.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("[INFO] 复合图表已保存: output/etf300_combined_chart.png")


def print_summary(df: pd.DataFrame):
    """
    打印数据摘要
    """
    if df is None or df.empty:
        return

    print("\n" + "=" * 60)
    print("沪深300ETF数据摘要")
    print("=" * 60)
    print(f"数据日期: {df['STAT_DATE'].iloc[0].strftime('%Y-%m-%d') if 'STAT_DATE' in df.columns else 'N/A'}")
    print(f"ETF数量: {df['SEC_CODE'].nunique()} 只")
    print(f"总份额: {df['TOT_VOL'].sum():,.2f} 万份")
    print("\n各ETF详情:")
    summary = df.groupby(["SEC_CODE", "FUND_NAME"])["TOT_VOL"].last().sort_values(ascending=False)
    for code, name in TARGET_ETFS.items():
        val = summary.get(code, 0)
        if isinstance(val, pd.Series):
            val = val.iloc[0] if len(val) > 0 else 0
        print(f"  {code} {name}: {val:,.2f} 万份")
    print("=" * 60)


def main():
    """主流程"""
    print("\n" + "#" * 60)
    print("#  沪深300ETF专项数据爬虫")
    print("#" * 60)

    # 1. 获取数据
    df_all = fetch_all_etf_data()

    if df_all is None or df_all.empty:
        print("[ERROR] 无法获取ETF数据")
        return

    # 2. 筛选目标ETF
    df_target = filter_target_etfs(df_all)

    if df_target.empty:
        print("[ERROR] 未找到目标ETF数据")
        return

    # 3. 打印摘要
    print_summary(df_target)

    # 4. 保存Excel
    today = datetime.now().strftime("%Y%m%d")
    excel_file = f"etf300_data_{today}.xlsx"
    save_to_excel(df_target, excel_file)

    # 5. 创建可视化
    create_visualizations(df_target)

    print("\n" + "#" * 60)
    print("#  执行完成!")
    print("#" * 60)


if __name__ == "__main__":
    import numpy as np
    main()
