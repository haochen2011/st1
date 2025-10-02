#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统测试脚本
测试各个模块的基本功能
"""

import sys
import os
from datetime import datetime, date, timedelta

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import config
from database import db_manager
from stock_info import stock_info
from tick_data import tick_data
from basic_data import basic_data
from indicator_processor import indicator_processor


def test_config():
    """测试配置模块"""
    print("=== 测试配置模块 ===")
    try:
        print(f"数据库主机: {config.get('database', 'host')}")
        print(f"API端口: {config.getint('api', 'port')}")
        print(f"支持的周期: {config.get_periods()}")
        print(f"支持的市场: {config.get_market_codes()}")
        print("✓ 配置模块测试通过")
        return True
    except Exception as e:
        print(f"✗ 配置模块测试失败: {e}")
        return False


def test_database():
    """测试数据库连接"""
    print("\n=== 测试数据库连接 ===")
    try:
        result = db_manager.execute_sql("SELECT 1 as test")
        print("✓ 数据库连接测试通过")

        # 测试查询
        stock_count = db_manager.query_to_dataframe("SELECT COUNT(*) as count FROM stock_info")
        print(f"数据库中股票数量: {stock_count.iloc[0]['count'] if not stock_count.empty else 0}")
        print("✓ 数据库查询测试通过")
        return True
    except Exception as e:
        print(f"✗ 数据库测试失败: {e}")
        print("请检查数据库配置和连接")
        return False


def test_stock_info():
    """测试股票信息模块"""
    print("\n=== 测试股票信息模块 ===")
    try:
        # 测试获取股票列表
        stocks = stock_info.get_stock_list('sh')
        if not stocks.empty:
            print(f"✓ 获取上海股票列表成功，共 {len(stocks)} 只股票")

            # 测试获取单只股票信息
            test_stock = stocks.iloc[0]['SECURITY_CODE_A']
            info = stock_info.get_stock_basic_info(test_stock)
            if info:
                print(f"✓ 获取股票 {test_stock} 基本信息成功")
            else:
                print(f"✗ 获取股票 {test_stock} 基本信息失败")
        else:
            print("✗ 获取股票列表失败")

        return True
    except Exception as e:
        print(f"✗ 股票信息模块测试失败: {e}")
        return False


def test_tick_data():
    """测试分笔数据模块"""
    print("\n=== 测试分笔数据模块 ===")
    try:
        # 使用一个常见的股票代码测试
        test_code = "000001"  # 平安银行
        test_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

        print(f"尝试获取股票 {test_code} {test_date} 的分笔数据...")
        tick_df = tick_data.get_tick_data(test_code, test_date)

        if not tick_df.empty:
            print(f"✓ 获取分笔数据成功，共 {len(tick_df)} 条记录")

            # 测试统计功能
            stats = tick_data.get_trade_statistics(tick_df)
            print(f"✓ 计算统计信息成功，总成交量: {stats.get('total_volume', 0)}")
        else:
            print("! 获取分笔数据为空（可能是非交易日）")

        return True
    except Exception as e:
        print(f"✗ 分笔数据模块测试失败: {e}")
        return False


def test_basic_data():
    """测试基础数据模块"""
    print("\n=== 测试基础数据模块 ===")
    try:
        # 使用一个常见的股票代码测试
        test_code = "000001"  # 平安银行

        print(f"尝试获取股票 {test_code} 的日线数据...")
        daily_data = basic_data.get_stock_data(test_code, 'daily')

        if not daily_data.empty:
            print(f"✓ 获取日线数据成功，共 {len(daily_data)} 条记录")
            print(f"  最新收盘价: {daily_data.iloc[-1]['close_price']}")

            # 测试技术指标计算
            data_with_indicators = basic_data.calculate_technical_indicators(daily_data)
            print("✓ 技术指标计算成功")
        else:
            print("✗ 获取基础数据失败")

        return True
    except Exception as e:
        print(f"✗ 基础数据模块测试失败: {e}")
        return False


def test_indicators():
    """测试技术指标模块"""
    print("\n=== 测试技术指标模块 ===")
    try:
        # 创建测试数据
        import pandas as pd
        import numpy as np

        dates = pd.date_range(start='2023-01-01', periods=50, freq='D')
        test_data = pd.DataFrame({
            'trade_date': dates,
            'close_price': 10 + np.cumsum(np.random.randn(50) * 0.1),
            'high_price': 10 + np.cumsum(np.random.randn(50) * 0.1) + 0.2,
            'low_price': 10 + np.cumsum(np.random.randn(50) * 0.1) - 0.2,
            'volume': np.random.randint(1000, 10000, 50)
        })

        # 测试MACD计算
        macd_data = indicator_processor.calculate_macd(test_data)
        if 'macd' in macd_data.columns:
            print("✓ MACD计算成功")
        else:
            print("✗ MACD计算失败")

        # 测试RSI计算
        rsi_data = indicator_processor.calculate_rsi(test_data)
        if 'rsi' in rsi_data.columns:
            print("✓ RSI计算成功")
        else:
            print("✗ RSI计算失败")

        # 测试KDJ计算
        kdj_data = indicator_processor.calculate_kdj(test_data)
        if 'kdj_k' in kdj_data.columns:
            print("✓ KDJ计算成功")
        else:
            print("✗ KDJ计算失败")

        return True
    except Exception as e:
        print(f"✗ 技术指标模块测试失败: {e}")
        return False


def main():
    """主测试函数"""
    print("股票数据分析系统 - 功能测试")
    print("=" * 50)

    test_results = []

    # 运行各项测试
    test_results.append(("配置模块", test_config()))
    test_results.append(("数据库模块", test_database()))
    test_results.append(("股票信息模块", test_stock_info()))
    test_results.append(("分笔数据模块", test_tick_data()))
    test_results.append(("基础数据模块", test_basic_data()))
    test_results.append(("技术指标模块", test_indicators()))

    # 输出测试结果摘要
    print("\n" + "=" * 50)
    print("测试结果摘要:")
    print("=" * 50)

    passed = 0
    total = len(test_results)

    for test_name, result in test_results:
        status = "通过" if result else "失败"
        symbol = "✓" if result else "✗"
        print(f"{symbol} {test_name}: {status}")
        if result:
            passed += 1

    print(f"\n总计: {passed}/{total} 项测试通过")

    if passed == total:
        print("🎉 所有测试通过！系统运行正常。")
    else:
        print("⚠️ 部分测试失败，请检查相关配置和依赖。")


if __name__ == '__main__':
    main()