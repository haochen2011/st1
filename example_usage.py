#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
使用示例
演示如何使用股票数据分析系统的各个功能
"""

import sys
import os
from datetime import datetime, timedelta

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from stock_info import stock_info
from tick_data import tick_data
from basic_data import basic_data
from indicator_processor import indicator_processor
from tick_processor import tick_processor


def example_stock_info():
    """示例：股票信息管理"""
    print("=" * 50)
    print("示例：股票信息管理")
    print("=" * 50)

    # 1. 获取股票列表
    print("1. 获取上海股票列表...")
    stocks = stock_info.get_stock_list('sh')
    if not stocks.empty:
        print(f"获取到 {len(stocks)} 只上海股票")
        print(stocks.head())

    # 2. 获取单只股票信息
    print("\n2. 获取平安银行基本信息...")
    info = stock_info.get_stock_basic_info('000001')
    if info:
        print(f"股票信息: {info}")

    # 3. 更新股票信息到数据库（可选，比较耗时）
    # stock_info.update_stock_info_to_db()


def example_tick_data():
    """示例：分笔数据处理"""
    print("\n" + "=" * 50)
    print("示例：分笔数据处理")
    print("=" * 50)

    stock_code = "000001"  # 平安银行
    trade_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

    # 1. 获取分笔数据
    print(f"1. 获取股票 {stock_code} {trade_date} 的分笔数据...")
    tick_df = tick_data.get_tick_data(stock_code, trade_date)

    if not tick_df.empty:
        print(f"获取到 {len(tick_df)} 条分笔数据")
        print(tick_df.head())

        # 2. 计算统计信息
        print("\n2. 计算分笔数据统计信息...")
        stats = tick_data.get_trade_statistics(tick_df)
        print(f"统计信息: {stats}")

        # 3. 保存数据
        print("\n3. 保存分笔数据...")
        result = tick_data.download_and_save_tick_data(stock_code, trade_date)
        print(f"保存结果: {result}")

        # 4. 重采样为K线数据
        print("\n4. 重采样为5分钟K线...")
        kline_5min = tick_processor.resample_tick_to_kline(tick_df, '5min')
        if not kline_5min.empty:
            print(f"生成 {len(kline_5min)} 条5分钟K线数据")
            print(kline_5min.head())

        # 5. 分析大单
        print("\n5. 分析大单交易...")
        large_orders = tick_processor.analyze_large_orders(tick_df, large_threshold=50000)
        if not large_orders.empty:
            print(f"发现 {len(large_orders)} 个时间段有大单交易")
            print(large_orders.head())
    else:
        print("未获取到分笔数据（可能是非交易日）")


def example_basic_data():
    """示例：基础数据处理"""
    print("\n" + "=" * 50)
    print("示例：基础数据处理")
    print("=" * 50)

    stock_code = "000001"  # 平安银行

    # 1. 获取日线数据
    print(f"1. 获取股票 {stock_code} 的日线数据...")
    daily_data = basic_data.get_stock_data(stock_code, 'daily')

    if not daily_data.empty:
        print(f"获取到 {len(daily_data)} 条日线数据")
        print(daily_data.tail())

        # 2. 计算技术指标
        print("\n2. 计算基础技术指标...")
        data_with_indicators = basic_data.calculate_technical_indicators(daily_data)
        print("技术指标列:", [col for col in data_with_indicators.columns if 'ma_' in col])

        # 3. 保存到数据库
        print("\n3. 保存基础数据到数据库...")
        basic_data.save_basic_data_to_db(data_with_indicators)

        # 4. 从数据库读取
        print("\n4. 从数据库读取基础数据...")
        db_data = basic_data.get_basic_data_from_db(stock_code, 'daily')
        print(f"从数据库读取到 {len(db_data)} 条数据")

        # 5. 获取最新数据
        print("\n5. 获取最新交易数据...")
        latest = basic_data.get_latest_data(stock_code, 'daily')
        if not latest.empty:
            print(f"最新收盘价: {latest.iloc[0]['close_price']}")
    else:
        print("未获取到基础数据")


def example_indicators():
    """示例：技术指标计算"""
    print("\n" + "=" * 50)
    print("示例：技术指标计算")
    print("=" * 50)

    stock_code = "000001"  # 平安银行

    # 获取基础数据
    print(f"获取股票 {stock_code} 的基础数据进行指标计算...")
    daily_data = basic_data.get_stock_data(stock_code, 'daily')

    if not daily_data.empty:
        print(f"基础数据: {len(daily_data)} 条")

        # 1. 计算MACD
        print("\n1. 计算MACD指标...")
        macd_data = indicator_processor.calculate_macd(daily_data)
        if 'macd' in macd_data.columns:
            latest_macd = macd_data['macd'].iloc[-1]
            latest_signal = macd_data['macd_signal'].iloc[-1]
            print(f"最新MACD: {latest_macd:.4f}, 信号线: {latest_signal:.4f}")

        # 2. 计算RSI
        print("\n2. 计算RSI指标...")
        rsi_data = indicator_processor.calculate_rsi(daily_data)
        if 'rsi' in rsi_data.columns:
            latest_rsi = rsi_data['rsi'].iloc[-1]
            print(f"最新RSI: {latest_rsi:.2f}")

        # 3. 计算KDJ
        print("\n3. 计算KDJ指标...")
        kdj_data = indicator_processor.calculate_kdj(daily_data)
        if 'kdj_k' in kdj_data.columns:
            latest_k = kdj_data['kdj_k'].iloc[-1]
            latest_d = kdj_data['kdj_d'].iloc[-1]
            latest_j = kdj_data['kdj_j'].iloc[-1]
            print(f"最新KDJ: K={latest_k:.2f}, D={latest_d:.2f}, J={latest_j:.2f}")

        # 4. 保存指标到数据库（示例）
        print("\n4. 保存指标数据到数据库...")
        indicators_dict = {}
        if 'macd' in macd_data.columns:
            indicators_dict['MACD'] = macd_data['macd']
        if 'rsi' in rsi_data.columns:
            indicators_dict['RSI'] = rsi_data['rsi']

        # indicator_processor.save_indicators_to_db(stock_code, 'daily', indicators_dict)
    else:
        print("未获取到基础数据，无法计算指标")


def example_complete_workflow():
    """示例：完整的数据处理流程"""
    print("\n" + "=" * 50)
    print("示例：完整的数据处理流程")
    print("=" * 50)

    stock_code = "000001"

    print(f"开始处理股票 {stock_code} 的完整数据流程...")

    # 1. 更新基础数据
    print("\n1. 更新基础数据...")
    updated_data = basic_data.update_basic_data(stock_code, ['daily'])

    # 2. 获取并处理分笔数据
    print("\n2. 处理分笔数据...")
    trade_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    tick_result = tick_data.download_and_save_tick_data(stock_code, trade_date)

    # 3. 计算技术指标
    print("\n3. 计算技术指标...")
    if 'daily' in updated_data:
        daily_df = updated_data['daily']

        # 计算各种指标
        daily_df = indicator_processor.calculate_macd(daily_df)
        daily_df = indicator_processor.calculate_rsi(daily_df)
        daily_df = indicator_processor.calculate_kdj(daily_df)

        print("计算完成的指标列:")
        indicator_cols = [col for col in daily_df.columns if any(x in col for x in ['macd', 'rsi', 'kdj'])]
        print(indicator_cols)

    print("\n完整流程处理完成！")


def main():
    """主函数"""
    print("股票数据分析系统 - 使用示例")
    print("请确保已经配置好数据库连接和安装了所有依赖")

    examples = [
        ("股票信息管理", example_stock_info),
        ("分笔数据处理", example_tick_data),
        ("基础数据处理", example_basic_data),
        ("技术指标计算", example_indicators),
        ("完整数据流程", example_complete_workflow)
    ]

    print("\n可用的示例:")
    for i, (name, _) in enumerate(examples, 1):
        print(f"{i}. {name}")
    print("0. 运行所有示例")

    try:
        choice = input("\n请选择要运行的示例 (0-5): ").strip()

        if choice == '0':
            # 运行所有示例
            for name, func in examples:
                print(f"\n正在运行: {name}")
                try:
                    func()
                except Exception as e:
                    print(f"示例 {name} 运行失败: {e}")
        elif choice in ['1', '2', '3', '4', '5']:
            index = int(choice) - 1
            name, func = examples[index]
            print(f"\n正在运行: {name}")
            func()
        else:
            print("无效的选择")

    except KeyboardInterrupt:
        print("\n用户中断操作")
    except Exception as e:
        print(f"\n运行示例时出错: {e}")


if __name__ == '__main__':
    main()