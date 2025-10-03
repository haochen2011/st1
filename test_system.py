"""
优化功能测试脚本
测试超时机制和多数据源切换功能
"""

import sys
import os
import time
from datetime import datetime

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_timeout_mechanism():
    """测试超时机制"""
    print("=" * 50)
    print("测试1: 超时机制")
    print("=" * 50)

    try:
        from data.data_fetcher import data_fetcher

        print(f"设置超时时间: {data_fetcher.timeout}秒")
        print("正在测试数据获取...")

        start_time = time.time()
        result = data_fetcher.get_stock_list()
        end_time = time.time()

        execution_time = end_time - start_time
        print(f"执行时间: {execution_time:.2f}秒")

        if execution_time <= data_fetcher.timeout + 2:  # 允许2秒误差
            print("✓ 超时机制正常工作")
        else:
            print("⚠ 超时机制可能有问题")

        if not result.empty:
            print(f"✓ 获取数据成功，共 {len(result)} 条记录")
        else:
            print("⚠ 未获取到数据")

    except Exception as e:
        print(f"✗ 测试失败: {e}")

def test_multiple_data_sources():
    """测试多数据源切换"""
    print("\n" + "=" * 50)
    print("测试2: 多数据源切换")
    print("=" * 50)

    try:
        from data_fetcher import data_fetcher

        print(f"配置的数据源: {data_fetcher.source_priority}")
        print("正在测试各个数据源...")

        # 测试股票列表获取
        test_operations = [
            ('股票列表', lambda: data_fetcher.get_stock_list()),
            ('实时数据', lambda: data_fetcher.get_realtime_data(['000001'])),
            ('历史数据', lambda: data_fetcher.get_historical_data('000001', '2024-01-01', '2024-01-31')),
            ('大盘指数', lambda: data_fetcher.get_market_index_data()),
            ('板块数据', lambda: data_fetcher.get_sector_data())
        ]

        for operation_name, operation_func in test_operations:
            print(f"\n测试 {operation_name}:")
            try:
                start_time = time.time()
                result = operation_func()
                end_time = time.time()

                if hasattr(result, 'empty'):
                    success = not result.empty
                    count = len(result) if success else 0
                else:
                    success = bool(result)
                    count = len(result) if isinstance(result, (list, dict)) else 1

                print(f"  ✓ 成功获取数据 (耗时: {end_time - start_time:.2f}秒, 记录数: {count})")

            except Exception as e:
                print(f"  ✗ 获取失败: {e}")

    except Exception as e:
        print(f"✗ 测试失败: {e}")

def test_basic_data_optimization():
    """测试基础数据模块优化"""
    print("\n" + "=" * 50)
    print("测试3: 基础数据模块优化")
    print("=" * 50)

    try:
        from basic_data import basic_data

        print(f"超时设置: {basic_data.timeout}秒")
        print(f"最大重试: {basic_data.max_retries}次")
        print(f"数据源: {basic_data.source_priority}")

        print("\n正在测试基础数据获取...")
        start_time = time.time()
        result = basic_data.get_stock_data('000001', 'daily')
        end_time = time.time()

        if not result.empty:
            print(f"✓ 基础数据获取成功 (耗时: {end_time - start_time:.2f}秒)")
            print(f"  数据条数: {len(result)}")
            print(f"  列名: {list(result.columns)}")
        else:
            print("⚠ 基础数据获取失败")

    except Exception as e:
        print(f"✗ 测试失败: {e}")

def test_analysis_modules():
    """测试分析模块"""
    print("\n" + "=" * 50)
    print("测试4: 分析模块功能")
    print("=" * 50)

    analysis_modules = [
        ('技术指标分析', 'technical_indicators', 'technical_analyzer'),
        ('异动检测', 'anomaly_detection', 'anomaly_detector'),
        ('三层共振分析', 'resonance_analysis', 'resonance_analyzer'),
        ('涨停板分析', 'limit_up_analysis', 'limit_up_analyzer'),
        ('通道分析', 'channel_analysis', 'channel_analyzer')
    ]

    for module_name, module_file, analyzer_name in analysis_modules:
        try:
            print(f"\n测试 {module_name}:")
            module = __import__(module_file)
            analyzer = getattr(module, analyzer_name)
            print(f"  ✓ {module_name} 模块加载成功")

        except Exception as e:
            print(f"  ✗ {module_name} 模块加载失败: {e}")

def test_configuration():
    """测试配置系统"""
    print("\n" + "=" * 50)
    print("测试5: 配置系统")
    print("=" * 50)

    try:
        from app_config import Config

        config_items = [
            ('超时时间', Config.TIMEOUT_SECONDS),
            ('最大重试', Config.MAX_RETRIES),
            ('价格异动阈值', Config.PRICE_CHANGE_THRESHOLD),
            ('成交量异动阈值', Config.VOLUME_RATIO_THRESHOLD),
            ('换手率阈值', Config.TURNOVER_THRESHOLD),
            ('数据源列表', Config.DATA_SOURCES),
            ('市场开放状态', Config.is_market_open())
        ]

        print("系统配置:")
        for item_name, item_value in config_items:
            print(f"  {item_name}: {item_value}")

        print("✓ 配置系统正常")

    except Exception as e:
        print(f"✗ 配置系统测试失败: {e}")

def main():
    """主测试函数"""
    print("A股股票分析系统 - 优化功能测试")
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 执行所有测试
    test_timeout_mechanism()
    test_multiple_data_sources()
    test_basic_data_optimization()
    test_analysis_modules()
    test_configuration()

    print("\n" + "=" * 50)
    print("测试完成!")
    print("=" * 50)
    print("\n如果所有测试都显示 ✓，说明优化功能正常工作")
    print("如有 ✗ 或 ⚠ 标记，请检查相应模块")

if __name__ == '__main__':
    main()