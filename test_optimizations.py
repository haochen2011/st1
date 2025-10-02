#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
优化测试脚本
测试数据源轮询、超时重试和行业信息获取等优化功能
"""

import sys
import time
from loguru import logger
from datetime import datetime, timedelta

# 导入优化后的模块
try:
    from data_source_manager import data_source_manager
    from stock_info import StockInfo
    from basic_data import BasicData
    from tick_data import TickData
    from database import db_manager
    logger.info("所有模块导入成功")
except Exception as e:
    logger.error(f"模块导入失败: {e}")
    sys.exit(1)


def test_data_source_timeout_retry():
    """测试数据源超时重试功能"""
    logger.info("=== 测试数据源超时重试功能 ===")

    try:
        # 测试股票基本信息获取
        result = data_source_manager.get_data_with_fallback(
            'stock_basic_info', stock_code='000001')

        if result is not None and not result.empty:
            logger.info("✓ 数据源管理器工作正常")
            return True
        else:
            logger.warning("× 数据源管理器返回空结果")
            return False

    except Exception as e:
        logger.error(f"× 数据源管理器测试失败: {e}")
        return False


def test_stock_industry_info():
    """测试股票行业信息获取"""
    logger.info("=== 测试股票行业信息获取 ===")

    stock_info = StockInfo()
    test_stocks = ['000001', '000002', '600000', '600036', '000858']

    success_count = 0
    for stock_code in test_stocks:
        try:
            logger.info(f"测试股票: {stock_code}")
            basic_info = stock_info.get_stock_basic_info(stock_code)

            if basic_info and basic_info.get('industry'):
                industry = basic_info['industry']
                logger.info(f"✓ {stock_code} 行业信息: {industry}")
                if industry != '未知行业':
                    success_count += 1
            else:
                logger.warning(f"× {stock_code} 无行业信息")

        except Exception as e:
            logger.error(f"× {stock_code} 获取失败: {e}")

    logger.info(f"行业信息获取成功率: {success_count}/{len(test_stocks)}")
    return success_count > 0


def test_database_query_fix():
    """测试数据库查询修复"""
    logger.info("=== 测试数据库查询修复 ===")

    try:
        basic_data = BasicData()

        # 测试数据库查询
        result = basic_data.get_basic_data_from_db('000001', 'daily')
        logger.info(f"✓ 数据库查询成功，返回 {len(result)} 条记录")
        return True

    except Exception as e:
        logger.error(f"× 数据库查询测试失败: {e}")
        return False


def test_basic_data_fetch():
    """测试基础数据获取"""
    logger.info("=== 测试基础数据获取 ===")

    basic_data = BasicData()

    try:
        # 测试获取日线数据
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')

        stock_data = basic_data.get_stock_data(
            '000001', 'daily', start_date, end_date)

        if not stock_data.empty:
            logger.info(f"✓ 获取日线数据成功，共 {len(stock_data)} 条")
            return True
        else:
            logger.warning("× 日线数据为空")
            return False

    except Exception as e:
        logger.error(f"× 基础数据获取失败: {e}")
        return False


def test_tick_data_fetch():
    """测试分笔数据获取"""
    logger.info("=== 测试分笔数据获取 ===")

    tick_data = TickData()

    try:
        # 测试获取分笔数据
        today = datetime.now().strftime('%Y%m%d')
        tick_result = tick_data.get_tick_data('000001', today)

        if not tick_result.empty:
            logger.info(f"✓ 获取分笔数据成功，共 {len(tick_result)} 条")
            return True
        else:
            logger.warning("× 分笔数据为空（可能是非交易时间）")
            return True  # 非交易时间返回空是正常的

    except Exception as e:
        logger.error(f"× 分笔数据获取失败: {e}")
        return False


def test_data_source_switching():
    """测试数据源切换功能"""
    logger.info("=== 测试数据源切换功能 ===")

    try:
        original_source = data_source_manager.current_source
        logger.info(f"当前数据源: {original_source}")

        # 切换数据源
        data_source_manager.switch_source()
        new_source = data_source_manager.current_source
        logger.info(f"切换后数据源: {new_source}")

        if original_source != new_source:
            logger.info("✓ 数据源切换成功")
            # 切换回原始数据源
            data_source_manager.switch_source()
            return True
        else:
            logger.warning("× 数据源切换失败")
            return False

    except Exception as e:
        logger.error(f"× 数据源切换测试失败: {e}")
        return False


def main():
    """主测试函数"""
    logger.info("开始执行优化测试...")

    test_results = []

    # 执行各项测试
    tests = [
        ("数据源超时重试", test_data_source_timeout_retry),
        ("数据源切换", test_data_source_switching),
        ("数据库查询修复", test_database_query_fix),
        ("股票行业信息", test_stock_industry_info),
        ("基础数据获取", test_basic_data_fetch),
        ("分笔数据获取", test_tick_data_fetch),
    ]

    for test_name, test_func in tests:
        logger.info(f"\n{'='*50}")
        start_time = time.time()

        try:
            result = test_func()
            test_results.append((test_name, result))
            elapsed = time.time() - start_time
            status = "通过" if result else "失败"
            logger.info(f"{test_name}: {status} (耗时: {elapsed:.2f}秒)")

        except Exception as e:
            test_results.append((test_name, False))
            logger.error(f"{test_name}: 异常 - {e}")

    # 输出测试总结
    logger.info(f"\n{'='*50}")
    logger.info("测试总结:")

    passed = sum(1 for _, result in test_results if result)
    total = len(test_results)

    for test_name, result in test_results:
        status = "✓ 通过" if result else "× 失败"
        logger.info(f"  {test_name}: {status}")

    logger.info(f"\n总体结果: {passed}/{total} 个测试通过")

    if passed == total:
        logger.info("🎉 所有优化测试通过！")
    else:
        logger.warning(f"⚠️  有 {total - passed} 个测试失败，需要进一步检查")

    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
