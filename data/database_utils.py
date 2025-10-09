#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库工具模块
用于检查和创建必要的数据库表
"""

from loguru import logger
from core.config import config


def ensure_basic_data_tables():
    """确保所有基础数据表都存在"""
    try:
        from data.enhanced_database import enhanced_db_manager

        # 支持的时间周期
        periods = ['1min', '5min', '10min', '15min', '30min', '1hour', 'daily', 'week', 'month', 'quarter', 'half_year',
                   'year']

        created_tables = []
        existing_tables = []

        for period in periods:
            table_name = f"basic_data_{period}"

            if enhanced_db_manager.table_exists(table_name):
                existing_tables.append(table_name)
            else:
                try:
                    enhanced_db_manager.create_basic_data_table(period)
                    created_tables.append(table_name)
                    logger.info(f"创建数据表: {table_name}")
                except Exception as e:
                    logger.error(f"创建数据表 {table_name} 失败: {e}")

        print(f"✅ 数据表检查完成:")
        print(f"   已存在表: {len(existing_tables)} 个")
        print(f"   新创建表: {len(created_tables)} 个")

        if created_tables:
            print("   新创建的表:", ", ".join(created_tables))

        return True

    except Exception as e:
        logger.error(f"检查数据表失败: {e}")
        print(f"❌ 数据表检查失败: {e}")
        return False


def get_latest_data_date(period: str = 'daily'):
    """获取指定周期的最新数据日期"""
    try:
        from data.enhanced_database import enhanced_db_manager

        table_name = f"basic_data_{period}"

        if not enhanced_db_manager.table_exists(table_name):
            return None

        sql = f"SELECT MAX(trade_date) as latest_date FROM {table_name}"
        result = enhanced_db_manager.safe_query_to_dataframe(
            sql,
            required_tables=[table_name]
        )

        if result.empty:
            return None

        return result.iloc[0]['latest_date']

    except Exception as e:
        logger.error(f"获取最新数据日期失败: {e}")
        return None


def get_data_statistics(period: str = 'daily'):
    """获取指定周期的数据统计信息"""
    try:
        from data.enhanced_database import enhanced_db_manager

        table_name = f"basic_data_{period}"

        if not enhanced_db_manager.table_exists(table_name):
            return {
                'table_exists': False,
                'total_records': 0,
                'stock_count': 0,
                'latest_date': None,
                'earliest_date': None
            }

        # 获取基本统计信息
        sql = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT stock_code) as stock_count,
            MAX(trade_date) as latest_date,
            MIN(trade_date) as earliest_date
        FROM {table_name}
        """

        result = enhanced_db_manager.safe_query_to_dataframe(
            sql,
            required_tables=[table_name]
        )

        if result.empty:
            return {
                'table_exists': True,
                'total_records': 0,
                'stock_count': 0,
                'latest_date': None,
                'earliest_date': None
            }

        stats = result.iloc[0].to_dict()
        stats['table_exists'] = True

        return stats

    except Exception as e:
        logger.error(f"获取数据统计信息失败: {e}")
        return {
            'table_exists': False,
            'total_records': 0,
            'stock_count': 0,
            'latest_date': None,
            'earliest_date': None,
            'error': str(e)
        }


def get_top_stocks_by_amount(period: str = 'daily', limit: int = 50, days: int = 30):
    """获取按成交额排序的热门股票"""
    try:
        from data.enhanced_database import enhanced_db_manager
        from datetime import datetime, timedelta

        table_name = f"basic_data_{period}"

        if not enhanced_db_manager.table_exists(table_name):
            return []

        # 计算起始日期
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)

        sql = f"""
        SELECT 
            stock_code, 
            AVG(amount) as avg_amount,
            COUNT(*) as trade_days
        FROM {table_name}
        WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
        GROUP BY stock_code
        HAVING trade_days >= 5  -- 至少有5个交易日的数据
        ORDER BY avg_amount DESC
        LIMIT {limit}
        """

        result = enhanced_db_manager.safe_query_to_dataframe(
            sql,
            required_tables=[table_name]
        )

        return result.to_dict('records') if not result.empty else []

    except Exception as e:
        logger.error(f"获取热门股票失败: {e}")
        return []