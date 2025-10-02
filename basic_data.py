#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基础数据管理模块
负责股票基础数据（OHLCV）的获取、存储和管理
"""

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import os
from pathlib import Path
from loguru import logger
from database import db_manager
from config import config


class BasicData:
    """基础数据管理类"""

    def __init__(self):
        self.data_path = config.get_data_path('basic_data')
        self.periods = config.get_periods()

    def get_stock_data(self, stock_code, period='daily', start_date=None, end_date=None, adjust='qfq'):
        """获取股票基础数据"""
        try:
            if end_date is None:
                end_date = datetime.now().strftime('%Y%m%d')
            if start_date is None:
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')

            if period in ['1min', '5min', '15min', '30min', '60min']:
                stock_data = ak.stock_zh_a_hist_min_em(
                    symbol=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                    period=period.replace('min', ''),
                    adjust=adjust
                )
            else:
                period_mapping = {'daily': 'daily', 'week': 'weekly', 'month': 'monthly'}
                ak_period = period_mapping.get(period, 'daily')
                stock_data = ak.stock_zh_a_hist(
                    symbol=stock_code,
                    period=ak_period,
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust
                )

            if not stock_data.empty:
                stock_data = self._standardize_columns(stock_data, stock_code, period)
                logger.info(f"获取股票 {stock_code} {period} 周期数据成功，共 {len(stock_data)} 条")
                return stock_data
            else:
                logger.warning(f"股票 {stock_code} {period} 周期无数据")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"获取股票 {stock_code} {period} 周期数据失败: {e}")
            return pd.DataFrame()

    def _standardize_columns(self, data, stock_code, period):
        """标准化基础数据列名"""
        try:
            column_mapping = {
                '日期': 'trade_date', '时间': 'trade_date', '开盘': 'open_price',
                '收盘': 'close_price', '最高': 'high_price', '最低': 'low_price',
                '成交量': 'volume', '成交额': 'amount', '振幅': 'amplitude',
                '涨跌幅': 'change_pct', '涨跌额': 'change_price', '换手率': 'turnover_rate'
            }

            for old_col, new_col in column_mapping.items():
                if old_col in data.columns:
                    data = data.rename(columns={old_col: new_col})

            data['stock_code'] = stock_code
            data['period_type'] = period

            if 'trade_date' in data.columns:
                data['trade_date'] = pd.to_datetime(data['trade_date']).dt.date

            numeric_columns = ['open_price', 'close_price', 'high_price', 'low_price', 'volume', 'amount',
                               'change_price', 'change_pct', 'turnover_rate']
            for col in numeric_columns:
                if col in data.columns:
                    data[col] = pd.to_numeric(data[col], errors='coerce')

            if 'change_price' not in data.columns and 'open_price' in data.columns and 'close_price' in data.columns:
                data['change_price'] = data['close_price'] - data['open_price']

            if 'change_pct' not in data.columns and 'change_price' in data.columns and 'open_price' in data.columns:
                data['change_pct'] = (data['change_price'] / data['open_price']) * 100

            return data

        except Exception as e:
            logger.error(f"标准化基础数据失败: {e}")
            return data

    def save_basic_data_to_db(self, basic_data):
        """保存基础数据到数据库"""
        if basic_data.empty:
            logger.warning("基础数据为空，跳过保存")
            return

        try:
            columns = ['stock_code', 'trade_date', 'period_type', 'open_price', 'close_price', 'high_price',
                       'low_price', 'volume', 'amount', 'change_price', 'change_pct', 'turnover_rate']

            for col in columns:
                if col not in basic_data.columns:
                    basic_data[col] = None

            db_data = basic_data[columns].copy()
            db_manager.insert_dataframe(db_data, 'basic_data', if_exists='append')
            logger.info(f"成功保存 {len(db_data)} 条基础数据到数据库")

        except Exception as e:
            logger.error(f"保存基础数据到数据库失败: {e}")

    def get_basic_data_from_db(self, stock_code, period='daily', start_date=None, end_date=None):
        """从数据库获取基础数据"""
        try:
            sql = "SELECT * FROM basic_data WHERE stock_code = :stock_code AND period_type = :period"
            params = {'stock_code': stock_code, 'period': period}

            if start_date:
                sql += " AND trade_date >= :start_date"
                params['start_date'] = start_date

            if end_date:
                sql += " AND trade_date <= :end_date"
                params['end_date'] = end_date

            sql += " ORDER BY trade_date"

            basic_data = db_manager.query_to_dataframe(sql, params)
            logger.info(f"从数据库获取股票 {stock_code} {period} 周期基础数据成功，共 {len(basic_data)} 条")
            return basic_data

        except Exception as e:
            logger.error(f"从数据库获取基础数据失败: {e}")
            return pd.DataFrame()

    def update_basic_data(self, stock_code, periods=None, force_update=False):
        """更新基础数据"""
        if periods is None:
            periods = self.periods

        try:
            updated_data = {}

            for period in periods:
                try:
                    if not force_update:
                        existing_data = self.get_basic_data_from_db(stock_code, period)
                        if not existing_data.empty:
                            latest_date = existing_data['trade_date'].max()
                            if latest_date >= datetime.now().date():
                                logger.info(f"股票 {stock_code} {period} 周期数据已是最新，跳过更新")
                                continue

                    new_data = self.get_stock_data(stock_code, period)

                    if not new_data.empty:
                        self.save_basic_data_to_db(new_data)
                        updated_data[period] = new_data
                        logger.info(f"股票 {stock_code} {period} 周期数据更新成功")

                except Exception as e:
                    logger.error(f"更新股票 {stock_code} {period} 周期数据失败: {e}")
                    continue

            return updated_data

        except Exception as e:
            logger.error(f"更新基础数据失败: {e}")
            return {}

    def get_latest_data(self, stock_code, period='daily'):
        """获取最新的基础数据"""
        try:
            sql = """
            SELECT * FROM basic_data 
            WHERE stock_code = :stock_code AND period_type = :period 
            ORDER BY trade_date DESC 
            LIMIT 1
            """
            params = {'stock_code': stock_code, 'period': period}
            return db_manager.query_to_dataframe(sql, params)

        except Exception as e:
            logger.error(f"获取最新基础数据失败: {e}")
            return pd.DataFrame()

    def calculate_technical_indicators(self, basic_data):
        """计算基础技术指标"""
        if basic_data.empty:
            return basic_data

        try:
            data = basic_data.copy()
            data = data.sort_values('trade_date')

            for window in [5, 10, 20, 60]:
                data[f'ma_{window}'] = data['close_price'].rolling(window).mean()

            if 'change_pct' not in data.columns:
                data['change_pct'] = data['close_price'].pct_change() * 100

            if 'amplitude' not in data.columns:
                data['amplitude'] = ((data['high_price'] - data['low_price']) / data['close_price'].shift(1)) * 100

            if 'turnover_rate' not in data.columns:
                data['turnover_rate'] = None

            logger.info("计算基础技术指标成功")
            return data

        except Exception as e:
            logger.error(f"计算基础技术指标失败: {e}")
            return basic_data


# 创建全局实例
basic_data = BasicData()