#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指标处理器模块
负责技术指标的计算和管理
"""

import pandas as pd
import numpy as np
import talib
from datetime import datetime
from loguru import logger
from database import db_manager
from config import config


class IndicatorProcessor:
    """技术指标处理器类"""

    def __init__(self):
        self.indicators = [
            'SMA', 'EMA', 'MACD', 'RSI', 'BOLL', 'KDJ', 'CCI', 'WR', 'BIAS'
        ]

    def calculate_macd(self, data, fast=12, slow=26, signal=9):
        """计算MACD指标"""
        try:
            close_prices = data['close_price'].values
            macd, macdsignal, macdhist = talib.MACD(close_prices, fast, slow, signal)

            data['macd'] = macd
            data['macd_signal'] = macdsignal
            data['macd_hist'] = macdhist

            return data
        except Exception as e:
            logger.error(f"计算MACD失败: {e}")
            return data

    def calculate_rsi(self, data, window=14):
        """计算RSI指标"""
        try:
            close_prices = data['close_price'].values
            rsi = talib.RSI(close_prices, window)
            data['rsi'] = rsi
            return data
        except Exception as e:
            logger.error(f"计算RSI失败: {e}")
            return data

    def calculate_kdj(self, data, k_window=9, d_window=3, j_window=3):
        """计算KDJ指标"""
        try:
            high_prices = data['high_price'].values
            low_prices = data['low_price'].values
            close_prices = data['close_price'].values

            k, d = talib.STOCH(high_prices, low_prices, close_prices,
                               fastk_period=k_window, slowk_period=d_window, slowd_period=j_window)
            j = 3 * k - 2 * d

            data['kdj_k'] = k
            data['kdj_d'] = d
            data['kdj_j'] = j

            return data
        except Exception as e:
            logger.error(f"计算KDJ失败: {e}")
            return data

    def save_indicators_to_db(self, stock_code, period, indicators_data):
        """保存指标数据到数据库"""
        try:
            for indicator_name, values in indicators_data.items():
                if isinstance(values, pd.Series):
                    for i, value in enumerate(values):
                        if pd.notna(value):
                            sql = """
                            INSERT INTO indicator_data (stock_code, trade_date, period_type, indicator_name, indicator_value)
                            VALUES (:stock_code, :trade_date, :period, :indicator_name, :value)
                            ON DUPLICATE KEY UPDATE indicator_value = VALUES(indicator_value)
                            """
                            params = {
                                'stock_code': stock_code,
                                'trade_date': indicators_data.index[i] if hasattr(indicators_data,
                                                                                  'index') else datetime.now().date(),
                                'period': period,
                                'indicator_name': indicator_name,
                                'value': float(value)
                            }
                            db_manager.execute_sql(sql, params)

            logger.info(f"保存指标数据成功: {stock_code}")
        except Exception as e:
            logger.error(f"保存指标数据失败: {e}")


# 创建全局实例
indicator_processor = IndicatorProcessor()