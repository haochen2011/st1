#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分笔数据处理器模块
负责分笔数据的高级处理和分析
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from loguru import logger
from data.tick_data import tick_data
from core.config import config


class TickProcessor:
    """分笔数据处理器类"""

    def __init__(self):
        self.periods = config.get_periods()

    def resample_tick_to_kline(self, tick_df, period='1min'):
        """将分笔数据重采样为K线数据"""
        if tick_df.empty:
            logger.warning("分笔数据为空，无法重采样")
            return pd.DataFrame()

        try:
            # 设置时间索引
            tick_df = tick_df.copy()
            tick_df.set_index('trade_time', inplace=True)

            # 定义重采样规则
            period_mapping = {
                '1min': '1T',
                '5min': '5T',
                '10min': '10T',
                '15min': '15T',
                '30min': '30T',
                '1hour': '1H',
                'daily': '1D',
                'week': '1W',
                'month': '1M',
                'quarter': '1Q',
                'half-year': '6M',
                'year': '1Y'
            }

            freq = period_mapping.get(period, '1T')

            # 重采样规则
            agg_rules = {
                'price': ['first', 'last', 'max', 'min'],
                'volume': 'sum',
                'amount': 'sum',
                'stock_code': 'first',
                'trade_date': 'first'
            }

            # 执行重采样
            resampled = tick_df.resample(freq).agg(agg_rules)

            # 重新构造列名
            resampled.columns = ['open_price', 'close_price', 'high_price', 'low_price', 'volume', 'amount',
                                 'stock_code', 'trade_date']

            # 删除空行
            resampled = resampled.dropna(subset=['open_price'])

            # 计算涨跌额和涨跌幅
            resampled['change_price'] = resampled['close_price'] - resampled['open_price']
            resampled['change_pct'] = (resampled['change_price'] / resampled['open_price']) * 100

            # 添加周期类型
            resampled['period_type'] = period

            # 重置索引
            resampled.reset_index(inplace=True)
            resampled['trade_date'] = resampled['trade_time'].dt.date

            logger.info(f"分笔数据重采样为 {period} 周期K线成功，共 {len(resampled)} 条")
            return resampled

        except Exception as e:
            logger.error(f"分笔数据重采样失败: {e}")
            return pd.DataFrame()

    def calculate_vwap(self, tick_df, window_minutes=5):
        """计算成交量加权平均价格 (VWAP)"""
        if tick_df.empty:
            return pd.DataFrame()

        try:
            tick_df = tick_df.copy()
            tick_df.set_index('trade_time', inplace=True)

            # 计算滑动窗口的VWAP
            window = f"{window_minutes}T"

            # 计算累计成交量和成交额
            rolling_amount = tick_df['amount'].rolling(window).sum()
            rolling_volume = tick_df['volume'].rolling(window).sum()

            # 计算VWAP
            tick_df['vwap'] = rolling_amount / rolling_volume
            tick_df['vwap'] = tick_df['vwap'].fillna(tick_df['price'])

            logger.info(f"计算 {window_minutes} 分钟VWAP成功")
            return tick_df.reset_index()

        except Exception as e:
            logger.error(f"计算VWAP失败: {e}")
            return pd.DataFrame()

    def analyze_large_orders(self, tick_df, large_threshold=100000):
        """分析大单交易"""
        if tick_df.empty:
            return pd.DataFrame()

        try:
            # 筛选大单
            large_orders = tick_df[tick_df['amount'] >= large_threshold].copy()

            if large_orders.empty:
                logger.info("没有找到大单交易")
                return pd.DataFrame()

            # 统计大单信息
            large_orders['is_large_buy'] = large_orders['trade_type'] == '买盘'
            large_orders['is_large_sell'] = large_orders['trade_type'] == '卖盘'

            # 按时间段统计
            large_orders.set_index('trade_time', inplace=True)

            # 5分钟统计
            summary = large_orders.resample('5T').agg({
                'amount': ['count', 'sum'],
                'volume': 'sum',
                'is_large_buy': 'sum',
                'is_large_sell': 'sum',
                'price': ['min', 'max', 'mean']
            })

            summary.columns = ['order_count', 'total_amount', 'total_volume', 'buy_count', 'sell_count', 'min_price',
                               'max_price', 'avg_price']
            summary = summary.dropna()

            # 计算大单净流入
            summary['net_inflow_count'] = summary['buy_count'] - summary['sell_count']

            logger.info(f"分析大单交易成功，阈值: {large_threshold}")
            return summary.reset_index()

        except Exception as e:
            logger.error(f"分析大单交易失败: {e}")
            return pd.DataFrame()

    def calculate_order_flow(self, tick_df, window_minutes=1):
        """计算订单流指标"""
        if tick_df.empty:
            return pd.DataFrame()

        try:
            tick_df = tick_df.copy()
            tick_df.set_index('trade_time', inplace=True)

            window = f"{window_minutes}T"

            # 分离买卖单
            buy_mask = tick_df['trade_type'] == '买盘'
            sell_mask = tick_df['trade_type'] == '卖盘'

            # 计算买卖量
            tick_df['buy_volume'] = np.where(buy_mask, tick_df['volume'], 0)
            tick_df['sell_volume'] = np.where(sell_mask, tick_df['volume'], 0)
            tick_df['buy_amount'] = np.where(buy_mask, tick_df['amount'], 0)
            tick_df['sell_amount'] = np.where(sell_mask, tick_df['amount'], 0)

            # 滑动窗口统计
            rolling_stats = tick_df.rolling(window).agg({
                'buy_volume': 'sum',
                'sell_volume': 'sum',
                'buy_amount': 'sum',
                'sell_amount': 'sum',
                'volume': 'sum',
                'amount': 'sum'
            })

            # 计算订单流指标
            rolling_stats['volume_ratio'] = rolling_stats['buy_volume'] / (
                        rolling_stats['buy_volume'] + rolling_stats['sell_volume'])
            rolling_stats['amount_ratio'] = rolling_stats['buy_amount'] / (
                        rolling_stats['buy_amount'] + rolling_stats['sell_amount'])
            rolling_stats['net_volume'] = rolling_stats['buy_volume'] - rolling_stats['sell_volume']
            rolling_stats['net_amount'] = rolling_stats['buy_amount'] - rolling_stats['sell_amount']

            # 合并原始数据
            result = tick_df.join(rolling_stats, rsuffix='_window')
            result = result.dropna()

            logger.info(f"计算 {window_minutes} 分钟订单流指标成功")
            return result.reset_index()

        except Exception as e:
            logger.error(f"计算订单流指标失败: {e}")
            return pd.DataFrame()

    def detect_price_anomalies(self, tick_df, threshold=3):
        """检测价格异常"""
        if tick_df.empty:
            return pd.DataFrame()

        try:
            tick_df = tick_df.copy()

            # 计算价格变化率
            tick_df['price_change_pct'] = tick_df['price'].pct_change() * 100

            # 计算移动平均和标准差
            window = 20
            tick_df['price_ma'] = tick_df['price'].rolling(window).mean()
            tick_df['price_std'] = tick_df['price'].rolling(window).std()

            # 检测异常（价格偏离移动平均超过threshold个标准差）
            tick_df['price_zscore'] = (tick_df['price'] - tick_df['price_ma']) / tick_df['price_std']
            tick_df['is_anomaly'] = np.abs(tick_df['price_zscore']) > threshold

            # 筛选异常点
            anomalies = tick_df[tick_df['is_anomaly'] == True].copy()

            if not anomalies.empty:
                logger.info(f"检测到 {len(anomalies)} 个价格异常点")
            else:
                logger.info("未检测到价格异常")

            return anomalies

        except Exception as e:
            logger.error(f"检测价格异常失败: {e}")
            return pd.DataFrame()

    def calculate_tick_indicators(self, tick_df):
        """计算分笔级别的技术指标"""
        if tick_df.empty:
            return pd.DataFrame()

        try:
            tick_df = tick_df.copy()
            tick_df = tick_df.sort_values('trade_time')

            # 计算移动平均
            for window in [5, 10, 20]:
                tick_df[f'ma_{window}'] = tick_df['price'].rolling(window).mean()

            # 计算价格动量
            tick_df['momentum_3'] = tick_df['price'] - tick_df['price'].shift(3)
            tick_df['momentum_5'] = tick_df['price'] - tick_df['price'].shift(5)

            # 计算成交量移动平均
            tick_df['volume_ma_10'] = tick_df['volume'].rolling(10).mean()
            tick_df['volume_ratio'] = tick_df['volume'] / tick_df['volume_ma_10']

            # 计算相对强弱指标 (简化版RSI)
            price_change = tick_df['price'].diff()
            gain = price_change.where(price_change > 0, 0)
            loss = -price_change.where(price_change < 0, 0)

            window = 14
            avg_gain = gain.rolling(window).mean()
            avg_loss = loss.rolling(window).mean()

            rs = avg_gain / avg_loss
            tick_df['rsi'] = 100 - (100 / (1 + rs))

            logger.info("计算分笔技术指标成功")
            return tick_df

        except Exception as e:
            logger.error(f"计算分笔技术指标失败: {e}")
            return tick_df

    def export_processed_data(self, processed_data, filename, export_path=None):
        """导出处理后的数据"""
        if processed_data.empty:
            logger.warning("处理后的数据为空，跳过导出")
            return None

        try:
            if export_path is None:
                export_path = config.get_data_path('tick_data')

            filepath = f"{export_path}/{filename}"

            if filepath.endswith('.xlsx'):
                processed_data.to_excel(filepath, index=False)
            elif filepath.endswith('.csv'):
                processed_data.to_csv(filepath, index=False, encoding='utf-8-sig')
            else:
                filepath += '.xlsx'
                processed_data.to_excel(filepath, index=False)

            logger.info(f"处理后的数据已导出到: {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"导出处理后的数据失败: {e}")
            return None


# 创建全局实例
tick_processor = TickProcessor()
