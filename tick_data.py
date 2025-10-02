#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分笔数据管理模块
负责股票分笔数据的获取、存储和管理
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


class TickData:
    """分笔数据管理类"""

    def __init__(self):
        self.data_path = config.get_data_path('tick_data')

    def get_tick_data(self, stock_code, trade_date=None):
        """获取股票分笔数据"""
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y%m%d')
        elif isinstance(trade_date, (date, datetime)):
            trade_date = trade_date.strftime('%Y%m%d')

        try:
            # 使用akshare获取分笔数据
            tick_data = ak.stock_zh_a_tick_tx_js(symbol=stock_code)

            if not tick_data.empty:
                # 标准化列名
                tick_data = self._standardize_columns(tick_data, stock_code, trade_date)
                logger.info(f"获取股票 {stock_code} {trade_date} 分笔数据成功，共 {len(tick_data)} 条")
                return tick_data
            else:
                logger.warning(f"股票 {stock_code} {trade_date} 无分笔数据")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"获取股票 {stock_code} {trade_date} 分笔数据失败: {e}")
            return pd.DataFrame()

    def _standardize_columns(self, tick_data, stock_code, trade_date):
        """标准化分笔数据列名"""
        try:
            # 重命名列
            column_mapping = {
                '成交时间': 'trade_time',
                '成交价格': 'price',
                '价格变动': 'price_change',
                '成交量': 'volume',
                '成交金额': 'amount',
                '性质': 'trade_type'
            }

            # 重命名存在的列
            for old_col, new_col in column_mapping.items():
                if old_col in tick_data.columns:
                    tick_data = tick_data.rename(columns={old_col: new_col})

            # 添加股票代码和交易日期
            tick_data['stock_code'] = stock_code
            tick_data['trade_date'] = datetime.strptime(trade_date, '%Y%m%d').date()

            # 处理交易时间
            if 'trade_time' in tick_data.columns:
                tick_data['trade_time'] = pd.to_datetime(
                    trade_date + ' ' + tick_data['trade_time'].astype(str)
                )

            # 数据类型转换
            if 'price' in tick_data.columns:
                tick_data['price'] = pd.to_numeric(tick_data['price'], errors='coerce')
            if 'price_change' in tick_data.columns:
                tick_data['price_change'] = pd.to_numeric(tick_data['price_change'], errors='coerce')
            if 'volume' in tick_data.columns:
                tick_data['volume'] = pd.to_numeric(tick_data['volume'], errors='coerce')
            if 'amount' in tick_data.columns:
                tick_data['amount'] = pd.to_numeric(tick_data['amount'], errors='coerce')

            # 处理交易性质
            if 'trade_type' in tick_data.columns:
                tick_data['trade_type'] = tick_data['trade_type'].fillna('中性盘')

            return tick_data

        except Exception as e:
            logger.error(f"标准化分笔数据失败: {e}")
            return tick_data

    def save_tick_data_to_excel(self, tick_data, stock_code, trade_date, stock_name=None):
        """保存分笔数据到Excel文件"""
        if tick_data.empty:
            logger.warning("分笔数据为空，跳过保存")
            return None

        try:
            if isinstance(trade_date, str):
                trade_date = datetime.strptime(trade_date, '%Y%m%d').date()
            elif isinstance(trade_date, datetime):
                trade_date = trade_date.date()
            # 获取股票名称
            if stock_name is None:
                from stock_info import stock_info
                info = stock_info.get_stock_info_from_db(stock_code)
                if not info.empty:
                    stock_name = info.iloc[0]['stock_name']
                    market = info.iloc[0]['market']
                    stock_code = market + stock_code
                else:
                    stock_name = stock_code

            # 生成文件名
            date_str = trade_date.strftime('%Y%m%d')
            filename = f"{stock_name}_{stock_code}_tick_{date_str}.xlsx"
            filepath = os.path.join(self.data_path, filename)

            # 保存到Excel
            tick_data.to_excel(filepath, index=False)
            logger.info(f"分笔数据已保存到: {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"保存分笔数据到Excel失败: {e}")
            return None

    def load_tick_data_from_excel(self, filepath):
        """从Excel文件加载分笔数据"""
        try:
            tick_data = pd.read_excel(filepath)
            logger.info(f"从 {filepath} 加载分笔数据成功，共 {len(tick_data)} 条")
            return tick_data
        except Exception as e:
            logger.error(f"从 {filepath} 加载分笔数据失败: {e}")
            return pd.DataFrame()

    def save_tick_data_to_db(self, tick_data):
        """保存分笔数据到数据库"""
        if tick_data.empty:
            logger.warning("分笔数据为空，跳过保存")
            return

        try:
            # 选择需要的列
            columns = ['stock_code', 'trade_time', 'price', 'price_change', 'volume', 'amount', 'trade_type',
                       'trade_date']
            db_data = tick_data[columns].copy()

            # 插入数据库
            db_manager.insert_dataframe(db_data, 'tick_data', if_exists='append')
            logger.info(f"成功保存 {len(db_data)} 条分笔数据到数据库")

        except Exception as e:
            logger.error(f"保存分笔数据到数据库失败: {e}")

    def get_tick_data_from_db(self, stock_code, start_date=None, end_date=None):
        """从数据库获取分笔数据"""
        try:
            sql = "SELECT * FROM tick_data WHERE stock_code = :stock_code"
            params = {'stock_code': stock_code}

            if start_date:
                sql += " AND trade_date >= :start_date"
                params['start_date'] = start_date

            if end_date:
                sql += " AND trade_date <= :end_date"
                params['end_date'] = end_date

            sql += " ORDER BY trade_time"

            tick_data = db_manager.query_to_dataframe(sql, params)
            logger.info(f"从数据库获取股票 {stock_code} 分笔数据成功，共 {len(tick_data)} 条")
            return tick_data

        except Exception as e:
            logger.error(f"从数据库获取分笔数据失败: {e}")
            return pd.DataFrame()

    def download_and_save_tick_data(self, stock_code, trade_date=None, save_excel=True, save_db=True):
        """下载并保存分笔数据"""
        try:
            # 获取分笔数据
            tick_data = self.get_tick_data(stock_code, trade_date)

            if tick_data.empty:
                logger.warning(f"股票 {stock_code} {trade_date} 无分笔数据可保存")
                return None

            result = {}

            # 保存到Excel
            if save_excel:
                excel_path = self.save_tick_data_to_excel(tick_data, stock_code, trade_date)
                result['excel_path'] = excel_path

            # 保存到数据库
            if save_db:
                self.save_tick_data_to_db(tick_data)
                result['db_saved'] = True

            result['data_count'] = len(tick_data)
            return result

        except Exception as e:
            logger.error(f"下载并保存分笔数据失败: {e}")
            return None

    def batch_download_tick_data(self, stock_codes, trade_date=None, save_excel=True, save_db=True):
        """批量下载分笔数据"""
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y%m%d')

        results = {}
        for stock_code in stock_codes:
            try:
                result = self.download_and_save_tick_data(stock_code, trade_date, save_excel, save_db)
                results[stock_code] = result
                logger.info(f"股票 {stock_code} 分笔数据处理完成")
            except Exception as e:
                logger.error(f"股票 {stock_code} 分笔数据处理失败: {e}")
                results[stock_code] = None

        return results

    def get_trade_statistics(self, tick_data):
        """计算分笔数据统计信息"""
        if tick_data.empty:
            return {}

        try:
            stats = {
                'total_records': len(tick_data),
                'total_volume': tick_data['volume'].sum(),
                'total_amount': tick_data['amount'].sum(),
                'avg_price': tick_data['price'].mean(),
                'max_price': tick_data['price'].max(),
                'min_price': tick_data['price'].min(),
                'price_range': tick_data['price'].max() - tick_data['price'].min(),
                'buy_volume': tick_data[tick_data['trade_type'] == '买盘']['volume'].sum(),
                'sell_volume': tick_data[tick_data['trade_type'] == '卖盘']['volume'].sum(),
                'neutral_volume': tick_data[tick_data['trade_type'] == '中性盘']['volume'].sum()
            }

            # 计算买卖比例
            if stats['total_volume'] > 0:
                stats['buy_ratio'] = stats['buy_volume'] / stats['total_volume']
                stats['sell_ratio'] = stats['sell_volume'] / stats['total_volume']
                stats['neutral_ratio'] = stats['neutral_volume'] / stats['total_volume']

            return stats

        except Exception as e:
            logger.error(f"计算分笔数据统计信息失败: {e}")
            return {}


# 创建全局实例
tick_data = TickData()