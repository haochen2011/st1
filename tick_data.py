"""
分笔数据管理模块
负责股票分笔数据的获取、存储和管理
优化版本：支持超时机制和多数据源自动切换
"""

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import os
import time
import threading
import queue
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable
from loguru import logger
from database import db_manager
from config import config


class TickData:
    """分笔数据管理类"""

    def __init__(self, timeout=10, max_retries=3):
        self.data_path = config.get_data_path('tick_data')
        self.timeout = timeout  # 超时时间（秒）
        self.max_retries = max_retries  # 最大重试次数

        # 定义多个数据源的获取方法
        self.data_sources = {
            'akshare_tx_primary': self._akshare_tx_primary_source,
            'akshare_tx_backup': self._akshare_tx_backup_source,
            'akshare_alternative': self._akshare_alternative_source
        }

        # 数据源优先级
        self.source_priority = ['akshare_tx_primary', 'akshare_tx_backup', 'akshare_alternative']

    def _with_timeout(self, func: Callable, *args, **kwargs) -> Any:
        """为函数添加超时机制"""
        result_queue = queue.Queue()
        exception_queue = queue.Queue()

        def target():
            try:
                result = func(*args, **kwargs)
                result_queue.put(result)
            except Exception as e:
                exception_queue.put(e)

        thread = threading.Thread(target=target)
        thread.daemon = True
        thread.start()
        thread.join(timeout=self.timeout)

        if thread.is_alive():
            logger.warning(f"函数 {func.__name__} 执行超时 ({self.timeout}秒)")
            raise TimeoutError(f"函数执行超时: {self.timeout}秒")

        if not exception_queue.empty():
            raise exception_queue.get()

        if not result_queue.empty():
            return result_queue.get()

        raise Exception("函数执行失败，无返回结果")

    def _try_multiple_sources(self, stock_code: str, trade_date: str) -> pd.DataFrame:
        """尝试多个数据源获取分笔数据"""
        last_error = None

        for source_name in self.source_priority:
            for attempt in range(self.max_retries):
                try:
                    logger.info(f"尝试使用数据源 {source_name} 获取股票 {stock_code} {trade_date} 分笔数据 (第{attempt+1}次尝试)")

                    source_func = self.data_sources[source_name]
                    result = self._with_timeout(source_func, stock_code, trade_date)

                    if not result.empty:
                        logger.success(f"使用数据源 {source_name} 成功获取股票 {stock_code} {trade_date} 分笔数据")
                        return result
                    else:
                        logger.warning(f"数据源 {source_name} 返回空数据")

                except TimeoutError as e:
                    logger.warning(f"数据源 {source_name} 超时: {e}")
                    last_error = e
                    time.sleep(1)  # 短暂延迟后重试

                except Exception as e:
                    logger.error(f"数据源 {source_name} 错误: {e}")
                    last_error = e
                    if attempt < self.max_retries - 1:
                        time.sleep(2)  # 延迟后重试

        logger.error(f"所有数据源均失败，最后错误: {last_error}")
        return pd.DataFrame()  # 分笔数据失败时返回空DataFrame而不是抛出异常

    def _akshare_tx_primary_source(self, stock_code: str, trade_date: str) -> pd.DataFrame:
        """主要数据源 - akshare 腾讯分笔接口"""
        tick_data = ak.stock_zh_a_tick_tx_js(symbol=stock_code)
        if not tick_data.empty:
            tick_data = self._standardize_columns(tick_data, stock_code, trade_date)
        return tick_data

    def _akshare_tx_backup_source(self, stock_code: str, trade_date: str) -> pd.DataFrame:
        """备用数据源 - 带市场前缀的腾讯接口"""
        # 根据股票代码添加市场前缀
        prefixed_code = f"sz{stock_code}" if not stock_code.startswith('6') else f"sh{stock_code}"
        tick_data = ak.stock_zh_a_tick_tx_js(symbol=prefixed_code)
        if not tick_data.empty:
            tick_data = self._standardize_columns(tick_data, stock_code, trade_date)
        return tick_data

    def _akshare_alternative_source(self, stock_code: str, trade_date: str) -> pd.DataFrame:
        """替代数据源 - 其他分笔数据接口或模拟数据"""
        try:
            # 尝试使用实时数据作为分笔数据的替代
            realtime_data = ak.stock_zh_a_spot_em()
            filtered_data = realtime_data[realtime_data['代码'] == stock_code]

            if not filtered_data.empty:
                # 将实时数据转换为分笔数据格式
                row = filtered_data.iloc[0]
                tick_data = pd.DataFrame({
                    'trade_time': [datetime.now().strftime('%H:%M:%S')],
                    'price': [row.get('最新价', 0)],
                    'price_change': [row.get('涨跌额', 0)],
                    'volume': [row.get('成交量', 0)],
                    'amount': [row.get('成交额', 0)],
                    'trade_type': ['实时数据']
                })

                tick_data = self._standardize_columns(tick_data, stock_code, trade_date)
                return tick_data

            return pd.DataFrame()

        except Exception as e:
            logger.warning(f"替代数据源失败: {e}")
            return pd.DataFrame()

    def get_tick_data(self, stock_code, trade_date=None):
        """获取股票分笔数据（支持超时和多数据源切换）"""
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y%m%d')
        elif isinstance(trade_date, (date, datetime)):
            trade_date = trade_date.strftime('%Y%m%d')

        try:
            # 使用多数据源切换机制
            tick_data = self._try_multiple_sources(stock_code, trade_date)

            if not tick_data.empty:
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
        """保存分笔数据到数据库（按日期分表）"""
        if tick_data.empty:
            logger.warning("分笔数据为空，跳过保存")
            return

        try:
            # 选择需要的列
            columns = ['stock_code', 'trade_time', 'price', 'price_change', 'volume', 'amount', 'trade_type',
                       'trade_date']
            db_data = tick_data[columns].copy()

            # 按交易日期分组保存到不同的表
            for trade_date, group_data in db_data.groupby('trade_date'):
                # 使用新的动态表插入方法
                success = db_manager.insert_dataframe_to_dynamic_table(
                    group_data, 'tick', trade_date, if_exists='append'
                )
                if success:
                    logger.info(f"成功保存 {len(group_data)} 条分笔数据到表 {db_manager.get_tick_table_name(trade_date)}")
                else:
                    logger.error(f"保存日期 {trade_date} 的分笔数据失败")

        except Exception as e:
            logger.error(f"保存分笔数据到数据库失败: {e}")

    def get_tick_data_from_db(self, stock_code, start_date=None, end_date=None):
        """从数据库获取分笔数据（从按日期分表中查询）"""
        try:
            from datetime import datetime, timedelta

            # 确定查询的日期范围
            if start_date is None:
                start_date = datetime.now().date()
            elif isinstance(start_date, str):
                start_date = datetime.strptime(start_date, '%Y-%m-%d').date()

            if end_date is None:
                end_date = start_date
            elif isinstance(end_date, str):
                end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

            all_data = []
            current_date = start_date

            # 遍历日期范围，从各个分表中查询数据
            while current_date <= end_date:
                table_name = db_manager.get_tick_table_name(current_date)

                # 检查表是否存在
                check_sql = f"""
                SELECT COUNT(*) as count FROM information_schema.tables
                WHERE table_schema = DATABASE() AND table_name = '{table_name}'
                """
                table_exists = db_manager.query_to_dataframe(check_sql)

                if not table_exists.empty and table_exists.iloc[0]['count'] > 0:
                    sql = f"SELECT * FROM {table_name} WHERE stock_code = :stock_code ORDER BY trade_time"
                    params = {'stock_code': stock_code}

                    daily_data = db_manager.query_to_dataframe(sql, params)
                    if not daily_data.empty:
                        all_data.append(daily_data)

                current_date += timedelta(days=1)

            # 合并所有数据
            if all_data:
                tick_data = pd.concat(all_data, ignore_index=True)
                tick_data = tick_data.sort_values('trade_time')
                logger.info(f"从数据库获取股票 {stock_code} 分笔数据成功，共 {len(tick_data)} 条")
                return tick_data
            else:
                logger.info(f"未找到股票 {stock_code} 在指定日期范围的分笔数据")
                return pd.DataFrame()

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


# 创建全局实例，使用配置中的超时设置
try:
    from config import config
    timeout = config.get_data_fetch_timeout()
    max_retries = config.get_max_retries()
    tick_data = TickData(timeout=timeout, max_retries=max_retries)
except:
    # 如果配置读取失败，使用默认值
    tick_data = TickData(timeout=10, max_retries=3)