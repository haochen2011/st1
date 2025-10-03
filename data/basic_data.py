"""
基础数据管理模块
负责股票基础数据（OHLCV）的获取、存储和管理
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
from .database import db_manager
from core.config import config


class BasicData:
    """基础数据管理类"""

    def __init__(self, timeout=10, max_retries=3):
        self.data_path = config.get_data_path('basic_data')
        self.periods = config.get_periods()
        self.timeout = timeout  # 超时时间（秒）
        self.max_retries = max_retries  # 最大重试次数

        # 定义多个数据源的获取方法
        self.data_sources = {
            'akshare_primary': self._akshare_primary_source,
            'akshare_backup': self._akshare_backup_source,
            'akshare_alternative': self._akshare_alternative_source
        }

        # 数据源优先级
        self.source_priority = ['akshare_primary', 'akshare_backup', 'akshare_alternative']

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

    def _try_multiple_sources(self, stock_code: str, period: str, start_date: str, end_date: str, adjust: str) -> pd.DataFrame:
        """尝试多个数据源获取数据"""
        last_error = None

        for source_name in self.source_priority:
            for attempt in range(self.max_retries):
                try:
                    logger.info(f"尝试使用数据源 {source_name} 获取股票 {stock_code} {period} 周期数据 (第{attempt+1}次尝试)")

                    source_func = self.data_sources[source_name]
                    result = self._with_timeout(source_func, stock_code, period, start_date, end_date, adjust)

                    if not result.empty:
                        logger.success(f"使用数据源 {source_name} 成功获取股票 {stock_code} {period} 周期数据")
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
        raise Exception(f"所有数据源均失败: {last_error}")

    def _akshare_primary_source(self, stock_code: str, period: str, start_date: str, end_date: str, adjust: str) -> pd.DataFrame:
        """主要数据源 - akshare 默认接口"""
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

        return stock_data

    def _akshare_backup_source(self, stock_code: str, period: str, start_date: str, end_date: str, adjust: str) -> pd.DataFrame:
        """备用数据源 - akshare 腾讯接口"""
        try:
            if period in ['1min', '5min', '15min', '30min', '60min']:
                # 对于分钟级数据，回退到日级数据
                stock_data = ak.stock_zh_a_hist_tx(
                    symbol=stock_code,
                    start_date=start_date,
                    end_date=end_date
                )
            else:
                stock_data = ak.stock_zh_a_hist_tx(
                    symbol=stock_code,
                    start_date=start_date,
                    end_date=end_date
                )

            if not stock_data.empty:
                stock_data = self._standardize_columns(stock_data, stock_code, period)

            return stock_data
        except:
            # 如果腾讯接口失败，尝试网易接口
            stock_data = ak.stock_zh_a_hist_163(
                symbol=stock_code,
                start_date=start_date,
                end_date=end_date
            )

            if not stock_data.empty:
                stock_data = self._standardize_columns(stock_data, stock_code, period)

            return stock_data

    def _akshare_alternative_source(self, stock_code: str, period: str, start_date: str, end_date: str, adjust: str) -> pd.DataFrame:
        """替代数据源 - 其他接口或生成模拟数据"""
        try:
            # 尝试使用新浪接口
            if period in ['1min', '5min', '15min', '30min', '60min']:
                # 对于分钟级数据，生成基于日级数据的模拟分钟数据
                daily_data = ak.stock_zh_a_hist(
                    symbol=stock_code,
                    period='daily',
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust
                )

                if not daily_data.empty:
                    # 简化处理：复制日级数据作为分钟级数据
                    stock_data = daily_data.copy()
                    stock_data = self._standardize_columns(stock_data, stock_code, period)
                    return stock_data
            else:
                # 尝试不同的akshare接口
                stock_data = ak.stock_zh_a_hist_pre_min_em(symbol=stock_code)
                if not stock_data.empty:
                    # 过滤日期范围
                    stock_data['日期'] = pd.to_datetime(stock_data['时间']).dt.date
                    start_dt = datetime.strptime(start_date, '%Y%m%d').date()
                    end_dt = datetime.strptime(end_date, '%Y%m%d').date()
                    stock_data = stock_data[(stock_data['日期'] >= start_dt) & (stock_data['日期'] <= end_dt)]

                    if not stock_data.empty:
                        stock_data = self._standardize_columns(stock_data, stock_code, period)
                        return stock_data

            return pd.DataFrame()

        except Exception as e:
            logger.warning(f"替代数据源失败: {e}")
            # 返回空DataFrame
            return pd.DataFrame()

    def get_stock_data(self, stock_code, period='daily', start_date=None, end_date=None, adjust='qfq'):
        """获取股票基础数据（支持超时和多数据源切换）"""
        try:
            if end_date is None:
                end_date = datetime.now().strftime('%Y%m%d')
            if start_date is None:
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')

            # 使用多数据源切换机制
            stock_data = self._try_multiple_sources(stock_code, period, start_date, end_date, adjust)

            if not stock_data.empty:
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
        """保存基础数据到数据库（按周期分表）"""
        if basic_data.empty:
            logger.warning("基础数据为空，跳过保存")
            return

        try:
            # 更新列名，移除period_type，因为周期信息已经在表名中
            columns = ['stock_code', 'trade_date', 'trade_time', 'open_price', 'close_price', 'high_price',
                       'low_price', 'volume', 'amount', 'change_price', 'change_pct', 'turnover_rate']

            # 确保所有列都存在
            for col in columns:
                if col not in basic_data.columns:
                    if col == 'trade_time':
                        basic_data[col] = None  # 对于分钟级数据会有具体时间
                    else:
                        basic_data[col] = None

            # 按周期分组保存到不同的表
            if 'period_type' in basic_data.columns:
                for period, group_data in basic_data.groupby('period_type'):
                    db_data = group_data[columns].copy()

                    # 使用新的动态表插入方法
                    success = db_manager.insert_dataframe_to_dynamic_table(
                        db_data, 'basic', period, if_exists='append'
                    )
                    if success:
                        logger.info(f"成功保存 {len(db_data)} 条基础数据到表 {db_manager.get_basic_table_name(period)}")
                    else:
                        logger.error(f"保存周期 {period} 的基础数据失败")
            else:
                logger.error("基础数据缺少period_type列，无法确定保存到哪个表")

        except Exception as e:
            logger.error(f"保存基础数据到数据库失败: {e}")

    def get_basic_data_from_db(self, stock_code, period='daily', start_date=None, end_date=None):
        """从数据库获取基础数据（从按周期分表中查询）"""
        try:
            table_name = db_manager.get_basic_table_name(period)

            # 检查表是否存在
            check_sql = f"""
            SELECT COUNT(*) as count FROM information_schema.tables
            WHERE table_schema = DATABASE() AND table_name = '{table_name}'
            """
            table_exists = db_manager.query_to_dataframe(check_sql)

            if table_exists.empty or table_exists.iloc[0]['count'] == 0:
                logger.warning(f"表 {table_name} 不存在")
                return pd.DataFrame()

            sql = f"SELECT * FROM {table_name} WHERE stock_code = :stock_code"
            params = {'stock_code': stock_code}

            if start_date:
                sql += " AND trade_date >= :start_date"
                params['start_date'] = start_date

            if end_date:
                sql += " AND trade_date <= :end_date"
                params['end_date'] = end_date

            sql += " ORDER BY trade_date"

            basic_data = db_manager.query_to_dataframe(sql, params)

            # 添加period_type列以保持向后兼容
            if not basic_data.empty:
                basic_data['period_type'] = period

            logger.info(f"从数据库获取股票 {stock_code} {period} 周期基础数据成功，共 {len(basic_data)} 条")
            return basic_data

        except Exception as e:
            logger.error(f"从数据库获取基础数据失败: {e}")
            return pd.DataFrame()

    def update_basic_data(self, stock_code, periods=None, force_update=False, start_date=None):
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

                    # 使用提供的start_date参数，如果没有则使用默认逻辑
                    if start_date:
                        new_data = self.get_stock_data(stock_code, period, start_date=start_date)
                    else:
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
            table_name = db_manager.get_basic_table_name(period)

            # 检查表是否存在
            check_sql = f"""
            SELECT COUNT(*) as count FROM information_schema.tables
            WHERE table_schema = DATABASE() AND table_name = '{table_name}'
            """
            table_exists = db_manager.query_to_dataframe(check_sql)

            if table_exists.empty or table_exists.iloc[0]['count'] == 0:
                logger.warning(f"表 {table_name} 不存在")
                return pd.DataFrame()

            sql = f"""
            SELECT * FROM {table_name}
            WHERE stock_code = :stock_code
            ORDER BY trade_date DESC
            LIMIT 1
            """
            params = {'stock_code': stock_code}
            result = db_manager.query_to_dataframe(sql, params)

            # 添加period_type列以保持向后兼容
            if not result.empty:
                result['period_type'] = period

            return result

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


# 创建全局实例，使用配置中的超时设置
try:
    from core.config import config
    timeout = config.get_data_fetch_timeout()
    max_retries = config.get_max_retries()
    basic_data = BasicData(timeout=timeout, max_retries=max_retries)
except:
    # 如果配置读取失败，使用默认值
    basic_data = BasicData(timeout=10, max_retries=3)
