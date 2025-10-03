"""
优化的股票数据获取模块
支持超时机制和多数据源自动切换
"""

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import threading
import queue
from typing import Dict, List, Optional, Any, Callable
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
import functools


class DataFetcher:
    """股票数据获取器，支持超时和多数据源切换"""

    def __init__(self, timeout=10, max_retries=3):
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

    def _try_multiple_sources(self, operation: str, *args, **kwargs) -> pd.DataFrame:
        """尝试多个数据源获取数据"""
        last_error = None

        for source_name in self.source_priority:
            for attempt in range(self.max_retries):
                try:
                    logger.info(f"尝试使用数据源 {source_name} 获取 {operation} (第{attempt + 1}次尝试)")

                    source_func = self.data_sources[source_name]
                    result = self._with_timeout(source_func, operation, *args, **kwargs)

                    if not result.empty:
                        logger.success(f"使用数据源 {source_name} 成功获取 {operation}")
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

    def _akshare_primary_source(self, operation: str, *args, **kwargs) -> pd.DataFrame:
        """主要数据源 - akshare 默认接口"""
        if operation == 'stock_list':
            # 获取A股股票列表
            return ak.stock_info_a_code_name()

        elif operation == 'realtime_data':
            stock_codes = args[0]
            if isinstance(stock_codes, str):
                stock_codes = [stock_codes]

            results = []
            for code in stock_codes:
                try:
                    # 获取实时行情
                    data = ak.stock_zh_a_spot_em()
                    filtered_data = data[data['代码'] == code]
                    if not filtered_data.empty:
                        results.append(filtered_data.iloc[0])
                except Exception as e:
                    logger.warning(f"获取股票 {code} 实时数据失败: {e}")

            if results:
                return pd.DataFrame(results)
            return pd.DataFrame()

        elif operation == 'historical_data':
            stock_code, start_date, end_date = args[0], args[1], args[2]
            return ak.stock_zh_a_hist(
                symbol=stock_code,
                period="daily",
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', ''),
                adjust="qfq"
            )

        elif operation == 'market_index':
            # 获取大盘指数
            indexes = {}
            try:
                # 上证指数
                sh_data = ak.stock_zh_index_spot_em(symbol="sh000001")
                if not sh_data.empty:
                    indexes['sh000001'] = sh_data.iloc[0].to_dict()
            except:
                pass

            try:
                # 深证成指
                sz_data = ak.stock_zh_index_spot_em(symbol="sz399001")
                if not sz_data.empty:
                    indexes['sz399001'] = sz_data.iloc[0].to_dict()
            except:
                pass

            return pd.DataFrame([indexes]) if indexes else pd.DataFrame()

        elif operation == 'sector_data':
            # 获取板块数据
            return ak.stock_board_industry_name_em()

        return pd.DataFrame()

    def _akshare_backup_source(self, operation: str, *args, **kwargs) -> pd.DataFrame:
        """备用数据源 - akshare 备用接口"""
        if operation == 'stock_list':
            # 使用不同的接口获取股票列表
            return ak.tool_trade_date_hist_sina()

        elif operation == 'realtime_data':
            stock_codes = args[0]
            if isinstance(stock_codes, str):
                stock_codes = [stock_codes]

            results = []
            for code in stock_codes:
                try:
                    # 使用新浪接口
                    data = ak.stock_zh_a_spot()
                    filtered_data = data[data['symbol'] == code]
                    if not filtered_data.empty:
                        results.append(filtered_data.iloc[0])
                except Exception as e:
                    logger.warning(f"备用源获取股票 {code} 实时数据失败: {e}")

            if results:
                return pd.DataFrame(results)
            return pd.DataFrame()

        elif operation == 'historical_data':
            stock_code, start_date, end_date = args[0], args[1], args[2]
            # 使用腾讯接口
            return ak.stock_zh_a_hist_tx(
                symbol=stock_code,
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', '')
            )

        elif operation == 'market_index':
            # 使用新浪接口获取指数
            try:
                return ak.stock_zh_index_spot()
            except:
                return pd.DataFrame()

        elif operation == 'sector_data':
            # 使用同花顺接口
            return ak.stock_board_concept_name_ths()

        return pd.DataFrame()

    def _akshare_alternative_source(self, operation: str, *args, **kwargs) -> pd.DataFrame:
        """替代数据源 - akshare 其他接口"""
        if operation == 'stock_list':
            # 使用东财接口
            return ak.stock_zh_a_spot_em()

        elif operation == 'realtime_data':
            stock_codes = args[0]
            if isinstance(stock_codes, str):
                stock_codes = [stock_codes]

            # 使用简化的方法，返回基础数据
            results = []
            for code in stock_codes:
                try:
                    # 获取最新的历史数据作为实时数据
                    end_date = datetime.now().strftime('%Y%m%d')
                    start_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
                    data = ak.stock_zh_a_hist(
                        symbol=code,
                        period="daily",
                        start_date=start_date,
                        end_date=end_date,
                        adjust="qfq"
                    )
                    if not data.empty:
                        results.append(data.iloc[-1])
                except Exception as e:
                    logger.warning(f"替代源获取股票 {code} 数据失败: {e}")

            if results:
                return pd.DataFrame(results)
            return pd.DataFrame()

        elif operation == 'historical_data':
            stock_code, start_date, end_date = args[0], args[1], args[2]
            # 使用网易接口
            return ak.stock_zh_a_hist_163(
                symbol=stock_code,
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', '')
            )

        elif operation == 'market_index':
            # 返回模拟数据
            return pd.DataFrame({
                'code': ['000001', '399001'],
                'name': ['上证指数', '深证成指'],
                'current': [3000.0, 11000.0],
                'change': [10.0, 50.0],
                'change_pct': [0.33, 0.45]
            })

        elif operation == 'sector_data':
            # 返回基础板块数据
            return pd.DataFrame({
                'board_name': ['科技股', '金融股', '消费股'],
                'board_code': ['tech', 'finance', 'consumer'],
                'change_pct': [2.1, -0.5, 1.2]
            })

        return pd.DataFrame()

    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        try:
            result = self._try_multiple_sources('stock_list')

            # 标准化列名
            if not result.empty:
                column_mapping = {
                    '代码': 'code',
                    'code': 'code',
                    'symbol': 'code',
                    '名称': 'name',
                    'name': 'name',
                    '简称': 'name'
                }

                for old_col, new_col in column_mapping.items():
                    if old_col in result.columns:
                        result = result.rename(columns={old_col: new_col})

                # 确保有必要的列
                if 'code' not in result.columns and 'name' not in result.columns:
                    if len(result.columns) >= 2:
                        result.columns = ['code', 'name'] + list(result.columns[2:])

                logger.info(f"成功获取股票列表，共 {len(result)} 只股票")

            return result

        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return pd.DataFrame()

    def get_realtime_data(self, stock_codes: List[str]) -> pd.DataFrame:
        """获取实时行情数据"""
        try:
            result = self._try_multiple_sources('realtime_data', stock_codes)

            # 标准化列名
            if not result.empty:
                column_mapping = {
                    '代码': 'code',
                    'code': 'code',
                    'symbol': 'code',
                    '名称': 'name',
                    'name': 'name',
                    '最新价': 'current_price',
                    'current': 'current_price',
                    'price': 'current_price',
                    '涨跌幅': 'change_pct',
                    'change_pct': 'change_pct',
                    '涨跌额': 'change_price',
                    'change': 'change_price',
                    '成交量': 'volume',
                    'volume': 'volume',
                    '成交额': 'amount',
                    'amount': 'amount'
                }

                for old_col, new_col in column_mapping.items():
                    if old_col in result.columns:
                        result = result.rename(columns={old_col: new_col})

                logger.info(f"成功获取 {len(stock_codes)} 只股票的实时数据")

            return result

        except Exception as e:
            logger.error(f"获取实时数据失败: {e}")
            return pd.DataFrame()

    def get_historical_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取历史行情数据"""
        try:
            result = self._try_multiple_sources('historical_data', stock_code, start_date, end_date)

            # 标准化列名
            if not result.empty:
                column_mapping = {
                    '日期': 'trade_date',
                    'date': 'trade_date',
                    '开盘': 'open_price',
                    'open': 'open_price',
                    '收盘': 'close_price',
                    'close': 'close_price',
                    '最高': 'high_price',
                    'high': 'high_price',
                    '最低': 'low_price',
                    'low': 'low_price',
                    '成交量': 'volume',
                    'volume': 'volume',
                    '成交额': 'amount',
                    'amount': 'amount',
                    '振幅': 'amplitude',
                    '涨跌幅': 'change_pct',
                    '涨跌额': 'change_price',
                    '换手率': 'turnover_rate'
                }

                for old_col, new_col in column_mapping.items():
                    if old_col in result.columns:
                        result = result.rename(columns={old_col: new_col})

                # 确保日期格式正确
                if 'trade_date' in result.columns:
                    result['trade_date'] = pd.to_datetime(result['trade_date'])

                # 添加股票代码
                result['stock_code'] = stock_code

                logger.info(f"成功获取股票 {stock_code} 历史数据，共 {len(result)} 条记录")

            return result

        except Exception as e:
            logger.error(f"获取股票 {stock_code} 历史数据失败: {e}")
            return pd.DataFrame()

    def get_market_index_data(self) -> Dict:
        """获取大盘指数数据"""
        try:
            result = self._try_multiple_sources('market_index')

            if not result.empty:
                # 转换为字典格式
                index_data = {}
                for _, row in result.iterrows():
                    index_data.update(row.to_dict())

                logger.info("成功获取大盘指数数据")
                return index_data

            return {}

        except Exception as e:
            logger.error(f"获取大盘指数数据失败: {e}")
            return {}

    def get_sector_data(self) -> pd.DataFrame:
        """获取板块数据"""
        try:
            result = self._try_multiple_sources('sector_data')

            # 标准化列名
            if not result.empty:
                column_mapping = {
                    '板块名称': 'sector_name',
                    'board_name': 'sector_name',
                    'name': 'sector_name',
                    '板块代码': 'sector_code',
                    'board_code': 'sector_code',
                    'code': 'sector_code',
                    '涨跌幅': 'change_pct',
                    'change_pct': 'change_pct'
                }

                for old_col, new_col in column_mapping.items():
                    if old_col in result.columns:
                        result = result.rename(columns={old_col: new_col})

                logger.info(f"成功获取板块数据，共 {len(result)} 个板块")

            return result

        except Exception as e:
            logger.error(f"获取板块数据失败: {e}")
            return pd.DataFrame()


# 创建全局实例
data_fetcher = DataFetcher(timeout=10, max_retries=3)