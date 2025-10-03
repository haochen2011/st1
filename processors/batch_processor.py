"""
批量数据处理器
提供一键获取所有A股股票数据的功能
优化的批量处理，支持并发、进度显示、错误重试
"""

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import asyncio
import aiohttp
import concurrent.futures
from threading import Lock
import time
import os
from pathlib import Path
from loguru import logger
from tqdm import tqdm
import json
from typing import List, Dict, Optional, Union
from ..data.database import db_manager
from core.config import config
from utils.stock_info import stock_info
from data.tick_data import tick_data
from data.basic_data import basic_data
import schedule


class BatchProcessor:
    """批量数据处理器"""

    def __init__(self, max_workers=10, retry_times=3, batch_size=50):
        self.max_workers = max_workers
        self.retry_times = retry_times
        self.batch_size = batch_size
        self.failed_tasks = []
        self.success_count = 0
        self.failed_count = 0
        self.lock = Lock()

        # 创建必要的目录
        self.output_dir = Path('./batch_output')
        self.output_dir.mkdir(exist_ok=True)

        self.log_dir = Path('./batch_logs')
        self.log_dir.mkdir(exist_ok=True)

    def get_all_stock_codes(self) -> List[Dict]:
        """获取所有A股股票代码列表，优化版本"""
        try:
            logger.info("开始获取所有A股股票列表...")

            # 方法1：直接使用akshare获取所有A股现货数据
            try:
                all_stocks_df = ak.stock_zh_a_spot()

                if not all_stocks_df.empty:
                    logger.info(f"股票数据列名: {list(all_stocks_df.columns)}")

                    # 截取代码后6位并新增列存储
                    all_stocks_df['代码后6位'] = all_stocks_df['代码'].str.slice(-6)

                    # 筛选后6位以0、3、6开头的股票
                    stock_filter = all_stocks_df['代码后6位'].str.match(r'^[036]')
                    filtered_stocks = all_stocks_df[stock_filter].copy()

                    # 将原始"代码"列替换为截取后的6位数字
                    filtered_stocks['代码'] = filtered_stocks['代码后6位']

                    # 删除临时的"代码后6位"列（可选）
                    filtered_stocks = filtered_stocks.drop(columns=['代码后6位'])
                    filtered_stocks['market'] = np.where(
                        filtered_stocks['代码'].str.startswith('6')
                    )
                    # 直接转为字典列表
                    stock_list = filtered_stocks[['代码', '名称', 'market']].rename(
                        columns={'代码': 'stock_code', '名称': 'stock_name'}
                    ).to_dict('records')

                    if stock_list:
                        logger.info(f"获取股票列表成功，共 {len(stock_list)} 只股票")

                        self._save_stock_list(stock_list)
                        return stock_list

            except Exception as e1:
                logger.warning(f"方法1失败: {e1}")

            # 方法2：尝试获取上海和深圳股票分别获取
            try:
                logger.info("尝试分别获取上海和深圳股票...")
                stock_list = []

                # 获取上海A股
                try:
                    sh_stocks = ak.stock_info_sh_name_code()
                    if not sh_stocks.empty:
                        for _, row in sh_stocks.iterrows():
                            # 检查不同的可能列名
                            code_col = None
                            name_col = None

                            for col in sh_stocks.columns:
                                if 'CODE' in col.upper() or '代码' in col:
                                    code_col = col
                                if 'NAME' in col.upper() or 'ABBR' in col.upper() or '名称' in col:
                                    name_col = col

                            if code_col and name_col:
                                stock_list.append({
                                    'stock_code': row[code_col],
                                    'stock_name': row[name_col],
                                    'market': 'sh'
                                })
                except Exception as e_sh:
                    logger.warning(f"获取上海股票失败: {e_sh}")

                # 获取深圳A股（从现货数据中筛选）
                try:
                    sz_stocks = ak.stock_zh_a_spot_em()
                    if not sz_stocks.empty:
                        sz_filter = sz_stocks['代码'].str.match(r'^[03]')
                        sz_filtered = sz_stocks[sz_filter]

                        for _, row in sz_filtered.iterrows():
                            stock_list.append({
                                'stock_code': row['代码'],
                                'stock_name': row['名称'],
                                'market': 'sz'
                            })
                except Exception as e_sz:
                    logger.warning(f"获取深圳股票失败: {e_sz}")

                if stock_list:
                    logger.info(f"分别获取股票列表成功，共 {len(stock_list)} 只股票")
                    self._save_stock_list(stock_list)
                    return stock_list

            except Exception as e2:
                logger.warning(f"方法2失败: {e2}")

            # 如果以上方法都失败，使用备用股票列表
            logger.info("使用备用股票列表...")
            return self._get_stock_codes_fallback()

        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return self._get_stock_codes_fallback()

    def _get_stock_codes_fallback(self) -> List[Dict]:
        """备用方法获取股票代码"""
        sample_stocks = [
            # 深圳主板
            {'stock_code': '000001', 'stock_name': '平安银行', 'market': 'sz'},
            {'stock_code': '000002', 'stock_name': '万科A', 'market': 'sz'},
            {'stock_code': '000858', 'stock_name': '五粮液', 'market': 'sz'},
            {'stock_code': '002415', 'stock_name': '海康威视', 'market': 'sz'},
            {'stock_code': '002594', 'stock_name': 'BYD', 'market': 'sz'},

            # 创业板
            {'stock_code': '300015', 'stock_name': '爱尔眼科', 'market': 'sz'},
            {'stock_code': '300750', 'stock_name': '宁德时代', 'market': 'sz'},
            {'stock_code': '300059', 'stock_name': '东方财富', 'market': 'sz'},

            # 上海主板
            {'stock_code': '600000', 'stock_name': '浦发银行', 'market': 'sh'},
            {'stock_code': '600036', 'stock_name': '招商银行', 'market': 'sh'},
            {'stock_code': '600519', 'stock_name': '贵州茅台', 'market': 'sh'},
            {'stock_code': '600887', 'stock_name': '伊利股份', 'market': 'sh'},
            {'stock_code': '601318', 'stock_name': '中国平安', 'market': 'sh'},
            {'stock_code': '601398', 'stock_name': '工商银行', 'market': 'sh'},
            {'stock_code': '601888', 'stock_name': '中国中免', 'market': 'sh'},
        ]

        logger.info(f"使用备用股票列表，共 {len(sample_stocks)} 只股票")
        self._save_stock_list(sample_stocks)
        return sample_stocks

    def _save_stock_list(self, stock_list: List[Dict]):
        """保存股票列表到文件"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = self.output_dir / f"stock_list_{timestamp}.json"

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(stock_list, f, ensure_ascii=False, indent=2)

            logger.info(f"股票列表已保存到: {filename}")

        except Exception as e:
            logger.error(f"保存股票列表失败: {e}")

    def batch_update_stock_info(self, stock_list: Optional[List[Dict]] = None) -> Dict:
        """批量更新股票基本信息到数据库"""
        if stock_list is None:
            stock_list = self.get_all_stock_codes()

        if not stock_list:
            return {'success': 0, 'failed': 0, 'errors': []}

        logger.info(f"开始批量更新 {len(stock_list)} 只股票的基本信息...")

        results = {'success': 0, 'failed': 0, 'errors': []}

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 创建进度条
            with tqdm(total=len(stock_list), desc="更新股票信息") as pbar:
                # 提交任务
                future_to_stock = {
                    executor.submit(self._update_single_stock_info, stock): stock
                    for stock in stock_list
                }

                # 处理结果
                for future in concurrent.futures.as_completed(future_to_stock):
                    stock = future_to_stock[future]
                    try:
                        success = future.result()
                        if success:
                            results['success'] += 1
                        else:
                            results['failed'] += 1
                            results['errors'].append(f"股票 {stock['stock_code']} 更新失败")
                    except Exception as e:
                        results['failed'] += 1
                        error_msg = f"股票 {stock['stock_code']} 更新异常: {str(e)}"
                        results['errors'].append(error_msg)
                        logger.error(error_msg)

                    pbar.update(1)

        logger.info(f"股票信息更新完成: 成功 {results['success']}, 失败 {results['failed']}")
        return results

    def _update_single_stock_info(self, stock: Dict) -> bool:
        """更新单个股票信息"""
        try:
            stock_code = stock['stock_code']

            # 获取股票详细信息
            info = stock_info.get_stock_basic_info(stock_code)
            if info:
                # 合并信息
                info.update(stock)

                # 保存到数据库
                success = stock_info.save_stock_info_to_db(info)
                return success
            return False

        except Exception as e:
            logger.error(f"更新股票 {stock.get('stock_code')} 信息失败: {e}")
            return False

    def batch_download_tick_data(self,
                                trade_date: Optional[Union[str, date]] = None,
                                stock_list: Optional[List[Dict]] = None) -> Dict:
        """批量下载所有股票的分笔数据"""

        if trade_date is None:
            # 默认获取昨天的数据（避免今天还没收盘）
            trade_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        elif isinstance(trade_date, date):
            trade_date = trade_date.strftime('%Y%m%d')

        if stock_list is None:
            stock_list = self.get_all_stock_codes()

        if not stock_list:
            return {'success': 0, 'failed': 0, 'errors': []}

        logger.info(f"开始批量下载 {len(stock_list)} 只股票 {trade_date} 的分笔数据...")

        results = {'success': 0, 'failed': 0, 'errors': [], 'trade_date': trade_date}

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            with tqdm(total=len(stock_list), desc=f"下载分笔数据 {trade_date}") as pbar:
                future_to_stock = {
                    executor.submit(self._download_single_tick_data, stock['stock_code'], trade_date): stock
                    for stock in stock_list
                }

                for future in concurrent.futures.as_completed(future_to_stock):
                    stock = future_to_stock[future]
                    try:
                        success = future.result()
                        if success:
                            results['success'] += 1
                        else:
                            results['failed'] += 1
                            results['errors'].append(f"股票 {stock['stock_code']} 分笔数据下载失败")
                    except Exception as e:
                        results['failed'] += 1
                        error_msg = f"股票 {stock['stock_code']} 分笔数据下载异常: {str(e)}"
                        results['errors'].append(error_msg)
                        logger.error(error_msg)

                    pbar.update(1)

        logger.info(f"分笔数据下载完成: 成功 {results['success']}, 失败 {results['failed']}")

        # 保存结果报告
        self._save_batch_report('tick_data', results)

        return results

    def _download_single_tick_data(self, stock_code: str, trade_date: str) -> bool:
        """下载单个股票的分笔数据"""
        try:
            # 重试机制
            for attempt in range(self.retry_times):
                try:
                    result = tick_data.download_and_save_tick_data(stock_code, trade_date)
                    if result.get('success', False):
                        return True

                    # 如果失败，等待一段时间再重试
                    if attempt < self.retry_times - 1:
                        time.sleep(1)

                except Exception as e:
                    if attempt < self.retry_times - 1:
                        time.sleep(1)
                        continue
                    else:
                        raise e

            return False

        except Exception as e:
            logger.error(f"下载股票 {stock_code} {trade_date} 分笔数据失败: {e}")
            return False

    def batch_download_basic_data(self,
                                 periods: Optional[List[str]] = None,
                                 stock_list: Optional[List[Dict]] = None,
                                 start_date: Optional[str] = None) -> Dict:
        """批量下载所有股票的基础数据（K线数据）"""

        if periods is None:
            periods = ['daily']  # 默认只下载日线

        if stock_list is None:
            stock_list = self.get_all_stock_codes()

        if not stock_list:
            return {'success': 0, 'failed': 0, 'errors': []}

        logger.info(f"开始批量下载 {len(stock_list)} 只股票的基础数据，周期: {periods}")

        results = {'success': 0, 'failed': 0, 'errors': [], 'periods': periods}

        # 对每个周期分别处理
        for period in periods:
            logger.info(f"正在处理周期: {period}")

            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                with tqdm(total=len(stock_list), desc=f"下载{period}数据") as pbar:
                    future_to_stock = {
                        executor.submit(self._download_single_basic_data, stock['stock_code'], period, start_date): stock
                        for stock in stock_list
                    }

                    for future in concurrent.futures.as_completed(future_to_stock):
                        stock = future_to_stock[future]
                        try:
                            success = future.result()
                            if success:
                                results['success'] += 1
                            else:
                                results['failed'] += 1
                                results['errors'].append(f"股票 {stock['stock_code']} {period} 数据下载失败")
                        except Exception as e:
                            results['failed'] += 1
                            error_msg = f"股票 {stock['stock_code']} {period} 数据下载异常: {str(e)}"
                            results['errors'].append(error_msg)
                            logger.error(error_msg)

                        pbar.update(1)

        logger.info(f"基础数据下载完成: 成功 {results['success']}, 失败 {results['failed']}")

        # 保存结果报告
        self._save_batch_report('basic_data', results)

        return results

    def _download_single_basic_data(self, stock_code: str, period: str, start_date: Optional[str] = None) -> bool:
        """下载单个股票的基础数据"""
        try:
            # 重试机制
            for attempt in range(self.retry_times):
                try:
                    result = basic_data.update_basic_data(stock_code, [period], start_date=start_date)
                    if result and period in result:
                        return True

                    if attempt < self.retry_times - 1:
                        time.sleep(1)

                except Exception as e:
                    if attempt < self.retry_times - 1:
                        time.sleep(1)
                        continue
                    else:
                        raise e

            return False

        except Exception as e:
            logger.error(f"下载股票 {stock_code} {period} 基础数据失败: {e}")
            return False

    def _save_batch_report(self, task_type: str, results: Dict):
        """保存批量处理报告"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = self.log_dir / f"{task_type}_batch_report_{timestamp}.json"

            # 添加时间戳和任务信息
            report = {
                'task_type': task_type,
                'timestamp': timestamp,
                'results': results
            }

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)

            logger.info(f"批量处理报告已保存到: {filename}")

        except Exception as e:
            logger.error(f"保存批量处理报告失败: {e}")

    def one_click_update_all(self,
                            include_tick: bool = True,
                            include_basic: bool = True,
                            periods: Optional[List[str]] = None,
                            trade_date: Optional[str] = None) -> Dict:
        """一键更新所有A股数据"""

        logger.info("=" * 60)
        logger.info("开始一键更新所有A股数据")
        logger.info("=" * 60)

        total_results = {
            'start_time': datetime.now().isoformat(),
            'stock_info': {},
            'tick_data': {},
            'basic_data': {},
            'total_time': 0
        }

        start_time = time.time()

        try:
            # 1. 获取所有股票列表
            logger.info("步骤1: 获取股票列表...")
            stock_list = self.get_all_stock_codes()
            if not stock_list:
                raise Exception("无法获取股票列表")

            # 2. 更新股票基本信息
            logger.info("步骤2: 更新股票基本信息...")
            stock_info_results = self.batch_update_stock_info(stock_list)
            total_results['stock_info'] = stock_info_results

            # 3. 下载分笔数据
            if include_tick:
                logger.info("步骤3: 下载分笔数据...")
                tick_results = self.batch_download_tick_data(trade_date, stock_list)
                total_results['tick_data'] = tick_results

            # 4. 下载基础数据
            if include_basic:
                logger.info("步骤4: 下载基础数据...")
                if periods is None:
                    periods = ['daily', '1hour', '30min']
                basic_results = self.batch_download_basic_data(periods, stock_list)
                total_results['basic_data'] = basic_results

            # 计算总耗时
            total_time = time.time() - start_time
            total_results['total_time'] = total_time
            total_results['end_time'] = datetime.now().isoformat()

            logger.info("=" * 60)
            logger.info(f"一键更新完成! 总耗时: {total_time:.2f} 秒")
            logger.info("=" * 60)

            # 保存总结报告
            self._save_batch_report('one_click_all', total_results)

            return total_results

        except Exception as e:
            logger.error(f"一键更新失败: {e}")
            total_results['error'] = str(e)
            total_results['total_time'] = time.time() - start_time
            return total_results

    def setup_scheduled_tasks(self):
        """设置定时任务"""
        try:
            # 每个交易日收盘后更新基础数据
            schedule.every().day.at("15:30").do(self._scheduled_basic_data_update)

            # 每个交易日收盘后更新分笔数据
            schedule.every().day.at("16:00").do(self._scheduled_tick_data_update)

            # 每周更新一次股票基本信息
            schedule.every().monday.at("09:00").do(self._scheduled_stock_info_update)

            logger.info("定时任务设置完成")

        except Exception as e:
            logger.error(f"设置定时任务失败: {e}")

    def _scheduled_basic_data_update(self):
        """定时基础数据更新"""
        logger.info("定时任务: 开始更新基础数据")
        self.batch_download_basic_data(['daily'])

    def _scheduled_tick_data_update(self):
        """定时分笔数据更新"""
        logger.info("定时任务: 开始更新分笔数据")
        today = datetime.now().strftime('%Y%m%d')
        self.batch_download_tick_data(today)

    def _scheduled_stock_info_update(self):
        """定时股票信息更新"""
        logger.info("定时任务: 开始更新股票信息")
        self.batch_update_stock_info()

    def export_all_to_excel(self, output_file: Optional[str] = None) -> str:
        """导出所有数据到Excel文件"""
        try:
            if output_file is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_file = self.output_dir / f"all_stock_data_{timestamp}.xlsx"

            logger.info(f"开始导出所有数据到Excel: {output_file}")

            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # 导出股票列表
                stock_list_df = pd.DataFrame(self.get_all_stock_codes())
                stock_list_df.to_excel(writer, sheet_name='股票列表', index=False)

                # 导出股票基本信息（从数据库）
                try:
                    stock_info_sql = "SELECT * FROM stock_info ORDER BY stock_code"
                    stock_info_df = db_manager.query_to_dataframe(stock_info_sql)
                    if not stock_info_df.empty:
                        stock_info_df.to_excel(writer, sheet_name='股票基本信息', index=False)
                except Exception as e:
                    logger.warning(f"导出股票基本信息失败: {e}")

                # 导出最新交易数据样本（前100只股票的最新日线数据）
                try:
                    latest_data_sql = """
                    SELECT * FROM basic_data
                    WHERE period = 'daily'
                    AND trade_date = (SELECT MAX(trade_date) FROM basic_data WHERE period = 'daily')
                    ORDER BY stock_code
                    LIMIT 100
                    """
                    latest_data_df = db_manager.query_to_dataframe(latest_data_sql)
                    if not latest_data_df.empty:
                        latest_data_df.to_excel(writer, sheet_name='最新交易数据样本', index=False)
                except Exception as e:
                    logger.warning(f"导出最新交易数据失败: {e}")

            logger.info(f"Excel导出完成: {output_file}")
            return str(output_file)

        except Exception as e:
            logger.error(f"导出Excel失败: {e}")
            raise


# 创建全局实例
batch_processor = BatchProcessor()
