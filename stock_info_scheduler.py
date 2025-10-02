import schedule
import time
import threading
from datetime import datetime, timedelta
from loguru import logger

from database import db_manager
from stock_info import stock_info  # 导入已有的股票信息管理实例
from config import config


class StockInfoScheduler:
    """股票信息定时更新调度器"""
    
    def __init__(self):
        self.is_running = False
        self.schedule_thread = None
        # 股票信息更新频率较低，默认每周一更新一次
        self.update_time = config.get('stock_info_update_time', '09:00')
        self.update_day = config.get('stock_info_update_day', 'monday')  # 每周更新日
        self.markets = config.get('market_codes', ['all'])  # 要更新的市场
        self.update_financial = True  # 是否更新财务数据
        self.full_update = False  # 是否全量更新（否则只更新新增股票）
        
    def set_update_schedule(self, time_str, day_of_week):
        """设置更新时间和星期几，星期几可用值: monday, tuesday, ..., sunday"""
        valid_days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        day_of_week = day_of_week.lower()
        
        try:
            # 验证时间格式
            datetime.strptime(time_str, '%H:%M')
            if day_of_week not in valid_days:
                raise ValueError(f"无效的星期值，必须是: {', '.join(valid_days)}")
                
            self.update_time = time_str
            self.update_day = day_of_week
            logger.info(f"已设置股票信息更新时间为: 每周{day_of_week} {time_str}")
            return True
        except ValueError as e:
            logger.error(f"时间设置错误: {e}")
            return False
            
    def set_markets(self, markets):
        """设置需要更新的市场，可选值: all, sh, sz"""
        valid_markets = ['all', 'sh', 'sz']
        filtered = [m for m in markets if m in valid_markets]
        if filtered:
            self.markets = filtered
            logger.info(f"已设置更新市场: {', '.join(filtered)}")
            return True
        logger.error(f"无效的市场设置，有效市场: {', '.join(valid_markets)}")
        return False
        
    def set_update_options(self, update_financial=True, full_update=False):
        """设置更新选项"""
        self.update_financial = update_financial
        self.full_update = full_update
        logger.info(f"已设置更新选项: 财务数据={update_financial}, 全量更新={full_update}")
        
    def update_task(self):
        """更新任务执行函数"""
        try:
            today = datetime.now().date()
            logger.info(f"开始执行股票信息更新任务，日期: {today.strftime('%Y-%m-%d')}")
            
            # 逐个市场更新
            total_updated = 0
            for market in self.markets:
                try:
                    # 获取市场股票列表
                    stock_list = stock_info.get_stock_list(market)
                    if stock_list.empty:
                        logger.warning(f"{market}市场没有获取到股票列表，跳过该市场更新")
                        continue
                        
                    # 如果不是全量更新，只获取新增股票
                    if not self.full_update:
                        existing_codes = stock_info.get_stock_info_from_db()['stock_code'].tolist()
                        new_stocks = stock_list[~stock_list['SECURITY_CODE_A'].isin(existing_codes)]
                        logger.info(f"{market}市场共有{len(stock_list)}只股票，其中新增{len(new_stocks)}只需要更新")
                        stock_list = new_stocks
                        
                    # 更新股票信息到数据库
                    if not stock_list.empty:
                        updated = stock_info.update_stock_info_to_db(stock_list)
                        total_updated += updated
                        logger.info(f"{market}市场股票信息更新完成，成功更新{updated}只股票")
                        
                        # 如果需要更新财务数据
                        if self.update_financial:
                            self._update_financial_data(stock_list['SECURITY_CODE_A'].tolist())
                            
                except Exception as e:
                    logger.error(f"{market}市场股票信息更新失败: {e}")
                    continue
            
            logger.info(f"股票信息更新任务完成，总共成功更新 {total_updated} 只股票")
            
        except Exception as e:
            logger.error(f"股票信息更新任务执行失败: {e}")
    
    def _update_financial_data(self, stock_codes):
        """更新财务数据（单独抽离，便于控制）"""
        if not stock_codes:
            return
            
        logger.info(f"开始更新 {len(stock_codes)} 只股票的财务数据")
        success_count = 0
        
        # 只更新最新年份的财务数据
        current_year = datetime.now().year
        
        for code in stock_codes:
            try:
                financial_data = stock_info.get_stock_financial_data(code, current_year)
                if not financial_data.empty:
                    # 标准化财务数据并保存到数据库
                    financial_data['stock_code'] = code
                    financial_data['year'] = current_year
                    financial_data = financial_data.rename(columns={'报告期': 'report_date'})
                    
                    # 保存到数据库
                    db_manager.insert_dataframe(
                        financial_data, 
                        'stock_financial', 
                        if_exists='append'
                    )
                    success_count += 1
                    
            except Exception as e:
                logger.error(f"股票 {code} 财务数据更新失败: {e}")
                continue
                
        logger.info(f"财务数据更新完成，成功更新 {success_count}/{len(stock_codes)} 只股票")
    
    def manual_trigger(self, full_update=False):
        """手动触发更新任务"""
        logger.info(f"手动触发股票信息更新任务，全量更新: {full_update}")
        # 保存当前的full_update设置并临时修改
        original_full = self.full_update
        self.full_update = full_update
        
        # 在新线程中执行，避免阻塞
        thread = threading.Thread(target=self.update_task, daemon=True)
        thread.start()
        
        # 恢复原始设置
        self.full_update = original_full
    
    def start_scheduler(self):
        """启动定时器"""
        if self.is_running:
            logger.info("定时器已经在运行中")
            return
            
        # 清除已有的任务
        schedule.clear()
        
        # 根据设置的星期几添加任务
        schedule_method = getattr(schedule.every(), self.update_day)
        schedule_method.at(self.update_time).do(self.update_task)
        
        logger.info(f"股票信息定时器已启动，每周{self.update_day} {self.update_time} 执行更新")
        
        self.is_running = True
        # 启动调度线程
        self.schedule_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.schedule_thread.start()
    
    def _run_scheduler(self):
        """调度器运行循环"""
        while self.is_running:
            schedule.run_pending()
            time.sleep(3600)  # 每小时检查一次（因为更新频率低）
        
    def stop_scheduler(self):
        """停止定时器"""
        if not self.is_running:
            logger.info("定时器已经停止")
            return
            
        self.is_running = False
        if self.schedule_thread:
            self.schedule_thread.join()
        schedule.clear()
        logger.info("股票信息定时器已停止")
    
    def is_active(self):
        """检查定时器是否在运行"""
        return self.is_running


# 创建全局实例
info_scheduler = StockInfoScheduler()


# 命令行交互支持
def run_interactive_mode():
    """交互式运行模式"""
    logger.info("股票信息定时更新器 - 交互式模式")
    logger.info("输入 'help' 查看可用命令")
    
    while True:
        try:
            cmd = input("> ").strip().lower()
            
            if cmd in ['exit', 'quit']:
                if info_scheduler.is_active():
                    info_scheduler.stop_scheduler()
                logger.info("程序已退出")
                break
                
            elif cmd == 'help':
                print("可用命令:")
                print("  start               - 启动定时器")
                print("  stop                - 停止定时器")
                print("  status              - 查看定时器状态")
                print("  trigger [full]      - 手动触发更新任务，加full参数表示全量更新")
                print("  set schedule        - 设置更新时间和星期，格式: set schedule HH:MM monday")
                print("  set markets         - 设置更新市场，格式: set markets all,sh")
                print("  set options         - 设置更新选项，格式: set options financial=True full=False")
                print("  exit/quit           - 退出程序")
                
            elif cmd == 'start':
                info_scheduler.start_scheduler()
                
            elif cmd == 'stop':
                info_scheduler.stop_scheduler()
                
            elif cmd == 'status':
                status = "运行中" if info_scheduler.is_active() else "已停止"
                print(f"定时器状态: {status}")
                print(f"更新时间: 每周{info_scheduler.update_day} {info_scheduler.update_time}")
                print(f"更新市场: {', '.join(info_scheduler.markets)}")
                print(f"更新选项: 财务数据={info_scheduler.update_financial}, 全量更新={info_scheduler.full_update}")
                
            elif cmd.startswith('trigger'):
                full_update = 'full' in cmd
                info_scheduler.manual_trigger(full_update)
                
            elif cmd.startswith('set schedule'):
                parts = cmd.split()
                if len(parts) == 4 and parts[2] and parts[3]:
                    info_scheduler.set_update_schedule(parts[2], parts[3])
                else:
                    print("命令格式错误，正确格式: set schedule HH:MM monday")
                    
            elif cmd.startswith('set markets'):
                parts = cmd.split(' ', 2)
                if len(parts) == 3:
                    markets = [m.strip() for m in parts[2].split(',') if m.strip()]
                    info_scheduler.set_markets(markets)
                else:
                    print("命令格式错误，正确格式: set markets all,sh")
                    
            elif cmd.startswith('set options'):
                parts = cmd.split(' ', 2)
                if len(parts) == 3:
                    try:
                        update_financial = True
                        full_update = False
                        for param in parts[2].split():
                            key, value = param.split('=')
                            if key == 'financial':
                                update_financial = value.lower() == 'true'
                            elif key == 'full':
                                full_update = value.lower() == 'true'
                        info_scheduler.set_update_options(update_financial, full_update)
                    except Exception as e:
                        print(f"参数解析错误: {e}")
                else:
                    print("命令格式错误，正确格式: set options financial=True full=False")
                    
            else:
                print("未知命令，请输入 'help' 查看可用命令")
                
        except KeyboardInterrupt:
            if info_scheduler.is_active():
                info_scheduler.stop_scheduler()
            logger.info("程序已退出")
            break
        except Exception as e:
            logger.error(f"命令执行错误: {e}")


if __name__ == "__main__":
    run_interactive_mode()
