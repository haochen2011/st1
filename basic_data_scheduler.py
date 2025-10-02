import schedule
import time
import threading
from datetime import datetime, timedelta
from loguru import logger
from basic_data import basic_data  # 导入已有的基础数据管理实例
from config import config


class BasicDataScheduler:
    """基础数据定时更新调度器"""
    
    def __init__(self):
        self.is_running = False
        self.schedule_thread = None
        # 基础数据通常在交易日早上更新前一天数据
        self.update_time = config.get('basic_update_time', '08:30')
        self.stock_codes = config.get('monitor_stock_codes', [])  # 监控股票列表
        self.periods = config.get('periods', ['daily', 'week', 'month'])  # 要更新的周期
        self.force_update = False  # 是否强制更新（即使数据已存在）
        
    def set_update_time(self, time_str):
        """设置更新时间，格式为'HH:MM'"""
        try:
            datetime.strptime(time_str, '%H:%M')
            self.update_time = time_str
            logger.info(f"已设置基础数据更新时间为: {time_str}")
            return True
        except ValueError:
            logger.error("时间格式错误，正确格式应为'HH:MM'")
            return False
            
    def set_stock_codes(self, codes):
        """设置需要更新的股票代码列表"""
        if isinstance(codes, list) and all(isinstance(code, str) for code in codes):
            self.stock_codes = codes
            logger.info(f"已设置监控股票列表，共 {len(codes)} 只股票")
            return True
        logger.error("股票代码列表格式错误，应为字符串列表")
        return False
        
    def set_periods(self, periods):
        """设置需要更新的周期类型"""
        valid_periods = ['daily', 'week', 'month', '1min', '5min', '15min', '30min', '60min']
        filtered = [p for p in periods if p in valid_periods]
        if filtered:
            self.periods = filtered
            logger.info(f"已设置更新周期: {', '.join(filtered)}")
            return True
        logger.error(f"无效的周期设置，有效周期: {', '.join(valid_periods)}")
        return False
        
    def set_force_update(self, force):
        """设置是否强制更新"""
        self.force_update = bool(force)
        logger.info(f"已设置强制更新: {self.force_update}")
        
    def update_task(self):
        """更新任务执行函数"""
        if not self.stock_codes:
            logger.warning("没有设置需要更新的股票代码，跳过更新任务")
            return
            
        try:
            today = datetime.now().date()
            # 判断是否为交易日（简单判断：周一至周五，排除节假日）
            if today.weekday() >= 5:  # 0=周一, 4=周五, 5=周六, 6=周日
                logger.info(f"今天是周末({today.strftime('%Y-%m-%d')})，不执行基础数据更新")
                return
                
            logger.info(f"开始执行基础数据更新任务，日期: {today.strftime('%Y-%m-%d')}")
            
            results = {}
            for stock_code in self.stock_codes:
                try:
                    # 执行更新
                    updated = basic_data.update_basic_data(
                        stock_code=stock_code,
                        periods=self.periods,
                        force_update=self.force_update
                    )
                    results[stock_code] = {
                        'success': True,
                        'periods_updated': list(updated.keys())
                    }
                    logger.info(f"股票 {stock_code} 基础数据更新完成，更新了 {len(updated)} 个周期")
                except Exception as e:
                    logger.error(f"股票 {stock_code} 基础数据更新失败: {e}")
                    results[stock_code] = {
                        'success': False,
                        'error': str(e)
                    }
            
            # 统计结果
            success_count = sum(1 for res in results.values() if res['success'])
            logger.info(f"基础数据更新任务完成，成功更新 {success_count}/{len(self.stock_codes)} 只股票")
            
        except Exception as e:
            logger.error(f"基础数据更新任务执行失败: {e}")
    
    def manual_trigger(self):
        """手动触发更新任务"""
        logger.info("手动触发基础数据更新任务")
        # 在新线程中执行，避免阻塞
        threading.Thread(target=self.update_task, daemon=True).start()
    
    def start_scheduler(self):
        """启动定时器"""
        if self.is_running:
            logger.info("定时器已经在运行中")
            return
            
        # 清除已有的任务
        schedule.clear()
        
        # 添加每日任务
        schedule.every().day.at(self.update_time).do(self.update_task)
        logger.info(f"基础数据定时器已启动，每日 {self.update_time} 执行更新")
        
        self.is_running = True
        # 启动调度线程
        self.schedule_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.schedule_thread.start()
    
    def _run_scheduler(self):
        """调度器运行循环"""
        while self.is_running:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次
        
    def stop_scheduler(self):
        """停止定时器"""
        if not self.is_running:
            logger.info("定时器已经停止")
            return
            
        self.is_running = False
        if self.schedule_thread:
            self.schedule_thread.join()
        schedule.clear()
        logger.info("基础数据定时器已停止")
    
    def is_active(self):
        """检查定时器是否在运行"""
        return self.is_running


# 创建全局实例
basic_scheduler = BasicDataScheduler()


# 命令行交互支持
def run_interactive_mode():
    """交互式运行模式"""
    logger.info("基础数据定时更新器 - 交互式模式")
    logger.info("输入 'help' 查看可用命令")
    
    while True:
        try:
            cmd = input("> ").strip().lower()
            
            if cmd in ['exit', 'quit']:
                if basic_scheduler.is_active():
                    basic_scheduler.stop_scheduler()
                logger.info("程序已退出")
                break
                
            elif cmd == 'help':
                print("可用命令:")
                print("  start          - 启动定时器")
                print("  stop           - 停止定时器")
                print("  status         - 查看定时器状态")
                print("  trigger        - 手动触发更新任务")
                print("  set time HH:MM - 设置更新时间")
                print("  set codes      - 设置股票代码列表，格式: set codes 600000,600036,000001")
                print("  set periods    - 设置更新周期，格式: set periods daily,week,60min")
                print("  set force T/F  - 设置是否强制更新，格式: set force True")
                print("  exit/quit      - 退出程序")
                
            elif cmd == 'start':
                basic_scheduler.start_scheduler()
                
            elif cmd == 'stop':
                basic_scheduler.stop_scheduler()
                
            elif cmd == 'status':
                status = "运行中" if basic_scheduler.is_active() else "已停止"
                print(f"定时器状态: {status}")
                print(f"更新时间: {basic_scheduler.update_time}")
                print(f"监控股票数量: {len(basic_scheduler.stock_codes)}")
                print(f"更新周期: {', '.join(basic_scheduler.periods)}")
                print(f"强制更新: {basic_scheduler.force_update}")
                if basic_scheduler.stock_codes:
                    print(f"股票列表: {', '.join(basic_scheduler.stock_codes[:5])}{'...' if len(basic_scheduler.stock_codes) > 5 else ''}")
                
            elif cmd == 'trigger':
                basic_scheduler.manual_trigger()
                
            elif cmd.startswith('set time'):
                parts = cmd.split()
                if len(parts) == 3:
                    basic_scheduler.set_update_time(parts[2])
                else:
                    print("命令格式错误，正确格式: set time HH:MM")
                    
            elif cmd.startswith('set codes'):
                parts = cmd.split(' ', 2)
                if len(parts) == 3:
                    codes = [code.strip() for code in parts[2].split(',') if code.strip()]
                    basic_scheduler.set_stock_codes(codes)
                else:
                    print("命令格式错误，正确格式: set codes 600000,600036,000001")
                    
            elif cmd.startswith('set periods'):
                parts = cmd.split(' ', 2)
                if len(parts) == 3:
                    periods = [p.strip() for p in parts[2].split(',') if p.strip()]
                    basic_scheduler.set_periods(periods)
                else:
                    print("命令格式错误，正确格式: set periods daily,week,60min")
                    
            elif cmd.startswith('set force'):
                parts = cmd.split()
                if len(parts) == 3:
                    basic_scheduler.set_force_update(parts[2].lower() == 'true')
                else:
                    print("命令格式错误，正确格式: set force True 或 set force False")
                    
            else:
                print("未知命令，请输入 'help' 查看可用命令")
                
        except KeyboardInterrupt:
            if basic_scheduler.is_active():
                basic_scheduler.stop_scheduler()
            logger.info("程序已退出")
            break
        except Exception as e:
            logger.error(f"命令执行错误: {e}")


if __name__ == "__main__":
    run_interactive_mode()
    