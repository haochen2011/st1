import schedule
import time
import threading
from datetime import datetime, timedelta
from loguru import logger
from tick_data import tick_data  # 导入已有的分笔数据管理实例
from config import config


class TickDataScheduler:
    """分笔数据定时下载调度器"""

    def __init__(self):
        self.is_running = False
        self.schedule_thread = None
        self.download_time = config.get('tick_download_time', '15:30')  # 默认下午3:30下载
        self.stock_codes = config.get('monitor_stock_codes', [])  # 从配置获取监控股票列表
        self.save_excel = True  # 保存到Excel
        self.save_db = True  # 保存到数据库

    def set_download_time(self, time_str):
        """设置下载时间，格式为'HH:MM'"""
        try:
            # 验证时间格式
            datetime.strptime(time_str, '%H:%M')
            self.download_time = time_str
            logger.info(f"已设置分笔数据下载时间为: {time_str}")
            return True
        except ValueError:
            logger.error("时间格式错误，正确格式应为'HH:MM'")
            return False

    def set_stock_codes(self, codes):
        """设置需要下载的股票代码列表"""
        if isinstance(codes, list) and all(isinstance(code, str) for code in codes):
            self.stock_codes = codes
            logger.info(f"已设置监控股票列表，共 {len(codes)} 只股票")
            return True
        logger.error("股票代码列表格式错误，应为字符串列表")
        return False

    def set_save_options(self, save_excel=True, save_db=True):
        """设置保存选项"""
        self.save_excel = save_excel
        self.save_db = save_db
        logger.info(f"已设置保存选项: Excel={save_excel}, 数据库={save_db}")

    def download_task(self):
        """下载任务执行函数"""
        if not self.stock_codes:
            logger.warning("没有设置需要下载的股票代码，跳过下载任务")
            return

        try:
            # 获取当前日期（A股交易日为非节假日的周一至周五）
            today = datetime.now().date()
            logger.info(f"开始执行分笔数据下载任务，日期: {today.strftime('%Y-%m-%d')}")

            # 执行批量下载
            results = tick_data.batch_download_tick_data(
                stock_codes=self.stock_codes,
                trade_date=today,
                save_excel=self.save_excel,
                save_db=self.save_db
            )

            # 统计结果
            success_count = sum(1 for res in results.values() if res and res.get('data_count', 0) > 0)
            logger.info(f"分笔数据下载任务完成，成功下载 {success_count}/{len(self.stock_codes)} 只股票数据")

        except Exception as e:
            logger.error(f"分笔数据下载任务执行失败: {e}")

    def manual_trigger(self):
        """手动触发下载任务"""
        logger.info("手动触发分笔数据下载任务")
        # 在新线程中执行，避免阻塞
        threading.Thread(target=self.download_task, daemon=True).start()

    def start_scheduler(self):
        """启动定时器"""
        if self.is_running:
            logger.info("定时器已经在运行中")
            return

        # 清除已有的任务
        schedule.clear()

        # 添加每日任务
        schedule.every().day.at(self.download_time).do(self.download_task)
        logger.info(f"定时器已启动，每日 {self.download_time} 执行分笔数据下载")

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
        logger.info("定时器已停止")

    def is_active(self):
        """检查定时器是否在运行"""
        return self.is_running


# 创建全局实例
tick_scheduler = TickDataScheduler()


# 命令行交互支持
def run_interactive_mode():
    """交互式运行模式"""
    logger.info("分笔数据定时下载器 - 交互式模式")
    logger.info("输入 'help' 查看可用命令")

    while True:
        try:
            cmd = input("> ").strip().lower()

            if cmd in ['exit', 'quit']:
                if tick_scheduler.is_active():
                    tick_scheduler.stop_scheduler()
                logger.info("程序已退出")
                break

            elif cmd == 'help':
                print("可用命令:")
                print("  start          - 启动定时器")
                print("  stop           - 停止定时器")
                print("  status         - 查看定时器状态")
                print("  trigger        - 手动触发下载任务")
                print("  set time HH:MM - 设置下载时间")
                print("  set codes      - 设置股票代码列表，格式: set codes 600000,600036,000001")
                print("  set save       - 设置保存选项，格式: set save excel=True db=True")
                print("  exit/quit      - 退出程序")

            elif cmd == 'start':
                tick_scheduler.start_scheduler()

            elif cmd == 'stop':
                tick_scheduler.stop_scheduler()

            elif cmd == 'status':
                status = "运行中" if tick_scheduler.is_active() else "已停止"
                print(f"定时器状态: {status}")
                print(f"下载时间: {tick_scheduler.download_time}")
                print(f"监控股票数量: {len(tick_scheduler.stock_codes)}")
                print(f"保存选项: Excel={tick_scheduler.save_excel}, 数据库={tick_scheduler.save_db}")
                if tick_scheduler.stock_codes:
                    print(
                        f"股票列表: {', '.join(tick_scheduler.stock_codes[:5])}{'...' if len(tick_scheduler.stock_codes) > 5 else ''}")

            elif cmd == 'trigger':
                tick_scheduler.manual_trigger()

            elif cmd.startswith('set time'):
                parts = cmd.split()
                if len(parts) == 3:
                    tick_scheduler.set_download_time(parts[2])
                else:
                    print("命令格式错误，正确格式: set time HH:MM")

            elif cmd.startswith('set codes'):
                parts = cmd.split(' ', 2)
                if len(parts) == 3:
                    codes = [code.strip() for code in parts[2].split(',') if code.strip()]
                    tick_scheduler.set_stock_codes(codes)
                else:
                    print("命令格式错误，正确格式: set codes 600000,600036,000001")

            elif cmd.startswith('set save'):
                parts = cmd.split(' ', 2)
                if len(parts) == 3:
                    try:
                        # 解析参数
                        save_excel = True
                        save_db = True
                        for param in parts[2].split():
                            key, value = param.split('=')
                            if key == 'excel':
                                save_excel = value.lower() == 'true'
                            elif key == 'db':
                                save_db = value.lower() == 'true'
                        tick_scheduler.set_save_options(save_excel, save_db)
                    except Exception as e:
                        print(f"参数解析错误: {e}")
                else:
                    print("命令格式错误，正确格式: set save excel=True db=True")

            else:
                print("未知命令，请输入 'help' 查看可用命令")

        except KeyboardInterrupt:
            if tick_scheduler.is_active():
                tick_scheduler.stop_scheduler()
            logger.info("程序已退出")
            break
        except Exception as e:
            logger.error(f"命令执行错误: {e}")


if __name__ == "__main__":
    run_interactive_mode()
