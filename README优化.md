# A股数据管理系统 v2.0 (优化版)

## 🚀 系统概述

这是一个全面优化的A股数据管理系统，支持一键获取所有A股股票的分笔数据、K线数据，并可批量存储到数据库和导出Excel。相比原版，在性能、稳定性和易用性方面都有显著提升。

## ✨ 核心功能

### 1. 一键数据更新
- **一键获取所有A股数据**: 股票信息 + 分笔数据 + K线数据
- **批量处理**: 支持4000+只股票的并发处理
- **智能重试**: 网络异常自动重试，确保数据完整性
- **进度显示**: 实时显示处理进度和统计信息

### 2. 优化的数据库管理
- **连接池**: 支持高并发数据库操作
- **批量插入**: 大幅提升数据存储速度
- **UPSERT操作**: 智能更新或插入数据
- **数据验证**: 自动清洗和验证数据

### 3. 增强的Excel导出
- **多Sheet导出**: 股票列表、基本信息、交易数据、技术指标等
- **数据格式化**: 自动格式化数字、日期和样式
- **条件格式**: 涨跌幅颜色显示，数据条等
- **大数据支持**: 分批导出，避免内存溢出

### 4. 定时任务
- **自动更新**: 每日收盘后自动更新数据
- **灵活配置**: 可自定义更新时间和频率
- **后台运行**: 支持后台定时执行

## 📦 安装配置

### 1. 环境要求
```bash
Python 3.8+
MySQL 5.7+
```

### 2. 安装依赖
```bash
pip install -r requirements.txt
```

### 3. 数据库配置
编辑 `config.ini` 文件：
```ini
[database]
host = localhost
port = 3306
user = root
password = your_password
database = stock_dragon
```

### 4. 初始化数据库
系统会自动创建所需的表结构。

## 🎯 快速开始

### 方式一：交互式界面（推荐）
```bash
python stock_data_manager.py
```

进入交互式菜单，选择相应功能：
```
1. 📊 一键更新所有A股数据    # 最常用功能
2. 📈 批量更新股票基本信息
3. ⏰ 批量下载分笔数据
4. 📉 批量下载K线数据
5. 📋 导出所有数据到Excel   # 导出功能
...
```

### 方式二：命令行模式
```bash
# 一键更新所有数据
python stock_data_manager.py --mode cli --command update-all

# 只更新股票基本信息
python stock_data_manager.py --mode cli --command update-stocks

# 下载指定日期的分笔数据
python stock_data_manager.py --mode cli --command download-tick --date 20241201

# 下载K线数据
python stock_data_manager.py --mode cli --command download-basic --periods daily,1hour

# 导出Excel
python stock_data_manager.py --mode cli --command export-excel
```

## 📊 主要模块说明

### 1. BatchProcessor (batch_processor.py)
批量数据处理器，核心功能模块：
```python
from batch_processor import batch_processor

# 一键更新所有数据
result = batch_processor.one_click_update_all(
    include_tick=True,      # 包含分笔数据
    include_basic=True,     # 包含K线数据
    periods=['daily', '1hour', '30min']  # K线周期
)

# 批量更新股票信息
result = batch_processor.batch_update_stock_info()

# 批量下载分笔数据
result = batch_processor.batch_download_tick_data('20241201')
```

### 2. EnhancedDatabaseManager (enhanced_database.py)
优化的数据库管理器：
```python
from enhanced_database import enhanced_db_manager

# 批量插入DataFrame
enhanced_db_manager.batch_insert_dataframe(df, 'table_name')

# UPSERT操作
enhanced_db_manager.upsert_dataframe(df, 'table_name', ['unique_col'])

# 查询数据
df = enhanced_db_manager.query_to_dataframe("SELECT * FROM stock_info")
```

### 3. EnhancedExcelExporter (enhanced_excel_exporter.py)
增强的Excel导出器：
```python
from enhanced_excel_exporter import enhanced_excel_exporter

# 导出所有股票数据
filename = enhanced_excel_exporter.export_all_stock_data(
    include_basic_data=True,
    include_tick_data=True,
    include_indicators=True
)

# 导出单个股票详情
filename = enhanced_excel_exporter.export_stock_detail_by_code('000001', days=90)
```

## 📈 使用示例

### 1. 新用户首次使用
```bash
# 1. 启动系统
python stock_data_manager.py

# 2. 选择 "1. 📊 一键更新所有A股数据"
# 3. 选择包含的数据类型（建议全选）
# 4. 等待处理完成（首次可能需要1-2小时）
# 5. 选择 "5. 📋 导出所有数据到Excel" 查看结果
```

### 2. 日常数据更新
```bash
# 每日收盘后更新
python stock_data_manager.py

# 选择 "1. 📊 一键更新所有A股数据"
# 通常只需要10-30分钟完成更新
```

### 3. 查询特定股票
```bash
# 选择 "6. 🔍 查询单个股票详情"
# 输入股票代码，如：000001
# 可选择导出该股票的详细Excel报告
```

### 4. 设置定时任务
```bash
# 选择 "9. ⏰ 设置定时任务"
# 配置每日自动更新时间
# 系统将在后台自动运行
```

## 📋 数据表结构

### stock_info (股票基本信息)
- stock_code: 股票代码
- stock_name: 股票名称  
- market: 市场(sh/sz)
- industry: 所属行业
- list_date: 上市日期
- total_shares: 总股本
- float_shares: 流通股本

### basic_data (K线数据)
- stock_code: 股票代码
- period: 周期(daily/1hour/30min等)
- trade_date: 交易日期
- open_price/high_price/low_price/close_price: OHLC价格
- volume: 成交量
- amount: 成交额

### tick_data (分笔数据)
- stock_code: 股票代码
- trade_date: 交易日期
- trade_time: 交易时间
- price: 成交价
- volume: 成交量
- trade_type: 交易类型

## 🔧 性能优化

### 1. 多线程并发
- 默认10个线程并发处理
- 可在 `BatchProcessor` 中调整 `max_workers` 参数

### 2. 批量操作
- 数据库批量插入，每批1000条记录
- Excel分批导出，避免内存溢出

### 3. 连接池
- 数据库连接池，支持20个并发连接
- 自动重连和连接检测

### 4. 内存优化
- DataFrame分块处理
- 及时释放大对象内存

## 🚨 注意事项

### 1. 网络要求
- 需要稳定的互联网连接
- 建议在网络条件良好时进行批量更新

### 2. 数据库配置
- 确保MySQL配置足够的连接数
- 建议设置较大的 `max_connections`

### 3. 磁盘空间
- 全量数据约需要几GB存储空间
- 确保足够的磁盘剩余空间

### 4. 时间安排
- 首次全量更新需要较长时间（1-3小时）
- 建议在交易时间外进行更新
- 分笔数据仅在交易日可获取

## 📞 技术支持

如果遇到问题，请检查：
1. 数据库连接配置是否正确
2. 网络连接是否稳定
3. Python依赖包是否完整安装
4. 查看 `logs/` 目录下的日志文件

## 🔄 版本历史

### v2.0 (优化版)
- ✅ 新增批量处理器，支持一键更新所有数据
- ✅ 优化数据库管理，提升存储性能
- ✅ 增强Excel导出，支持多sheet和格式化  
- ✅ 添加交互式界面，提升易用性
- ✅ 完善错误处理和日志记录
- ✅ 支持定时任务和后台运行

### v1.0 (原版)
- 基础的股票数据获取功能
- 简单的数据库存储
- 基础的Excel导出

---

**Happy Trading! 📈**