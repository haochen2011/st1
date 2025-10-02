# 股票数据分析系统

一个基于Python的股票数据分析系统，支持分笔数据、基础数据和技术指标的采集、存储、处理和API服务。

## 功能特性

### 数据结构
- **原始数据层**：分笔数据(tick)和F10基本信息
- **基础数据层**：OHLCV数据，支持多个时间周期
- **指标数据层**：技术指标计算和存储

### 主要功能
- 股票数据采集（分笔、基础、财务数据）
- 多周期K线数据生成
- 技术指标计算（MACD、RSI、KDJ等）
- 数据库存储和管理
- REST API接口
- Excel文件导入导出

## 安装和配置

### 1. 安装依赖
```bash
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

### 2. 数据库配置
编辑 `config.ini` 文件，配置MySQL数据库连接：

```ini
[database]
host = localhost
port = 3306
user = root
password = your_password
database = stock_analysis
```

### 3. 创建数据库
在MySQL中创建数据库：
```sql
CREATE DATABASE stock_analysis CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

## 使用方法

### 启动服务器
```bash
python run_server.py
```

服务器将在 http://localhost:5000 启动

### 数据采集示例

#### 获取股票列表
```python
from stock_info import stock_info

# 获取所有股票列表
stocks = stock_info.get_stock_list()

# 更新股票信息到数据库
stock_info.update_stock_info_to_db()
```

#### 获取分笔数据
```python
from tick_data import tick_data

# 获取某只股票的分笔数据
tick_df = tick_data.get_tick_data('600000', '20231201')

# 保存到Excel和数据库
tick_data.download_and_save_tick_data('600000', '20231201')
```

#### 获取基础数据
```python
from basic_data import basic_data

# 获取日线数据
daily_data = basic_data.get_stock_data('600000', 'daily')

# 更新所有周期数据
basic_data.update_basic_data('600000')
```

#### 计算技术指标
```python
from indicator_processor import indicator_processor

# 计算MACD
data_with_macd = indicator_processor.calculate_macd(daily_data)

# 计算RSI
data_with_rsi = indicator_processor.calculate_rsi(daily_data)
```

## API接口

### 股票信息
- `GET /api/stock/list` - 获取股票列表
- `GET /api/stock/info/{stock_code}` - 获取股票基本信息

### 数据查询
- `GET /api/data/tick/{stock_code}` - 获取分笔数据
- `GET /api/data/basic/{stock_code}` - 获取基础数据

### 数据更新
- `POST /api/data/update/{stock_code}` - 更新股票数据

### 技术指标
- `GET /api/indicator/calculate/{stock_code}` - 计算技术指标
- `GET /api/indicator/get/{stock_code}/{indicator_name}` - 获取指标数据

### 统计分析
- `GET /api/analysis/statistics/{stock_code}` - 获取统计信息

### 健康检查
- `GET /api/health` - 系统健康检查

## 支持的周期
- 1分钟 (1min)
- 5分钟 (5min)  
- 10分钟 (10min)
- 15分钟 (15min)
- 30分钟 (30min)
- 1小时 (1hour)
- 日线 (daily)
- 周线 (week)
- 月线 (month)
- 季线 (quarter)
- 半年线 (half-year)
- 年线 (year)

## 支持的技术指标
- 移动平均线 (MA)
- 指数移动平均 (EMA)
- MACD
- 相对强弱指标 (RSI)
- 布林带 (BOLL)
- KDJ随机指标
- 顺势指标 (CCI)
- 威廉指标 (WR)
- 乖离率 (BIAS)

## 数据存储格式

### 分笔数据文件命名
```
{股票名称}_{股票代码}_tick_{日期}.xlsx
例如: 平安银行_000001_tick_20231201.xlsx
```

### 基础数据文件命名  
```
{股票名称}_{股票代码}_basic_qfq.xlsx
例如: 平安银行_000001_basic_qfq.xlsx
```

## 目录结构
```
stock_analysis_sys/
├── all_api.py              # 总API接口
├── basic_data.py           # 基础数据管理
├── basic_processor.py      # 基础数据处理器
├── config.ini              # 配置文件
├── config.py               # 配置管理
├── database.py             # 数据库管理
├── indicator_api.py        # 指标API
├── indicator_processor.py  # 指标处理器
├── README.md               # 说明文档
├── requirements.txt        # 依赖包
├── run_server.py          # 服务器启动
├── stock_info.py          # 股票信息管理
├── tick_data.py           # 分笔数据管理
├── tick_processor.py      # 分笔数据处理器
├── data/                  # 数据存储目录
│   ├── tick/              # 分笔数据
│   ├── basic/             # 基础数据
│   └── indicator/         # 指标数据
└── logs/                  # 日志文件
```

## 注意事项
1. 首次运行需要配置数据库连接
2. 建议定期备份数据库和Excel文件
3. 分笔数据量较大，注意存储空间
4. API接口支持跨域访问
5. 日志文件自动轮转，保留30天

## 版权声明
本项目仅供学习和研究使用，请遵守相关法律法规。