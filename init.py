import sqlite3

BASE_URL = 'https://fapi.binance.com'
db_path='/Users/crypto_trades.db' ###replace with your own db path


def create_tables():
    """创建必要的数据库表"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 创建交易对表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS um_pairs (
        exchange TEXT,
        symbol TEXT PRIMARY KEY,
        last_update TIMESTAMP,
        blacklist   TEXT
    )
    ''')

    # 创建数据表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS um_data (
        timestamp TIMESTAMP,
        symbol TEXT,
        mark_price REAL,
        index_price REAL,
        basis REAL,
        basis_percent REAL,
        last_funding_rate REAL,
        oi REAL,
        long_short_account_ratio REAL,
        top_trader_account_ls_ratio REAL,
        top_trader_position_ls_ratio REAL,
        taker_buy_sell_ratio REAL,
        PRIMARY KEY (timestamp, symbol)
    )
    ''')

    conn.commit()
    conn.close()

create_tables()
