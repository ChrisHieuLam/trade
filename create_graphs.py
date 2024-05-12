#DATE: 09/05/2024
#VERSION: 1.0
import pandas as pd
from sqlalchemy.engine import URL
from sqlalchemy.engine import create_engine
from sqlalchemy import text
import pyodbc 
from sqlalchemy import insert
from sqlalchemy import MetaData, Table
import sqlalchemy as db
from sqlalchemy import select
from datetime import datetime, timedelta
from openpyxl import load_workbook
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
import base64

#DATABASE INFO
SERVER_NAME = 'lamfa.webhop.net'
DATABASE_NAME = 'dbDataWhale'
TABLE_NAME = 'tbData'
USER_NAME = 'appcoder'
PASS_WORD = 'Team_500'

conn_str = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=lamfa.webhop.net;'
    r'DATABASE=dbDataWhale;'
    r'UID=appcoder;'
    r'PWD=Team_500;'
)

#CONNECTION
connection_url = URL.create('mssql+pyodbc', query={'odbc_connect': conn_str})
engine = create_engine(connection_url, module=pyodbc)
connection = engine.connect()

#TABLES
metadata = db.MetaData() #extracting the metadata
tbProperties= db.Table('tbProperties', metadata, 
autoload_with=engine)
tbSymbol= db.Table('tbSymbol', metadata, 
autoload_with=engine)
tbStrategy= db.Table('tbStrategy', metadata, 
autoload_with=engine)
tbSummary= db.Table('tbSummary', metadata, 
autoload_with=engine)
tbTradeList = db.Table('tbTradeList', metadata, 
autoload_with=engine)
metadata.reflect(bind=engine)

#FUNCTIONALITIES
def to_excel_float(date_string):
    if date_string is None:
        return None
    date_obj = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
    base_date = datetime(1899, 12, 30)
    days_since_base = date_obj.toordinal() - base_date.toordinal()
    fraction_of_day = (date_obj - datetime(date_obj.year, date_obj.month, date_obj.day)).total_seconds() / (24 * 3600)
    return days_since_base + fraction_of_day

def format_number_5(number):
    num_str = str(number)
    start, end = num_str.split(".")
    while len(start) < 3:
        start = "0" + start
    while len(end) < 3:
        end = end + "0"
    return start[:3] + end[:2]
def format_number_4(number):
    num_str = str(number)
    start, end = num_str.split(".")
    while len(start) < 2:
        start = "0" + start
    while len(end) < 3:
        end = end + "0"
    return start[:2] + end[:2]

while True:
    while True:
        try:
            symbol = input("Enter your SYMBOL: ")
            propertiesID = []
            strategyID = []
            strategy_code = []
            strategy_ID_todraw = []
            symbolID = []
            with engine.connect() as connection:
                condition = tbSymbol.c.SymbolName == symbol
                query = db.select(tbSymbol.c.SymbolID).where(condition)
                symbolID_raw = connection.execute(query)
                for row in symbolID_raw:
                    symbolID.append(row[0])
                connection.close()

            with engine.connect() as connection:
                query = db.select(tbProperties.c.PropertiesID).where(tbProperties.c.SymbolID.in_(symbolID))
                propertiesID_raw = connection.execute(query)
                for row in propertiesID_raw:
                    propertiesID.append(row[0])
                connection.close()

            with engine.connect() as connection:
                query = db.select(tbSummary).where(tbSummary.c.PropertiesID.in_(propertiesID)).order_by(tbSummary.c.ProfitFactor.desc(), tbSummary.c.PercentProfitable.desc())
                strategyID_raw = connection.execute(query)
                for row in strategyID_raw:
                    strategyID.append(row[1])
                connection.close()

            with engine.connect() as connection:
                for i in strategyID:
                    query = db.select(tbStrategy).where(tbStrategy.c.StrategyID == i)
                    strategy_raw = connection.execute(query)
                    for row in strategy_raw:
                        strategy_code.append(row[1].strip())
                        print(row)
                connection.close()

            while True:
                try:
                    strategy = input("Enter the STRATEGY CODE: ")
                    if strategy in strategy_code:
                        break
                    else:
                        print("Not a valid STRATEGY from above!")
                except Exception as e:
                    print(e)
                    print("Not a valid STRATEGY!")

            with engine.connect() as connection:
                query = db.select(tbStrategy.c.StrategyID).where(tbStrategy.c.StrategyCode == strategy)
                strategy_todraw_raw = connection.execute(query)
                for row in strategy_todraw_raw:
                    if row[0] in strategyID:
                        strategy_ID_todraw.append(row[0])
                connection.close()
            for i in strategy_ID_todraw:
                trade_date = []
                trade_price = []
                with engine.connect() as connection:
                    query = db.select(tbSummary).where(tbSummary.c.StrategyID == i)
                    summary = connection.execute(query)
                    for row in summary:
                        profit_fac = row[12]
                        per_pro = row[20]
                        avg_trade = row[21]
                        max_drawdown = row[8]
                        max_runup = row[7]
                        net_profit = row[4]
                        total_closed_trades = row[13]
                        profit_factor = format_number_5(row[12])
                        percent_profitable = format_number_4(row[20])
                    connection.close()
                with engine.connect() as connection:
                    query = db.select(tbSummary.c.PropertiesID).where(tbSummary.c.StrategyID == i)
                    summary_prop = connection.execute(query)
                    for row in summary_prop:
                        properties_ID_file = row[0]
                    connection.close()
                with engine.connect() as connection:
                    query = db.select(tbStrategy.c.StrategyCode).where(tbStrategy.c.StrategyID == i)
                    strat_code = connection.execute(query)
                    for row in strat_code:
                        strategy_code_file = row[0].strip()
                    connection.close()
                with engine.connect() as connection:
                    query = db.select(tbProperties).where(tbProperties.c.PropertiesID == properties_ID_file)
                    backtest = connection.execute(query)
                    for row in backtest:
                        timeframe = row[5]
                        trading_range = row[2]
                        back_testing = row[3]
                        strat_inputs = row[-2]
                        strat_prop = row[-1]
                        start_str, end_str = row[3].split(" — ")
                        # Convert the string representations to datetime objects
                        try:
                            start_date = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
                            end_date = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")
                        except Exception:
                            start_date = datetime.strptime(start_str, "%Y-%m-%d %H:%M")
                            end_date = datetime.strptime(end_str, "%Y-%m-%d %H:%M")
                        # Format start and end dates to YYYYMMDD format
                        formatted_start_date = start_date.strftime("%Y%m%d")
                        formatted_end_date = end_date.strftime("%Y%m%d")
                        # Concatenate formatted dates with an underscore
                        backtestingrange = f"{formatted_start_date}_{formatted_end_date}"
                    connection.close()
                with engine.connect() as connection:
                    query = select(tbTradeList).where(tbTradeList.c.StrategyID == i)
                    trade_data = connection.execute(query)
                    for row in trade_data:
                        trade_date.append(row[5].strftime("%Y-%m-%d %H:%M:%S"))  # Convert datetime to timestamp
                        if float(row[6]) != 0:
                            trade_price.append(float(row[6]))  # Convert to float
                    excel_float_dates = [to_excel_float(date_str) for date_str in trade_date if datetime.strptime(date_str,"%Y-%m-%d %H:%M:%S") > datetime.strptime("2000-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")]
                    datetime_dates = [datetime(1899, 12, 30) + timedelta(days=float_date) for float_date in excel_float_dates if float_date]
                    plt.figure(figsize=(20, 6))  
                    plt.scatter(datetime_dates, trade_price, c=excel_float_dates, cmap='viridis', edgecolor='black', alpha=0.5)
                    plt.xlabel('Date')
                    plt.ylabel('Price USD')
                    plt.title('List of Trades')
                    plt.grid(True)
                    plt.switch_backend('agg')
                    path = os.path.join("/home/appcoder/graph_saved/", f"{symbol}_{strategy_code_file}_{profit_factor}_{percent_profitable}_{backtestingrange}")
                    # Create the new folder
                    os.makedirs(path, exist_ok=True)
                    plt.savefig(os.path.join(path,f'{symbol}_{strategy_code_file}_{profit_factor}_{percent_profitable}_{backtestingrange}_price_usd.png'))
                    plt.close()
                    connection.close()
                with engine.connect() as connection:
                    query = select(tbTradeList).where(tbTradeList.c.StrategyID == i)
                    trade_data = connection.execute(query)
                    profit_usd = []
                    for row in trade_data:
                        profit_usd.append(float(row[8]))
                    profit_usd.pop(0)
                    plt.figure(figsize=(20, 6))  
                    plt.scatter(datetime_dates, profit_usd, c=excel_float_dates, cmap='viridis', edgecolor='black', alpha=0.5)
                    plt.xlabel('Date')
                    plt.ylabel('Profit USD')
                    plt.title('List of Trades')
                    plt.grid(True)
                    plt.switch_backend('agg')
                    plt.savefig(os.path.join(path,f'{symbol}_{strategy_code_file}_{profit_factor}_{percent_profitable}_{backtestingrange}_profit_usd.png'))
                    plt.close()
                    filename = f"{symbol}_{strategy_code_file}_{profit_factor}_{percent_profitable}_{backtestingrange}.html"
                    connection.close()
                with open(os.path.join(path,f'{symbol}_{strategy_code_file}_{profit_factor}_{percent_profitable}_{backtestingrange}_profit_usd.png'), "rb") as img_file:
                    profit_data_pic = img_file.read()
                    profit_base64 = base64.b64encode(profit_data_pic).decode("utf-8")
                with open(os.path.join(path,f'{symbol}_{strategy_code_file}_{profit_factor}_{percent_profitable}_{backtestingrange}_price_usd.png'), "rb") as img_file:
                    price_data_pic = img_file.read()
                    price_base64 = base64.b64encode(price_data_pic).decode("utf-8")

                # HTML code with embedded image
                html_code = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>{symbol}_{strategy_code_file}_{profit_factor}_{percent_profitable}_{backtestingrange}</title>
                </head>
                <body>
                <h1>Infomation</h1>
                    Symbol: {symbol}
                    <br> <br>  
                    Strategy: {strategy_code_file}
                    <br> <br> 
                    Timeframe: {timeframe}
                    <br> 
                    Profit Factor: {profit_fac}
                    <br> <br> 
                    Percent Profitable: {per_pro}
                    <br> <br> 
                    Avg Trade: {avg_trade}
                    <br> <br> 
                    Max Drawdown: {max_drawdown}
                    <br> <br> 
                    Max RunUp: {max_runup}
                    <br> <br> 
                    Net Profit: {net_profit}
                    <br> <br> 
                    Total Closed Trades: {total_closed_trades}
                    <br> <br> 
                    Trading Range: {trading_range}
                    <br> <br> 
                    Backtesting Range: {back_testing}
                    <br> <br> 
                    Strategy Inputs: {strat_inputs}
                    <br> <br> 
                    Strategy Properties: {strat_prop}
                    <br> <br> 
                <h1>Graph</h1>
                <img src="data:image/png;base64,{price_base64}" alt="Graph of Price/Date">
                <img src="data:image/png;base64,{profit_base64}" alt="Graph of Price/Date">
                </body>
                </html>
                """
                
                file_path = os.path.join(path, filename)
                # Save HTML to a file
                with open(file_path, "w") as html_file:
                    html_file.write(html_code)
            break
        except Exception as e:
            print(e)
            print("Name not found! Please enter correctly")
    



