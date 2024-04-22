#4/10/2024
# version 1

# Note:
# Import data from tv to SQL server 
import json
import re
import shutil
import pandas as pd
from sqlalchemy.engine import URL
from sqlalchemy.engine import create_engine
from sqlalchemy import text
import pyodbc 
from sqlalchemy import insert
from sqlalchemy import MetaData, Table
import sqlalchemy as db
from sqlalchemy import select
import sys
from datetime import datetime
import os
from openpyxl import load_workbook
import time




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

# Path to your CSV file
connection_url = URL.create('mssql+pyodbc', query={'odbc_connect': conn_str})
engine = create_engine(connection_url, module=pyodbc)
connection = engine.connect()

while True:
    excel_path = '/home/appcoder/excels-tv'
    excel_files = [file for file in os.listdir(excel_path) if file.endswith('.xlsx')]
    while not any(os.listdir(excel_path)):
        print("Waiting for file")
        time.sleep(2)
    for workbook in excel_files:
        workbook_path = os.path.join(excel_path, workbook)
        work_book = load_workbook(workbook_path)
        data_sheet1 = {}
        data_sheet2 = {} 
        for sheet_name in work_book.sheetnames:
            sheet = work_book[sheet_name]
            sheet_data = []
            for row in sheet.iter_rows(values_only=True):
                if sheet_name == 'Sheet1':
                    data_sheet1[row[0]] = row[1]
                elif sheet_name == 'Sheet2':
                    data_sheet2[row[0]] = row[1] 

        if 'Title' in data_sheet1.keys():
            properties_data = data_sheet1
            del properties_data['Title']
            summary_data = data_sheet2
            del summary_data['Unnamed: 0']
        else:
            properties_data = data_sheet2
            del properties_data['Title']
            summary_data = data_sheet1
            del summary_data['Unnamed: 0']



        #defining tables in SQLAlchemy

        metadata = db.MetaData() #extracting the metadata
        tbProperties= db.Table('tbProperties', metadata, 
        autoload_with=engine)
        tbSymbol= db.Table('tbSymbol', metadata, 
        autoload_with=engine)
        tbStrategy= db.Table('tbStrategy', metadata, 
        autoload_with=engine)
        tbSummary= db.Table('tbSummary', metadata, 
        autoload_with=engine)
        metadata.reflect(bind=engine)

        #check if Symbol is inserted or not
        #insert if not
        with engine.connect() as connection:
            select_query = db.select(tbSymbol.c.SymbolCode)
            return_Codes = connection.execute(select_query).fetchall()
            codes = []
            for code in return_Codes:
                codes.append(code[0])
            symbolname = properties_data['Symbol']
            while len(symbolname) != 20:
                symbolname += ' '
            if symbolname not in codes:
                put_in = symbolname.split(":")
                ins = tbSymbol.insert().values(**{
                "SymbolCode": properties_data['Symbol'],
                "SymbolName": put_in[1]
                }
                )
                connection.execute(ins)
                connection.commit()
            connection.close()

        # insert SymbolID to Properties
        with engine.connect() as connection:
            select_columns = db.select(tbSymbol.c.SymbolID)
            condition = tbSymbol.c.SymbolCode == properties_data['Symbol']
            query = db.select(tbSymbol.c.SymbolID).where(condition)
            output = connection.execute(query)
            #Get Symbol ID WHERE Symbol == properties_data['Symbol']
            inputs = None
            for out in output:
                inputs = out[0]
            #get start end dates
            dates = properties_data['Trading range'].split("—")
            try:
                start_date = datetime.strptime(dates[0].rstrip(),"%Y-%m-%d %H:%M:%S")
                end_date = datetime.strptime(dates[1].lstrip(),"%Y-%m-%d %H:%M:%S")
            except ValueError:
                start_date = datetime.strptime(dates[0].rstrip(),"%Y-%m-%d %H:%M")
                end_date = datetime.strptime(dates[1].lstrip(),"%Y-%m-%d %H:%M")
            
            #Get keys that inputed
            strategy_properties_keys = ['Initial capital', 'Order size', 'Pyramiding', 'Commission', 'Slippage', 'Verify price for limit orders', 'Margin for long positions', 'Margin for short positions','Recalculate After order is filled', 'Recalculate On every tick','Recalculate On bar close','Backtesting precision. Use bar magnifier']
            inputed = ['Trading range', 'Backtesting range','Symbol', 'Timeframe','Chart type','Point Value','Precision']
            strategy_properties = {}
            for key in strategy_properties_keys:
                strategy_properties[key] = properties_data[key]
            concatenated = strategy_properties_keys + inputed
            #Get remained keys that not inputed
            strategy_inputs = {key: properties_data[key] for key in set(properties_data.keys()) - set(concatenated)}
            #make json
            json_prop = json.dumps(strategy_properties)
            json_int = json.dumps(strategy_inputs)

            #input to Properties
            inss = tbProperties.insert().values(**{
            "SymbolID": inputs,
            "TradingRange": properties_data['Trading range'],
            "BacktestingRange": properties_data['Backtesting range'],
            "Timeframe": properties_data['Timeframe'],
            "ChartType": properties_data['Chart type'],
            "PointValue": properties_data['Point Value'],
            "Precision": properties_data['Precision'],
            "BacktestStartYear": start_date.year,
            "BacktestStartDay": "{} - {}".format(start_date.day, start_date.month),
            "BacktestStopYear": end_date.year,
            "BacktestStopDay": "{} - {}".format(end_date.day, end_date.month),
            "StrategyInputs": json_int,
            "StrategyProperties": json_prop,

            }
            )   
            connection.execute(inss)
            connection.commit()
            connection.close()

        #insert StrategyName into tbStrategy
        with engine.connect() as connection:
            Strategy_Name = workbook.split(".")
            # Given string
            file_name = workbook

        # Define a regular expression pattern to match the date in the format YYYY-MM-DD
            date_pattern = r'\d{4}-\d{2}-\d{2}'

        # Use regular expression to search for the date pattern in the string
            match = re.search(date_pattern, file_name)

        # If a match is found, extract the date string
            if match:
                date_str = match.group(0)
                # Convert the date string to a datetime object
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            else:
                print("No date in filename")
            strat_code = workbook.split("_")
            Strat_Insert = tbStrategy.insert().values(**{
                "StrategyCode": strat_code[0],
                "StrategyName": Strategy_Name[0],
                "StrategyTestDate": date_obj,
            }
            )
            connection.execute(Strat_Insert)
            connection.commit()
            connection.close()

        #insert PropertiesID and StrategyID into tbSummary
        #get propID
        with engine.connect() as connection:
            latest_row_queryProperties = select(tbProperties.c.PropertiesID).order_by(tbProperties.c.PropertiesID.desc()).limit(1)
            prop = connection.execute(latest_row_queryProperties)
            property_ID = []
            for j in prop:
                property_ID.append(j[0])
            
            connection.close()

        #get startID
        with engine.connect() as connection:
            latest_row_queryStrategy = select(tbStrategy.c.StrategyID).order_by(tbStrategy.c.StrategyID.desc()).limit(1)
            strat = connection.execute(latest_row_queryStrategy)
            strategy_ID = []
            for j in strat:
                strategy_ID.append(j[0])
            connection.close()

        with engine.connect() as connection:
            insertProp = tbSummary.insert().values(**{
                "PropertiesID": property_ID[0],
                "StrategyID": strategy_ID[0],
                "Currency": "USD",
                "NetProfit": summary_data['Net Profit'],
                "GrossProfit": summary_data['Gross Profit'],
                "GrossLoss": summary_data['Gross Loss'],
                "MaxRun_Up": summary_data['Max Run-up'],
                "MaxDrawdown": summary_data['Max Drawdown'],
                "BuyHoldReturn": summary_data['Buy & Hold Return'],
                "SharpeRatio": summary_data['Sharpe Ratio'],
                "SortinoRatio": summary_data['Sortino Ratio'],
                "ProfitFactor": summary_data['Profit Factor'],
                "MaxContractsHeld": summary_data['Max Contracts Held'],
                "OpenPL": summary_data['Open PL'],
                "CommissionPaid": summary_data['Commission Paid'],
                "TotalClosedTrades": summary_data['Total Closed Trades'],
                "TotalOpenTrades": summary_data['Total Open Trades'],
                "NumberWinningTrades": summary_data['Number Winning Trades'],
                "NumberLosingTrades": summary_data['Number Losing Trades'],
                "PercentProfitable": summary_data['Percent Profitable'],
                "AvgTrade": summary_data['Avg Trade'],
                "AvgWinningTrade": summary_data['Avg Winning Trade'],
                "AvgLosingTrade": summary_data['Avg Losing Trade'],
                "RatioAvgWinAvgLoss": summary_data['Ratio Avg Win / Avg Loss'],
                "LargestWinningTrade": summary_data['Largest Winning Trade'],
                "LargestLosingTrade": summary_data['Largest Losing Trade'],
                "AvgNumBarsinTrades": summary_data['Avg # Bars in Trades'],
                "AvgNumBarsinWinningTrades": summary_data['Avg # Bars in Winning Trades'],
                "AvgNumBarsinLosingTrades": summary_data['Avg # Bars in Losing Trades'],
                "MarginCalls": summary_data['Margin Calls']
            })
            connection.execute(insertProp)
            connection.commit()
            connection.close()
        shutil.move(workbook_path, "/home/appcoder/excel_saved")



