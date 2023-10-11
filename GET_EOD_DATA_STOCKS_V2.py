import json
import pandas as pd
pd.set_option('display.max_columns', None)
desired_width = 320
pd.set_option('display.width', desired_width)
import requests
from eodhd import APIClient
from EOD_API_KEY import API_KEY
import openpyxl
import os
import pickle

# OUTPUT PICKLE

def GET_EOD_DATA_STOCKS(
        EXCHANGE,
        OLDEST_DATE,
        TODAYS_DATE,
        LOOKBACK_YEARS,
        TIMEFRAME,
        LIST_NAME
        ):
    # FROM TO DATES - 1_YEAR / 10_YEAR / 15_YEAR / 20_YEAR
    LOOKBACK = LOOKBACK_YEARS
    FROM = OLDEST_DATE  # Oldest Date
    TO = TODAYS_DATE

    # EXCHANGE
    exchange = EXCHANGE

    # TXT IMPORT
    txt_file_DIR = f'C:\\DATA\\SCAN_LISTS\\{exchange}\\'

    # TOP, EARNINGS_TODAY_BMO, EARNINGS_YESTERDAY_AMC, EARNINGS_YESTERDAY_BMO
    txt_name_BASE_list = [f'{LIST_NAME}']

    # PICKLE OUTPUT
    pickle_output_DIR = f'C:\\DATA\\STOCKS\\{exchange}\\{LOOKBACK}\\PICKLE\\'

    # Load library
    api = APIClient(API_KEY)

    # URL: GET EOD DATA
    EOD_URL = "https://eodhistoricaldata.com/api/eod/"

    # URL PARAMS
    PARAMS = {
        "fmt": "json",
        "period": f"{TIMEFRAME}",
        "from": FROM,
        "to": TO,
        "order": "d",
        "api_token": API_KEY
    }


    # Get list of tickers from TXT
    STOCK_TICKERS_LIST = []

    # Open txt files in TXT list and add tickers to ticker list
    for ticker_list_name in txt_name_BASE_list:
        print(ticker_list_name)
        FULL_txt_path = txt_file_DIR + ticker_list_name + '.txt'
        # Open txt file
        open_txt = open(FULL_txt_path, 'r')

        for line in open_txt:
            stripped = line.strip()
            # print(stripped)
            STOCK_TICKERS_LIST.append(stripped)
        open_txt.close()

    # FOR DEBUGGING OR 1 stock
    # STOCK_TICKERS_LIST = ['NVDA']
    # print(STOCK_TICKERS_LIST)

    # Create consolidated list to not download duplicate tickers
    consolidated_list = []
    for ticker in STOCK_TICKERS_LIST:
        if ticker not in consolidated_list:
            consolidated_list.append(ticker)

    print(consolidated_list)

    # Get length of list
    len_of_list = len(consolidated_list)

    # Create counter
    counter = 0

    # Open Session
    S = requests.Session()


    # Start for LOOP
    for ticker in consolidated_list:
        # increment counter
        counter += 1
        ticker_counter_string = f"{counter}/{len_of_list} - {ticker}"

        # print(ticker)
        REQUEST_URL = f"{EOD_URL}{ticker}.{exchange}"
        # print(REQUEST_URL)

        if TIMEFRAME == 'd':
            # PICKLE FILE NAME
            file_name = f'EOD_{ticker}_{exchange}'


        if TIMEFRAME == 'w':
            # PICKLE FILE NAME
            file_name = f'EOW_{ticker}_{exchange}'

        if TIMEFRAME == 'm':
            # PICKLE FILE NAME
            file_name = f'EOM_{ticker}_{exchange}'

        try:
            request = S.get(REQUEST_URL, params=PARAMS)

            # Request URL
            request_url = request.url
            # print(request_url)

            # Status Code
            status_code = request.status_code
            status_string = f"Status code: {status_code}"


            # Log Status code if NOT == 200
            if status_code != 200:
                log_dir = f'C:\\DATA\\STOCKS\\{exchange}\\{LOOKBACK}\\LOGS.txt'
                with open(log_dir, 'w') as LOG:
                    LOG.write(f"{ticker} - {counter}/{len_of_list}" + '\n')
                    LOG.write(request_url + '\n')
                    LOG.write(f"Status code: {status_code}" + '\n')

            # json data
            json_data = request.json()
            # print(json_data)

            # Pretty json data
            json_file = json.dumps(json_data)
            # print(json_file)

            # Json to DataFrame
            DF = pd.read_json(json_file)

            # PRINT MOST CURRENT DATE
            DATE = DF.loc[0, 'date']
            print(f"{status_string} - {DATE} - {ticker_counter_string}")

            # Save PickleFile
            FULL_PICKLE_DIR = pickle_output_DIR + file_name + '.pkl'
            with open(FULL_PICKLE_DIR, 'wb') as f:
                pickle.dump(DF, f)

        except Exception as e:
            print(e)

