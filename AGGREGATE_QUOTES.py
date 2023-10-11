import json
import os
from pytz import timezone
from datetime import datetime
import pandas as pd
pd.set_option('display.max_columns', None)
desired_width = 320
pd.set_option('display.width', desired_width)
import pickle
import numpy as np
import matplotlib.pyplot as plt

# TRADE DATE STRING
trade_date_str = "2023-09-22"

# FILE NAME TYPES
# FILENAMES_Q_T = ['Quotes', 'Trades']
FILENAMES_Q_T = ['Quotes']



for file_type in FILENAMES_Q_T:

    # QUOTES
    if file_type == 'Quotes':
        # file_type -> upper
        file_type_upper = file_type.upper()

        # SPLIT PICKLE DF INPUT
        INPUT_PICKLE_DIR = f"C:\\DATA\\WEBSOCKETS\\SPLIT_DF\\{file_type_upper}\\STOCKS\\US\\PICKLE\\"

        # LIST OF FILES IN DIR
        DF_FILE_LIST = os.listdir(INPUT_PICKLE_DIR)

        # CREATE LIST TO COLLECT DF's FOR MASTER DF
        DF_COLLECTOR_LIST = []

        # OPEN DF FILES
        for DF_FILE in DF_FILE_LIST:
            # print(DF_FILE)

            # PKL FILE TO DF
            DF = pd.read_pickle(INPUT_PICKLE_DIR + DF_FILE)
            # print(DF.head())

            # FILTER AND SORT DF
            DF_SORTED = DF

            # CONVERT TIMESTAMP TO DATETIME
            DF_SORTED['Datetime'] = pd.to_datetime(DF_SORTED['t'], unit='ms')

            # SORT BY TIME
            DF_SORTED = DF_SORTED.sort_values(by=['t'], ascending=True)

            # FORMAT DAY AND TIME
            DF_SORTED['Day'] = DF_SORTED['Datetime'].dt.strftime('%Y-%m-%d')
            DF_SORTED['H:M:S'] = DF_SORTED['Datetime'].dt.strftime('%H:%M:%S')

            # DROP COLUMNS
            DF_SORTED = DF_SORTED.drop(['t', 'Datetime'], axis=1)
            # DROP INDEX COLUMN
            DF_SORTED = DF_SORTED.reset_index(drop=True)


            # DF_SORTED['Day'] = DF_SORTED['Datetime'].dt.strftime('%Y-%m-%d')
            # DF_SORTED['Hour'] = DF_SORTED['Datetime'].dt.strftime('%H')
            # DF_SORTED['Hour'] = DF_SORTED['Hour'].astype(int) + 2  # CONVERTING FROM UTC TO POLAND TIME
            # DF_SORTED['Minute'] = DF_SORTED['Datetime'].dt.strftime('%M')
            # DF_SORTED['Second'] = DF_SORTED['Datetime'].dt.strftime('%S.%f')

            DF_SORTED.rename(columns={'s': 'Ticker'}, inplace=True)
            DF_SORTED.rename(columns={'ap': 'Ask_Price'}, inplace=True)
            DF_SORTED.rename(columns={'bp': 'Bid_Price'}, inplace=True)
            DF_SORTED.rename(columns={'as': 'Ask_Size'}, inplace=True)
            DF_SORTED.rename(columns={'bs': 'Bid_Size'}, inplace=True)

            # print(DF_SORTED.head(10))

            ### -- GROUP BY-- ###
            GROUP_SECONDS = DF_SORTED.groupby('H:M:S', as_index=False)


            #### -- BID SIZE --
            # BID SIZE - MIN
            BID_SIZE_MIN = GROUP_SECONDS[['Bid_Size']].agg(np.min)
            BID_SIZE_MIN.rename(columns={'Bid_Size': 'Min_Bid_Size'}, inplace=True)

            # ASK SIZE - MEAN
            BID_SIZE_MEAN = GROUP_SECONDS[['Bid_Size']].agg(np.mean)
            BID_SIZE_MEAN.rename(columns={'Bid_Size': 'Mean_Bid_Size'}, inplace=True)

            # ASK SIZE - MAX
            BID_SIZE_MAX = GROUP_SECONDS[['Bid_Size']].agg(np.max)
            BID_SIZE_MAX.rename(columns={'Bid_Size': 'Max_Bid_Size'}, inplace=True)

            # ASK SIZE - SUM
            BID_SIZE_SUM = GROUP_SECONDS[['Bid_Size']].agg(np.sum)
            BID_SIZE_SUM.rename(columns={'Bid_Size': 'Total_Bid_Size'}, inplace=True)



            ### -- ASK SIZE --
            # ASK SIZE - MIN
            ASK_SIZE_MIN = GROUP_SECONDS[['Ask_Size']].agg(np.min)
            ASK_SIZE_MIN.rename(columns={'Ask_Size': 'Min_Ask_Size'}, inplace=True)

            # ASK SIZE - MEAN
            ASK_SIZE_MEAN = GROUP_SECONDS[['Ask_Size']].agg(np.mean)
            ASK_SIZE_MEAN.rename(columns={'Ask_Size': 'Mean_Ask_Size'}, inplace=True)

            # ASK SIZE - MAX
            ASK_SIZE_MAX = GROUP_SECONDS[['Ask_Size']].agg(np.max)
            ASK_SIZE_MAX.rename(columns={'Ask_Size': 'Max_Ask_Size'}, inplace=True)

            # ASK SIZE - SUM
            ASK_SIZE_SUM = GROUP_SECONDS[['Ask_Size']].agg(np.sum)
            ASK_SIZE_SUM.rename(columns={'Ask_Size': 'Total_Ask_Size'}, inplace=True)




            #### -- BID PRICE --
            # BID PRICE - MIN
            BID_PRICE_MIN = GROUP_SECONDS[['Bid_Price']].agg(np.min)
            BID_PRICE_MIN.rename(columns={'Bid_Price': 'Min_Bid_Price'}, inplace=True)

            # BID PRICE - MEAN
            BID_PRICE_MEAN = GROUP_SECONDS[['Bid_Price']].agg(np.mean)
            BID_PRICE_MEAN.rename(columns={'Bid_Price': 'Mean_Bid_Price'}, inplace=True)

            # BID PRICE - MAX
            BID_PRICE_MAX = GROUP_SECONDS[['Bid_Price']].agg(np.max)
            BID_PRICE_MAX.rename(columns={'Bid_Price': 'Max_Bid_Price'}, inplace=True)


            ### -- ASK PRICE --
            # ASK PRICE - MIN
            ASK_PRICE_MIN = GROUP_SECONDS[['Ask_Price']].agg(np.min)
            ASK_PRICE_MIN.rename(columns={'Ask_Price': 'Min_Ask_Price'}, inplace=True)

            # ASK PRICE - MEAN
            ASK_PRICE_MEAN = GROUP_SECONDS[['Ask_Price']].agg(np.mean)
            ASK_PRICE_MEAN.rename(columns={'Ask_Price': 'Mean_Ask_Price'}, inplace=True)

            # ASK PRICE - MAX
            ASK_PRICE_MAX = GROUP_SECONDS[['Ask_Price']].agg(np.max)
            ASK_PRICE_MAX.rename(columns={'Ask_Price': 'Max_Ask_Price'}, inplace=True)



            #-- CREATE FINAL DF
            # GET TICKER SYMBOL
            TICKER = DF_SORTED.loc[0, 'Ticker']

            # CREATE FINAL COLUMN NAMES
            FINAL_DF = pd.DataFrame(columns=['H:M:S',
                                             'Mean_Size_Ratio_Bid',
                                             'Max_Size_Ratio_Bid',
                                             'Total_Size_Ratio_Bid',
                                             'Mean_Size_Ratio_Ask',
                                             'Max_Size_Ratio_Ask',
                                             'Total_Size_Ratio_Ask',
                                             'Min_Bid_Size',
                                             'Mean_Bid_Size',
                                             'Max_Bid_Size',
                                             'Total_Bid_Size',
                                             'TBS_Z_Score',
                                             'TAS_Z_Score',
                                             'TBS-Z_TAS-Z_Difference',
                                             'TBS-Z_TAS-Z_Difference_Mean',
                                             'TBS-Z_TAS-Z_Difference_SD',
                                             'TBS-Z_TAS-Z_Difference_Z',
                                             'Min_Ask_Size',
                                             'Mean_Ask_Size',
                                             'Max_Ask_Size',
                                             'Total_Ask_Size',
                                             'Min_Bid_Price',
                                             'Mean_Bid_Price',
                                             'Max_Bid_Price',
                                             'Min_Ask_Price',
                                             'Mean_Ask_Price',
                                             'Max_Ask_Price'
                                             ])

            # TIME COLUMN
            FINAL_DF['H:M:S'] = ASK_SIZE_MIN['H:M:S']

            # BID SIZE COLUMNS
            FINAL_DF['Min_Bid_Size'] = BID_SIZE_MIN['Min_Bid_Size']
            FINAL_DF['Mean_Bid_Size'] = BID_SIZE_MEAN['Mean_Bid_Size'].apply(np.int64)  # CONVERT TO INT
            FINAL_DF['Max_Bid_Size'] = BID_SIZE_MAX['Max_Bid_Size']
            FINAL_DF['Total_Bid_Size'] = BID_SIZE_SUM['Total_Bid_Size']

            # ASK SIZE COLUMNS
            FINAL_DF['Min_Ask_Size'] = ASK_SIZE_MIN['Min_Ask_Size']
            FINAL_DF['Mean_Ask_Size'] = ASK_SIZE_MEAN['Mean_Ask_Size'].apply(np.int64)  # CONVERT TO INT
            FINAL_DF['Max_Ask_Size'] = ASK_SIZE_MAX['Max_Ask_Size']
            FINAL_DF['Total_Ask_Size'] = ASK_SIZE_SUM['Total_Ask_Size']


            PERIOD = 60  # EQUIVALENT TO 1 MIN
            SHIFT = 1  # EXCLUDES CURRENT VALUE FROM CALC

            # TOTAL BID SIZE - ROLLING MEAN
            FINAL_DF['TBS_Rolling_Mean'] = FINAL_DF['Total_Bid_Size'].rolling(PERIOD).mean().shift(SHIFT)
            # TOTAL BID SIZE - STANDARD DEVIATION
            FINAL_DF['TBS_Standard_Deviation'] = FINAL_DF['Total_Bid_Size'].rolling(PERIOD).std().shift(SHIFT)
            # TOTAL BID SIZE - Z-SCORE
            FINAL_DF['TBS_Z_Score'] = np.subtract(FINAL_DF['Total_Bid_Size'], FINAL_DF['TBS_Rolling_Mean']) / FINAL_DF['TBS_Standard_Deviation']
            FINAL_DF['TBS_Z_Score'] = np.round(FINAL_DF['TBS_Z_Score'], 2)

            # TOTAL ASK SIZE - ROLLING MEAN
            FINAL_DF['TAS_Rolling_Mean'] = FINAL_DF['Total_Ask_Size'].rolling(PERIOD).mean().shift(SHIFT)
            # TOTAL ASK SIZE - STANDARD DEVIATION
            FINAL_DF['TAS_Standard_Deviation'] = FINAL_DF['Total_Ask_Size'].rolling(PERIOD).std().shift(SHIFT)
            # TOTAL ASK SIZE - Z-SCORE
            FINAL_DF['TAS_Z_Score'] = np.subtract(FINAL_DF['Total_Ask_Size'], FINAL_DF['TAS_Rolling_Mean']) / FINAL_DF['TAS_Standard_Deviation']
            FINAL_DF['TAS_Z_Score'] = np.round(FINAL_DF['TAS_Z_Score'], 2)

            # NEW COLUMN: TBS-Z_TAS-Z_Difference
            FINAL_DF['TBS-Z_TAS-Z_Difference'] = np.subtract(FINAL_DF['TAS_Z_Score'], FINAL_DF['TBS_Z_Score'])

            # TBS-Z_TAS-Z_Difference_Mean
            FINAL_DF['TBS-Z_TAS-Z_Difference_Mean'] = FINAL_DF['TBS-Z_TAS-Z_Difference'].rolling(PERIOD).mean().shift(SHIFT)
            # TBS-Z_TAS-Z_Difference_SD
            FINAL_DF['TBS-Z_TAS-Z_Difference_SD'] = FINAL_DF['TBS-Z_TAS-Z_Difference'].rolling(PERIOD).std().shift(SHIFT)
            # TBS-Z_TAS-Z_Difference_Z
            FINAL_DF['TBS-Z_TAS-Z_Difference_Z'] = np.subtract(FINAL_DF['TBS-Z_TAS-Z_Difference'], FINAL_DF['TBS-Z_TAS-Z_Difference_Mean']) / FINAL_DF['TBS-Z_TAS-Z_Difference_SD']


            # ASK PRICE COLUMNS
            FINAL_DF['Min_Ask_Price'] = ASK_PRICE_MIN['Min_Ask_Price']
            FINAL_DF['Mean_Ask_Price'] = np.round(ASK_PRICE_MEAN['Mean_Ask_Price'], 3)
            FINAL_DF['Max_Ask_Price'] = ASK_PRICE_MAX['Max_Ask_Price']

            # BID PRICE COLUMNS
            FINAL_DF['Min_Bid_Price'] = BID_PRICE_MIN['Min_Bid_Price']
            FINAL_DF['Mean_Bid_Price'] = np.round(BID_PRICE_MEAN['Mean_Bid_Price'], 3)
            FINAL_DF['Max_Bid_Price'] = BID_PRICE_MAX['Max_Bid_Price']

            ### --- RATIO COLUMNS
            FINAL_DF['Mean_Size_Ratio_Bid'] = np.divide(FINAL_DF['Mean_Bid_Size'], FINAL_DF['Mean_Ask_Size']).apply(np.int64)  # CONVERT TO INT
            FINAL_DF['Max_Size_Ratio_Bid'] = np.divide(FINAL_DF['Max_Bid_Size'], FINAL_DF['Max_Ask_Size']).apply(np.int64)  # CONVERT TO INT
            FINAL_DF['Total_Size_Ratio_Bid'] = np.divide(FINAL_DF['Total_Bid_Size'], FINAL_DF['Total_Ask_Size']).apply(np.int64)  # CONVERT TO INT

            FINAL_DF['Mean_Size_Ratio_Ask'] = np.divide(FINAL_DF['Mean_Ask_Size'], FINAL_DF['Mean_Bid_Size']).apply(np.int64)  # CONVERT TO INT
            FINAL_DF['Max_Size_Ratio_Ask'] = np.divide(FINAL_DF['Max_Ask_Size'], FINAL_DF['Max_Bid_Size']).apply(np.int64)  # CONVERT TO INT
            FINAL_DF['Total_Size_Ratio_Ask'] = np.divide(FINAL_DF['Total_Ask_Size'], FINAL_DF['Total_Bid_Size']).apply(np.int64)  # CONVERT TO INT


            # DROP COLUMNS
            FINAL_DF = FINAL_DF.drop(['TBS_Rolling_Mean', 'TBS_Standard_Deviation', 'TAS_Rolling_Mean', 'TAS_Standard_Deviation'], axis=1)


            print(FINAL_DF.head(20))

            # DF TO EXCEL
            FULL_EXCEL_PATH = f"C:\\DATA\\WEBSOCKETS\\AGGREGATE\\{file_type_upper}\\{TICKER}_Aggregate_Quotes_{trade_date_str}.xlsx"
            FINAL_DF.to_excel(rf"{FULL_EXCEL_PATH}", index=False)


    # TRADES
    if file_type == 'Trades':
        # SPLIT PICKLE DF INPUT
        INPUT_PICKLE_DIR = "C:\\DATA\\WEBSOCKETS\\SPLIT_DF\\TRADES\\STOCKS\\US\\PICKLE\\"

        # LIST OF FILES IN DIR
        DF_FILE_LIST = os.listdir(INPUT_PICKLE_DIR)

        # CREATE LIST TO COLLECT DF's FOR MASTER DF
        DF_COLLECTOR_LIST = []

        # OPEN DF FILES
        for DF_FILE in DF_FILE_LIST:
            # print(DF_FILE)

            # PKL FILE TO DF
            DF = pd.read_pickle(INPUT_PICKLE_DIR + DF_FILE)

            # FILTER AND SORT DF
            DF_SORTED = DF.loc[DF['dp'] == True].sort_values(by=['v'], ascending=False)

            # CONVERT TIMESTAMP TO DATETIME
            DF_SORTED['Datetime'] = pd.to_datetime(DF_SORTED['t'], unit='ms')

            DF_SORTED['Day'] = DF_SORTED['Datetime'].dt.strftime('%Y-%m-%d')
            DF_SORTED['Hour'] = DF_SORTED['Datetime'].dt.strftime('%H')
            DF_SORTED['Hour'] = DF_SORTED['Hour'].astype(int) + 2  # CONVERTING FROM UTC TO POLAND TIME
            DF_SORTED['Minute'] = DF_SORTED['Datetime'].dt.strftime('%M')
            DF_SORTED['Second'] = DF_SORTED['Datetime'].dt.strftime('%S.%f')

            DF_SORTED.rename(columns={'s': 'Ticker'}, inplace=True)
            DF_SORTED.rename(columns={'p': 'Price'}, inplace=True)
            DF_SORTED.rename(columns={'c': 'Condition'}, inplace=True)
            DF_SORTED.rename(columns={'v': 'Volume'}, inplace=True)
            DF_SORTED.rename(columns={'dp': 'Dark_Pool'}, inplace=True)
            DF_SORTED.rename(columns={'ms': 'Market_Status'}, inplace=True)

            # DROP COLUMNS
            DF_SORTED = DF_SORTED.drop(['t', 'Datetime'], axis=1)
            # DROP INDEX COLUMN
            DF_SORTED = DF_SORTED.reset_index(drop=True)
            # print(DF_SORTED.info())

            # PRINT HEAD()
            print('\n')
            print(DF_SORTED.head(10).to_string(index=False))

            # ADD DF TO LIST
            TOP_10_ROWS_DF = DF_SORTED.head(10)
            DF_COLLECTOR_LIST.append(TOP_10_ROWS_DF)

        # CREATE MASTER DF
        FINAL_MASTER_DF = pd.concat(DF_COLLECTOR_LIST)
        print(FINAL_MASTER_DF)

        # DF TO EXCEL
        FULL_EXCEL_PATH = f"C:\\DATA\\WEBSOCKETS\\MASTER_DF\\MASTER_DP_TRADES_{trade_date_str}.xlsx"
        FINAL_MASTER_DF.to_excel(rf"{FULL_EXCEL_PATH}", index=False)










