import websocket
import ssl
import json
from EOD_API_KEY import API_KEY
import time as t
from datetime import datetime
import time as ts
import multiprocessing as mp

# TIME
CLOSE_TIME = "09:00"
start_time_ts = ts.time()
start_time = datetime.now()
start_time_str = start_time.strftime("%H:%M")
socket_type = 'wss'  # ws= no ssl; wss= ssl
def RECONNECT_TRADES(TICKER_LIST):
    # URL
    TRADES_URL = f"{socket_type}://ws.eodhistoricaldata.com/ws/crypto?api_token={API_KEY}"

    # CREATE CONNECTION
    ws_t = websocket.create_connection(TRADES_URL)


    # SUBSCRIBE TO TRADES
    for TICKER in TICKER_LIST:
        # JSON SUBSCRIPTION MESSAGES
        SUBSCRIBE = json.dumps({"action": "subscribe", "symbols": f"{TICKER}"})

        # SUBSCRIBE TO STREAMING DATA
        ws_t.send(SUBSCRIBE)


    # 1 - TRADES LOGIN
    LOGIN_CHECK_STATUS = False
    try:
        trades_reply = ws_t.recv()
        decoded_trades_reply = json.loads(trades_reply)
        print(decoded_trades_reply)

    except Exception as e:
        print(f"Trades Login exception: {e}")

    # LOGIN STATUS CHECK
    if decoded_trades_reply["status_code"] == 200:
        TRADES_RECONNECT_STATUS = True
    else:
        TRADES_RECONNECT_STATUS = False
        print(f"Trades Login Status Check: {LOGIN_CHECK_STATUS}")

    return [TRADES_RECONNECT_STATUS, ws_t]


def GET_TRADES(TICKER_LIST, TODAYS_DATE_STR, LOG_FILE_DIR):
    # DEBUG DISCONNECTION
    DISCONNECTED_ONCE = False

    # URL
    TRADES_URL = f"{socket_type}://ws.eodhistoricaldata.com/ws/crypto?api_token={API_KEY}"

    # CREATE CONNECTION
    ws_t = websocket.create_connection(TRADES_URL)


    # CREATE JSON FILE TO STORE T&S DATA
    TRADES_DIR_OUT = "C:\\DATA\\WEBSOCKETS\\TRADES\\CRYPTO\\JSON\\"

    FILE_TRADES_NAME = f"Trades_{TODAYS_DATE_STR}.json"

    FULL_DIR2 = TRADES_DIR_OUT + FILE_TRADES_NAME

    JSON_TRADES_LIST = []

    # OPEN FILE
    JSON_TRADES_FILE = open(FULL_DIR2, 'a')


    # SUBSCRIBE TO TRADES
    for TICKER in TICKER_LIST:
        # JSON SUBSCRIPTION MESSAGES
        SUBSCRIBE = json.dumps({"action": "subscribe", "symbols": f"{TICKER}"})

        # SUBSCRIBE TO STREAMING DATA
        ws_t.send(SUBSCRIBE)



    # 1 - Trades Login
    LOGIN_CHECK_STATUS = False
    try:
        trades_reply = ws_t.recv()
        decoded_trades_reply = json.loads(trades_reply)

    except Exception as e:
        print(f"Trades Login exception: {e}")


    # LOGIN STATUS CHECK
    if decoded_trades_reply["status_code"] == 200:
        LOGIN_CHECK_STATUS = True
        print(f"Trades Login Status: OK")
    else:
        print(f"Trades Login Status: {LOGIN_CHECK_STATUS}")


    # 2 - START STREAMING
    time = datetime.now()
    time_str = time.strftime("%H:%M")
    while time_str != CLOSE_TIME and LOGIN_CHECK_STATUS:
    # while timer != 0 and LOGIN_CHECK_STATUS:  # DEBUGGING
        # GET RESPONSE - Trades
        try:
            trades_reply = ws_t.recv()
            decoded_trades_reply = json.loads(trades_reply)

            print(decoded_trades_reply)
            JSON_TRADES_LIST.append(decoded_trades_reply)

        # ERROR HANDLING
        except websocket.WebSocketException as e:
            # GET TIME
            time = datetime.now()
            time_str = time.strftime("%H:%M:%S")

            Trades_error_string = f"{time_str} - Websocket Exception: {e}"
            print(Trades_error_string)

            # TRY TO RECONNECT
            # WAIT 5 seconds
            t.sleep(5)
            RECONNECT_STATUS = RECONNECT_TRADES(TICKER_LIST)[0]
            ws_t = RECONNECT_TRADES(TICKER_LIST)[1]

            reconnect_string = f"{time_str} - Trades reconnect Status: " + str(RECONNECT_STATUS)
            print(reconnect_string)

            # LOG ERROR
            with open(LOG_FILE_DIR, 'a') as LOG:
                LOG.write(Trades_error_string + '\n')
                LOG.write(reconnect_string + '\n')

        except websocket.WebSocketConnectionClosedException as e:
            # GET TIME
            time = datetime.now()
            time_str = time.strftime("%H:%M:%S")

            Trades_error_string = f"{time_str} - Websocket Connection closed exception: {e}"
            print(Trades_error_string)

            # TRY TO RECONNECT
            # WAIT 5 seconds
            t.sleep(5)
            RECONNECT_STATUS = RECONNECT_TRADES(TICKER_LIST)[0]
            ws_t = RECONNECT_TRADES(TICKER_LIST)[1]

            reconnect_string = f"{time_str} - Trades reconnect Status: " + str(RECONNECT_STATUS)
            print(reconnect_string)

            # LOG ERROR
            with open(LOG_FILE_DIR, 'a') as LOG:
                LOG.write(Trades_error_string + '\n')
                LOG.write(reconnect_string + '\n')


        except websocket.WebSocketBadStatusException as e:
            # GET TIME
            time = datetime.now()
            time_str = time.strftime("%H:%M:%S")

            Trades_error_string = f"{time_str} - Bad Status Exception: {e}"
            print(Trades_error_string)

            # TRY TO RECONNECT
            # WAIT 5 seconds
            t.sleep(5)
            RECONNECT_STATUS = RECONNECT_TRADES(TICKER_LIST)[0]
            ws_t = RECONNECT_TRADES(TICKER_LIST)[1]

            reconnect_string = f"{time_str} - Trades reconnect Status: " + str(RECONNECT_STATUS)
            print(reconnect_string)


        except EOFError as e:
            # GET TIME
            time = datetime.now()
            time_str = time.strftime("%H:%M:%S")

            Trades_error_string = f"{time_str} - EOF ERROR: {e}"
            print(Trades_error_string)

            # TRY TO RECONNECT
            # WAIT 5 seconds
            t.sleep(5)
            RECONNECT_STATUS = RECONNECT_TRADES(TICKER_LIST)[0]
            ws_t = RECONNECT_TRADES(TICKER_LIST)[1]

            reconnect_string = f"{time_str} - Trades reconnect Status: " + str(RECONNECT_STATUS)
            print(reconnect_string)

            # LOG ERROR
            with open(LOG_FILE_DIR, 'a') as LOG:
                LOG.write(Trades_error_string + '\n')
                LOG.write(reconnect_string + '\n')

        except ssl.SSL_ERROR_EOF as e:
            # GET TIME
            time = datetime.now()
            time_str = time.strftime("%H:%M:%S")

            Trades_error_string = f"{time_str} - SSL ERROR EOF: {e}"
            print(Trades_error_string)

            # TRY TO RECONNECT
            # WAIT 5 seconds
            t.sleep(5)
            RECONNECT_STATUS = RECONNECT_TRADES(TICKER_LIST)[0]
            ws_t = RECONNECT_TRADES(TICKER_LIST)[1]

            reconnect_string = f"{time_str} - Trades reconnect Status: " + str(RECONNECT_STATUS)
            print(reconnect_string)

            # LOG ERROR
            with open(LOG_FILE_DIR, 'a') as LOG:
                LOG.write(Trades_error_string + '\n')
                LOG.write(reconnect_string + '\n')




        # SIMULATE DISCONNECTION
        # if DISCONNECTED_ONCE == False:
        #     ws_t.close()
        #     DISCONNECTED_ONCE = True

        # GET TIME
        time = datetime.now()
        time_str = time.strftime("%H:%M")


    # Output TRADES JSON file
    JSON_TRADES_FILE.write(json.dumps(JSON_TRADES_LIST, indent=4))
    print("Trades Json Saved to Disk")

    # CLOSE TRADES JSON FILE
    JSON_TRADES_FILE.close()

    end_time_ts = ts.time()
    elapsed_time_ts = (end_time_ts - start_time_ts)
    print(f"Elapsed time: {elapsed_time_ts} seconds.")


# START PROGRAM
if __name__ == '__main__':
    # TODAYS DATE
    TODAYS_DATE = datetime.now()
    TODAYS_DATE_STR = TODAYS_DATE.strftime("%Y-%m-%d")

    # LOG FILE DIR
    LOG_FILE_DIR = 'C:\\DATA\\WEBSOCKETS\\LOGS\\log.txt'

    # GET LIST
    txt_file_DIR = 'C:\\DATA\\WEBSOCKETS\\SCAN_LIST\\'
    txt_file_name = 'CRYPTO'
    FULL_txt_path = txt_file_DIR + txt_file_name + '.txt'

    # CREATE LIST
    TICKER_LIST = []

    # OPEN TICKER LIST
    open_list = open(FULL_txt_path, 'r')
    for line in open_list:
        stripped = line.strip()
        TICKER_LIST.append(stripped)
    open_list.close()

    # DEBUGGING LIST
    # TICKER_LIST = ['BTC-USD']
    print(TICKER_LIST)

    # CREATE MULTIPROCESSOR OBJECTS


    # -- TRADES
    P1_TRADES = mp.Process(target=GET_TRADES, args=(TICKER_LIST, TODAYS_DATE_STR, LOG_FILE_DIR))

    # START MULTIPROCESSORS
    P1_TRADES.start()