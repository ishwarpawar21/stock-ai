import pandas as pd
import pandas_ta as ta
import json
from pathlib import Path
from datetime import datetime, timedelta
import upstox_client
from upstox_client.rest import ApiException
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import os
from pyspark.sql.functions import col, to_timestamp
import pyspark.sql
from kedro.io import DataCatalog
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType



def load_access_token(filepath: str) -> str:
    filepath = Path(filepath)
    
    if not filepath.is_file():
        raise FileNotFoundError(f"Access token file not found at {filepath}")
    
    with open(filepath, 'r') as f:
        access_token_data = json.load(f)
    return access_token_data['access_token']

def fetch_historical_data(access_token: str, instrument_key: str, interval: str) -> json:

    # Get the current date and time
    end_date = datetime.now()

    # Calculate the start date
    start_date = end_date - timedelta(days=179)

    # Format the dates as 'YYYY-MM-DD'
    end_date = end_date.strftime('%Y-%m-%d')
    start_date = start_date.strftime('%Y-%m-%d')
    
    api_version = '2.0'
    api_instance = upstox_client.HistoryApi()
    api_instance.api_client.configuration.access_token = access_token
    try:
        api_response = api_instance.get_historical_candle_data1(instrument_key, interval, end_date, start_date, api_version)
        # Extract candle data from JSON response
        response_dict = api_response.to_dict()
        api_responsejason = json.dumps(response_dict, indent=4)
        return api_responsejason
    except ApiException as e:
        print(f"Exception when calling HistoryApi->get_historical_candle_data for {instrument_key}: {e}\n")
        return api_responsejason
    


def process_and_convert_to_parquet(response_json: str) -> pd.DataFrame:
    # Parse the JSON response
    response_object = json.loads(response_json)
    
    # Extract data from the parsed JSON object
    status = response_object.get("status")
    candles = response_object.get("data", {}).get("candles", [])
    
    if status == "success":
        # Convert the candles data to the correct types
        candles = [
            [
                datetime.strptime(candle[0], "%Y-%m-%dT%H:%M:%S%z"),
                float(candle[1]),
                float(candle[2]),
                float(candle[3]),
                float(candle[4]),
                int(candle[5]),
                int(candle[6])
            ]
        for candle in candles
        ]
        
        # Define column names
        columns = ["Timestamp", "Open", "High", "Low", "Close", "Volume", "Open Interest"]
        
        # Create Pandas DataFrame
        df = pd.DataFrame(candles, columns=columns)
        
        return df

    else:
        raise ValueError("Response status is not successful")


def calculate_dates():
    end_date = datetime.now()
    start_date = end_date - timedelta(days=180)
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')


def calculate_indicators(candles_pd: pd.DataFrame) -> pd.DataFrame:
    # Calculate technical indicators using pandas_ta

    # Calculate technical indicators using pandas_ta

    # Aroon Oscillator
    aroon = ta.aroon(candles_pd['High'], candles_pd['Low'])
    candles_pd['AROON_Up'] = aroon['AROONU_14']  # Assign Aroon Up to 'AROON_Up'
    candles_pd['AROON_Dn'] = aroon['AROOND_14']  # Assign Aroon Down to 'AROON_Dn'
    candles_pd['AROON_Sc'] = aroon['AROONU_14']  # Assign Aroon Oscillator to 'AROON_Sc'

    # Balance of Power (BOP)
    candles_pd['BOP'] = ta.bop(candles_pd['Open'], candles_pd['High'], candles_pd['Low'], candles_pd['Close'])

    # Commodity Channel Index (CCI)
    candles_pd['CCI'] = ta.cci(candles_pd['High'], candles_pd['Low'], candles_pd['Close'])

    # Donchian Channels (DC)
    dc = ta.donchian(candles_pd['High'], candles_pd['Low'])
    candles_pd['DC_upper'] = dc['DCU_20_20']
    candles_pd['DC_middle'] = dc['DCM_20_20']
    candles_pd['DC_lower'] = dc['DCL_20_20']

    # Exponential Moving Average (EMA)
    candles_pd['EMA'] = ta.ema(candles_pd['Close'],window=14)

    # Relative Strength Index (RSI)
    candles_pd['RSI'] = ta.rsi(candles_pd['Close'],window=14)

    # Stochastic Oscillator
    stoch = ta.stoch(candles_pd['High'], candles_pd['Low'], candles_pd['Close'])
    candles_pd['STOCH_k'] = stoch['STOCHk_14_3_3']
    candles_pd['STOCH_d'] = stoch['STOCHd_14_3_3']

    # Calculate Ichimoku Cloud
    ichimoku_df = ta.ichimoku(candles_pd['High'], candles_pd['Low'], candles_pd['Close'])
    ichimoku_df = pd.DataFrame(ichimoku_df[0])
    # Assign the columns to candles_pd
    candles_pd['ICHIMOKU_a'] = ichimoku_df['ISA_9']
    candles_pd['ICHIMOKU_b'] = ichimoku_df['ISB_26']
    candles_pd['ICHIMOKU_base'] = ichimoku_df['ITS_9']
    candles_pd['ICHIMOKU_span'] = ichimoku_df['IKS_26']
    candles_pd['ICHIMOKU_CS'] = ichimoku_df['ICS_26']  # Adding Ichimoku Chikou Span (Lagging Span) column

    candles_pd['KAMA'] = ta.kama(candles_pd['Close'])

    # Bollinger Bands (BBANDS)
    bbands = ta.bbands(candles_pd['Close'],window=14, window_dev=2)
    candles_pd['BBANDS_upper'] = bbands['BBU_5_2.0']
    candles_pd['BBANDS_middle'] = bbands['BBM_5_2.0']
    candles_pd['BBANDS_lower'] = bbands['BBL_5_2.0']

    # Moving Average Convergence Divergence (MACD)
    macd = ta.macd(candles_pd['Close'])
    candles_pd['MACD'] = macd['MACD_12_26_9']
    candles_pd['MACD_signal'] = macd['MACDs_12_26_9']
    candles_pd['MACD_diff'] = macd['MACDh_12_26_9']

    # Volume-related indicators (commented out since Volume data is not provided)
    # candles_pd['VWAP'] = ta.vwap(candles_pd['High'], candles_pd['Low'], candles_pd['Close'], candles_pd['Volume'])
    # candles_pd['ADL'] = ta.adl(candles_pd['High'], candles_pd['Low'], candles_pd['Close'], candles_pd['Volume'])
    # candles_pd['CMF'] = ta.cmf(candles_pd['High'], candles_pd['Low'], candles_pd['Close'], candles_pd['Volume'])
    # candles_pd['EOM'] = ta.eom(candles_pd['High'], candles_pd['Low'], candles_pd['Volume'])
    # candles_pd['FI'] = ta.fi(candles_pd['Close'], candles_pd['Volume'])
    # candles_pd['MFI'] = ta.mfi(candles_pd['High'], candles_pd['Low'], candles_pd['Close'], candles_pd['Volume'])
    # candles_pd['NVI'] = ta.nvi(candles_pd['Close'], candles_pd['Volume'])
    # candles_pd['OBV'] = ta.obv(candles_pd['Close'], candles_pd['Volume'])
    # candles_pd['PVI'] = ta.pvi(candles_pd['Close'], candles_pd['Volume'])
    # candles_pd['VO'] = ta.vo(candles_pd['Close'], candles_pd['Volume'])

    # Additional indicators

    # Chande Momentum Oscillator (CMO)
    candles_pd['CMO'] = ta.cmo(candles_pd['Close'])

    # Directional Price Oscillator (DPO)
    candles_pd['DPO'] = ta.dpo(candles_pd['Close'])

    # Hull Moving Average (HMA)
    candles_pd['HMA'] = ta.hma(candles_pd['Close'])

    # Kaufman's Adaptive Moving Average (KAMA)
    candles_pd['KAMA'] = ta.kama(candles_pd['Close'])

    # Keltner Channels (KC)
    kc = ta.kc(candles_pd['High'], candles_pd['Low'], candles_pd['Close'])
    candles_pd['KC_upper'] = kc['KCUe_20_2']
    candles_pd['KC_middle'] = kc['KCBe_20_2']
    candles_pd['KC_lower'] = kc['KCLe_20_2']

    # Momentum (MOM)
    candles_pd['MOM'] = ta.mom(candles_pd['Close'])

    # Percentage Price Oscillator (PPO)
    ppo = ta.ppo(candles_pd['Close'])
    candles_pd['PPO'] = ppo['PPO_12_26_9']

    # Rate of Change (ROC)
    candles_pd['ROC'] = ta.roc(candles_pd['Close'])

    # Triple Exponential Moving Average (TRIMA)
    candles_pd['TRIMA'] = ta.trima(candles_pd['Close'])

    # Ultimate Oscillator (UO)
    candles_pd['UO'] = ta.uo(candles_pd['High'], candles_pd['Low'], candles_pd['Close'])

    # Williams %R (WILLR)
    candles_pd['WILLR'] = ta.willr(candles_pd['High'], candles_pd['Low'], candles_pd['Close'])

    # Average True Range (ATR)
    candles_pd['ATR'] = ta.atr(candles_pd['High'], candles_pd['Low'], candles_pd['Close'])

    # Simple Moving Average (SMA)
    candles_pd['SMA'] = ta.sma(candles_pd['Close'],window=14)

    # Weighted Moving Average (WMA)
    candles_pd['WMA'] = ta.wma(candles_pd['Close'])

    # Z-Score (Z)
    candles_pd['Z'] = ta.zscore(candles_pd['Close'])

    # Awesome Oscillator (AO)
    candles_pd['AO'] = ta.ao(candles_pd['High'], candles_pd['Low'])

    # Elder Ray Index (ERI)
    eri = ta.eri(candles_pd['High'], candles_pd['Low'], candles_pd['Close'])
    candles_pd['ERI_bull'] = eri['BULLP_13']
    candles_pd['ERI_bear'] = eri['BEARP_13']

    # Fisher Transform
    fisher = ta.fisher(candles_pd['High'], candles_pd['Low'])
    candles_pd['FISHER'] = fisher['FISHERT_9_1']
    candles_pd['FISHER_signal'] = fisher['FISHERTs_9_1']
    candles_pd['High_next'] = candles_pd['High'].shift(-1)
    candles_pd['Low_next'] = candles_pd['Low'].shift(-1)
    print("Values added")
    print("indicators data added")
    # Return the DataFrame with new indicators
    return candles_pd


def feature_engineering(calculate_indicators_df: pd.DataFrame) -> pd.DataFrame:
    # Assuming data is a pandas DataFrame
    calculate_indicators_df['Timestamp'] = pd.to_datetime(calculate_indicators_df['Timestamp'])
    
    # Add weekday number
    calculate_indicators_df['weekday_num'] = calculate_indicators_df['Timestamp'].dt.dayofweek  # Monday=0, Sunday=6
    
    # Add weekday name
    calculate_indicators_df['weekday_name'] = calculate_indicators_df['Timestamp'].dt.day_name()
    
    # Add month number
    calculate_indicators_df['month_num'] = calculate_indicators_df['Timestamp'].dt.month
    
    # Add month name
    calculate_indicators_df['month_name'] = calculate_indicators_df['Timestamp'].dt.month_name()
    
    # Add flag for weekly options expiration (every Thursday)
    calculate_indicators_df['is_weekly_expiration'] = calculate_indicators_df['Timestamp'].dt.dayofweek == 3  # Thursday is 3
    
    # Add flag for monthly options expiration (last Thursday of each month)
    calculate_indicators_df['is_last_thursday'] = calculate_indicators_df['Timestamp'].apply(lambda x: x.is_month_end and x.weekday() == 3)
    calculate_indicators_df['is_monthly_expiration'] = calculate_indicators_df['is_last_thursday'].astype(int)
    
    # add weekly expiry time remaining.. 
    # Drop intermediate columns
    calculate_indicators_df.drop(columns=['is_last_thursday'], inplace=True)
    
    return calculate_indicators_df

def split_data(df, train_start_date, train_end_date, test_start_date, test_end_date):
    train_df = df.filter((col("Timestamp") >= train_start_date) & (col("Timestamp") <= train_end_date))
    test_df = df.filter((col("Timestamp") >= test_start_date) & (col("Timestamp") <= test_end_date))
    return train_df, test_df

def create_train_test_splits(df_cleaned_data, TT_dataset):
    # List of relevant columns based on provided parameters
    relevant_columns = [
        'Timestamp','High', 'Low', 'Close', 'Volume', 'Open',
        'weekday_num','month_num','is_weekly_expiration','is_monthly_expiration',
        'SMA', 'EMA', 'RSI', 
        'MACD', 'MACD_signal', 
        'BBANDS_upper', 'BBANDS_middle','BBANDS_lower',
        'High_next','Low_next'
    ]
    # Subset the DataFrame
    BNF_df_sub = df_cleaned_data.select([col(c) for c in relevant_columns])

    # List of columns to check for null values
    na_columns_to_check = [
        'SMA', 'EMA', 'RSI', 
        'MACD', 'MACD_signal', 
        'BBANDS_upper', 'BBANDS_middle','BBANDS_lower',
        'High_next','Low_next'
    ]

    # Remove rows with null values in the specified columns
    df_cleaned = BNF_df_sub.dropna(subset=na_columns_to_check)

    # Convert 'Timestamp' to timestamp type if not already
    df_cleaned = df_cleaned.withColumn("Timestamp", to_timestamp(col("Timestamp")))
    
    splits = []
    # Define start and end dates for the dataset
    #min_date_row = df_cleaned.agg({"Timestamp": "min"}).collect()[0][0]
    max_date_row = df_cleaned.agg({"Timestamp": "max"}).collect()[0][0]
    
    if TT_dataset =="16w_8w":
        # 1. Split for 16 weeks of training and 8 weeks testing
        train_end = max_date_row - timedelta(weeks=8)
        train_start = train_end - timedelta(weeks=16)
        test_start = train_end
        test_end = max_date_row
        train_df, test_df = split_data(df_cleaned,train_start, train_end, test_start, test_end)
        return train_df, test_df
    
    if TT_dataset =="20w_4w":
        # 2. Split for 20 weeks of training and 4 weeks testing
        train_end = max_date_row - timedelta(weeks=4)
        train_start = train_end - timedelta(weeks=20)
        test_start = train_end
        test_end = max_date_row
        train_df, test_df = split_data(df_cleaned,train_start, train_end, test_start, test_end)
        return train_df, test_df

    if TT_dataset =="22w_2w":
        # 3. Split for 22 weeks of training and 2 weeks testing
        train_end = max_date_row - timedelta(weeks=2)
        train_start = train_end - timedelta(weeks=22)
        test_start = train_end
        test_end = max_date_row
        train_df, test_df = split_data(df_cleaned,train_start, train_end, test_start, test_end)
        return train_df, test_df

    if TT_dataset =="23w_1w":
        # 4. Split for 23 weeks of training and 1 week testing
        train_end = max_date_row - timedelta(weeks=1)
        train_start = train_end - timedelta(weeks=23)
        test_start = train_end
        test_end = max_date_row
        train_df, test_df = split_data(df_cleaned,train_start, train_end, test_start, test_end)
        
        return train_df, test_df
    

def create_train_test_splits_s3(df_cleaned_data, TT_dataset) -> pd.DataFrame:
    # List of relevant columns based on provided parameters
    relevant_columns = [
        'Timestamp', 'High', 'Low', 'Close', 'Volume', 'Open',
        'weekday_num', 'month_num', 'is_weekly_expiration', 'is_monthly_expiration',
        'SMA', 'EMA', 'RSI', 
        'MACD', 'MACD_signal', 
        'BBANDS_upper', 'BBANDS_middle', 'BBANDS_lower',
        'High_next', 'Low_next'
    ]
    
    # Subset the DataFrame
    BNF_df_sub = df_cleaned_data[relevant_columns]

    # List of columns to check for null values
    na_columns_to_check = [
        'SMA', 'EMA', 'RSI', 
        'MACD', 'MACD_signal', 
        'BBANDS_upper', 'BBANDS_middle', 'BBANDS_lower',
        'High_next', 'Low_next'
    ]

    # Remove rows with null values in the specified columns
    df_cleaned = BNF_df_sub.dropna(subset=na_columns_to_check)

    # Convert 'Timestamp' to datetime type if not already
    df_cleaned['Timestamp'] = pd.to_datetime(df_cleaned['Timestamp'])
    
    splits = []
    # Define start and end dates for the dataset
    #min_date_row = df_cleaned['Timestamp'].min()
    max_date_row = df_cleaned['Timestamp'].max()
    
    if TT_dataset == "16w_8w":
        # 1. Split for 16 weeks of training and 8 weeks testing
        train_end = max_date_row - timedelta(weeks=8)
        train_start = train_end - timedelta(weeks=16)
        test_start = train_end
        test_end = max_date_row
        train_df, test_df = split_data_s3(df_cleaned, train_start, train_end, test_start, test_end)
        return train_df, test_df
    
    if TT_dataset == "20w_4w":
        # 2. Split for 20 weeks of training and 4 weeks testing
        train_end = max_date_row - timedelta(weeks=4)
        train_start = train_end - timedelta(weeks=20)
        test_start = train_end
        test_end = max_date_row
        train_df, test_df = split_data_s3(df_cleaned, train_start, train_end, test_start, test_end)
        return train_df, test_df

    if TT_dataset == "22w_2w":
        # 3. Split for 22 weeks of training and 2 weeks testing
        train_end = max_date_row - timedelta(weeks=2)
        train_start = train_end - timedelta(weeks=22)
        test_start = train_end
        test_end = max_date_row
        train_df, test_df = split_data_s3(df_cleaned, train_start, train_end, test_start, test_end)
        return train_df, test_df

    if TT_dataset == "23w_1w":
        # 4. Split for 23 weeks of training and 1 week testing
        train_end = max_date_row - timedelta(weeks=1)
        train_start = train_end - timedelta(weeks=23)
        test_start = train_end
        test_end = max_date_row
        train_df, test_df = split_data_s3(df_cleaned, train_start, train_end, test_start, test_end)
        return train_df, test_df

def split_data_s3(df, train_start, train_end, test_start, test_end):
    train_df = df[(df['Timestamp'] >= train_start) & (df['Timestamp'] < train_end)]
    test_df = df[(df['Timestamp'] >= test_start) & (df['Timestamp'] <= test_end)]
    return train_df, test_df