import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

def preprocess_data(train_df: pd.DataFrame, test_df: pd.DataFrame, time_step: int = 60):
    # Convert 'Timestamp' to datetime
    train_df['Timestamp'] = pd.to_datetime(train_df['Timestamp'], errors='coerce')
    test_df['Timestamp'] = pd.to_datetime(test_df['Timestamp'], errors='coerce')

    # Drop rows with NaT (Not a Time) values that couldn't be converted
    train_df.dropna(subset=['Timestamp'], inplace=True)
    test_df.dropna(subset=['Timestamp'], inplace=True)

    # Extract features from the timestamp
    train_df['weekday_num'] = train_df['Timestamp'].dt.weekday
    train_df['month_num'] = train_df['Timestamp'].dt.month
    train_df['year'] = train_df['Timestamp'].dt.year
    test_df['weekday_num'] = test_df['Timestamp'].dt.weekday
    test_df['month_num'] = test_df['Timestamp'].dt.month
    test_df['year'] = test_df['Timestamp'].dt.year

    # Drop the original 'Timestamp' column if it is not needed
    train_df = train_df.drop(columns=['Timestamp'])
    test_df = test_df.drop(columns=['Timestamp'])

    feature_columns = [
        'High', 'Low', 'Open',
        'weekday_num', 'month_num', 'year',
        'SMA', 'EMA', 'RSI', 
        'MACD', 'MACD_signal', 
        'BBANDS_upper', 'BBANDS_middle', 'BBANDS_lower'
    ]
    target_columns = ['High_next', 'Low_next']

    # Handle missing values and infinities in features
    train_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    test_df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Fill or drop NaNs (simple approach, consider more sophisticated imputation if necessary)
    train_df.fillna(method='ffill', inplace=True)
    test_df.fillna(method='ffill', inplace=True)

    # Normalizing the features
    scaler = MinMaxScaler()
    x_train_scaled = scaler.fit_transform(train_df[feature_columns])
    x_test_scaled = scaler.transform(test_df[feature_columns])

    # Targets
    y_train = train_df[target_columns].values
    y_test = test_df[target_columns].values

    # Reshape input data to 3D for LSTM and GRU
    def create_sequences(data, time_step):
        return np.array([data[i:i + time_step] for i in range(len(data) - time_step)])
    
    x_train_reshaped = create_sequences(x_train_scaled, time_step)
    x_test_reshaped = create_sequences(x_test_scaled, time_step)

    # Adjust y_train and y_test to match the new shape of x_train_reshaped and x_test_reshaped
    y_train = y_train[time_step:]
    y_test = y_test[time_step:]

    return x_train_reshaped, y_train, x_test_reshaped, y_test, scaler
