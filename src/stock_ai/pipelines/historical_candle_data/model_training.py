import mlflow
import mlflow.keras
from keras.models import Sequential
from keras.layers import LSTM, GRU, Dense
from keras.optimizers import Adam

def create_lstm_model(input_shape):
    model = Sequential()
    model.add(LSTM(50, return_sequences=True, input_shape=input_shape))
    model.add(LSTM(50, return_sequences=False))
    model.add(Dense(25))
    model.add(Dense(2))  # Predicting two outputs (High_next, Low_next)
    model.compile(optimizer=Adam(), loss='mean_squared_error')
    return model

def create_gru_model(input_shape):
    model = Sequential()
    model.add(GRU(50, return_sequences=True, input_shape=input_shape))
    model.add(GRU(50, return_sequences=False))
    model.add(Dense(25))
    model.add(Dense(2))
    model.compile(optimizer=Adam(), loss='mean_squared_error')
    return model

def train_and_log_models(x_train, y_train, x_test, y_test):
    input_shape = (x_train.shape[1], x_train.shape[2])
    # # LSTM Model
    # lstm_model = create_lstm_model(input_shape)
    # lstm_history = lstm_model.fit(x_train, y_train, epochs=10, batch_size=64, validation_data=(x_test, y_test))

    # GRU Model
    gru_model = create_gru_model(input_shape)
    gru_history = gru_model.fit(x_train, y_train, epochs=10, batch_size=64, validation_data=(x_test, y_test))

    # # Evaluate and log
    # lstm_loss = lstm_model.evaluate(x_test, y_test)
    gru_loss = gru_model.evaluate(x_test, y_test)

    # print("lstm_loss", lstm_loss)
    # print("gru_loss", gru_loss)

    return gru_model, gru_history
    
    # mlflow.end_run()

    # with mlflow.start_run():
    #     # LSTM Model
    #     lstm_model = create_lstm_model(input_shape)
    #     lstm_history = lstm_model.fit(x_train, y_train, epochs=10, batch_size=64, validation_data=(x_test, y_test))

    #     # Log LSTM model
    #     mlflow.keras.log_model(lstm_model, "lstm_model")
    #     mlflow.log_params({"model_type": "LSTM", "epochs": 10, "batch_size": 64})
    #     mlflow.log_metrics({"lstm_train_loss": lstm_history.history['loss'][-1], "lstm_val_loss": lstm_history.history['val_loss'][-1]})

    #     # GRU Model
    #     gru_model = create_gru_model(input_shape)
    #     gru_history = gru_model.fit(x_train, y_train, epochs=10, batch_size=64, validation_data=(x_test, y_test))

    #     # Log GRU model
    #     mlflow.keras.log_model(gru_model, "gru_model")
    #     mlflow.log_params({"model_type": "GRU", "epochs": 10, "batch_size": 64})
    #     mlflow.log_metrics({"gru_train_loss": gru_history.history['loss'][-1], "gru_val_loss": gru_history.history['val_loss'][-1]})

    #     # Evaluate and log
    #     lstm_loss = lstm_model.evaluate(x_test, y_test)
    #     gru_loss = gru_model.evaluate(x_test, y_test)
    #     mlflow.log_metrics({"lstm_test_loss": lstm_loss, "gru_test_loss": gru_loss})

    #     return lstm_model, gru_model, lstm_history, gru_history
