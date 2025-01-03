from utils.bigquery_handler import BigQueryHandler
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.callbacks import EarlyStopping
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error
import math


class LSTMModel:
    def __init__(self, df, categorical_features, target_column, time_steps=6):
        self.df = df
        self.categorical_features = categorical_features
        self.target_column = target_column
        self.time_steps = time_steps

    def create_sequences_multi(X, y, time_steps=6):
        X_seq, y_seq = [], []
        for i in range(len(X) - time_steps):
            X_seq.append(X[i:i+time_steps])
            y_seq.append(y[i+time_steps])
        return np.array(X_seq), np.array(y_seq)


    def preprocess_data(self):

        self.df = pd.get_dummies(self.df, columns=self.categorical_features, drop_first=True)
        self.feature_columns = self.df.columns.drop([self.target_column])

        self.scaler_X = MinMaxScaler(feature_range=(0, 1))
        self.scaled_X = self.scaler_X.fit_transform(self.df[self.feature_columns])

        self.scaler_y = MinMaxScaler(feature_range=(0, 1))
        self.scaled_y = self.scaler_y.fit_transform(self.df[[self.target_column]])

        self.X, self.y = self.create_sequences_multi(self.scaled_X, self.scaled_y, self.time_steps)

        split_index = int(len(self.X) * 0.8)
        self.X_train, self.X_test = self.X[:split_index], self.X[split_index:]
        self.y_train, self.y_test = self.y[:split_index], self.y[split_index:]


    def build_model(self):
        model = Sequential([
            LSTM(256, activation='relu', input_shape=(self.time_steps, self.X.shape[2]), return_sequences=True),
            Dropout(0.3),
            LSTM(128, activation='relu'),
            Dropout(0.3),
            Dense(1)
        ])
        model.compile(optimizer='adam', loss='mean_squared_error', metrics=['mae'])


    def train_model(self, epochs=50, batch_size=32):
        early_stop = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)

        history = self.model.fit(
            self.X_train, self.y_train, 
            epochs=50, 
            batch_size=32, 
            validation_split=0.2, 
            callbacks=[early_stop], 
            verbose=1
        )


    def evaluate_model(self):
        loss, mae = self.model.evaluate(self.X_test, self.y_test, verbose=1)
        print(f"Loss (MSE): {loss}, MAE: {mae}")

        last_sequence = self.X_test[-1]
        last_sequence = np.expand_dims(last_sequence, axis=0)
        prediccion_escalada = self.model.predict(last_sequence)
        prediccion_original = self.scaler_y.inverse_transform(prediccion_escalada)
        print(f"Predicción para el siguiente valor: {prediccion_original[0, 0]}")

        y_pred = self.model.predict(self.X_test)
        y_test_original = self.scaler_y.inverse_transform(self.y_test)
        y_pred_original = self.scaler_y.inverse_transform(self.y_pred)

        rmse = math.sqrt(mean_squared_error(y_test_original, y_pred_original))
        mape = mean_absolute_percentage_error(y_test_original, y_pred_original) * 100
        print(f"RMSE: {rmse}, MAPE: {mape}%")