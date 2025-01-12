import mlflow
import mlflow.pytorch
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
import numpy as np
from pyspark.sql import SparkSession
from sklearn.model_selection import train_test_split

def create_sequences(data, sequence_length):
    X, y = [], []
    for i in range(len(data) - sequence_length):
        X.append(np.stack(data["features"][i:i + sequence_length]))
        y.append(data["arrival_delay"][i + sequence_length])
    return np.array(X, dtype=np.float32), np.array(y, dtype=np.float32)

def train_model(X_train, y_train, X_test, y_test):
    # Define LSTM model
    class LSTMModel(nn.Module):
        def __init__(self, input_size, hidden_size, num_layers, output_size):
            super(LSTMModel, self).__init__()
            self.hidden_size = hidden_size
            self.num_layers = num_layers
            self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
            self.fc = nn.Linear(hidden_size, output_size)

        def forward(self, x):
            h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
            c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
            out, _ = self.lstm(x, (h0, c0))
            out = self.fc(out[:, -1, :])
            return out

    # Define model parameters
    input_size = 7  # Number of features
    hidden_size = 50  # Number of LSTM units
    num_layers = 2  # Number of LSTM layers
    output_size = 1  # Predict a single value (arrival delay)

    # Initialize the model
    model = LSTMModel(input_size, hidden_size, num_layers, output_size)

    # Define loss function and optimizer
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

    # Start an MLflow run
    with mlflow.start_run():
        # Log parameters
        mlflow.log_param("hidden_size", hidden_size)
        mlflow.log_param("num_layers", num_layers)
        mlflow.log_param("learning_rate", 0.001)

        # Train the model
        num_epochs = 10
        for epoch in range(num_epochs):
            outputs = model(X_train)
            loss = criterion(outputs, y_train)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            mlflow.log_metric("loss", loss.item(), step=epoch)

            with torch.no_grad():
                val_outputs = model(X_test)
                val_los = criterion(val_outputs, y_test)
                mlflow.log_metric("val_loss", val_los.item(), step=epoch)

        # Log the trained model
        mlflow.pytorch.log_model(model, "lstm_model")

    # Register the model
    model_uri = f"runs:/{mlflow.active_run().info.run_id}/lstm_model"
    registered_model = mlflow.register_model(model_uri, "FlightDelayLSTMModel")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("FlightDelayPrediction").getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    processed_df = spark.read.format("delta").load("dbfs:/mnt/aviation-data/processed_data.delta")

    sampled_df = processed_df.sample(fraction=0.1)

    # Convert to Pandas DataFrame for LSTM input
    pandas_df = sampled_df.select("features", "arrival_delay").toPandas()

    sequence_length = 60
    X, y = create_sequences(pandas_df, sequence_length)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    X_train_tensor = torch.tensor(X_train, dtype = torch.float32)
    y_train_tensor = torch.tensor(y_train, dtype = torch.float32).view(-1, 1)

    X_test_tensor = torch.tensor(X_test, dtype = torch.float32)
    y_test_tensor = torch.tensor(y_test, dtype = torch.float32).view(-1, 1)

    train_model(X_train, y_train, X_test, y_test)