import mlflow
from sklearn.metrics import mean_squared_error

def monitor_model():
    # Log production metrics
    mlflow.log_metric("production_mse", mean_squared_error(y_true, y_pred))

if __name__ == "__main__":
    monitor_model()