import mlflow
import torch

def deploy_model(test_X_tensor):
    model_uri = "models:/FlightDelayLSTMModel/1"
    loaded_model = mlflow.pytorch.load_model(model_uri)

    with torch.no_grad():
        predictions = loaded_model(test_X_tensor)

if __name__ == "__main__":
    deploy_model()        