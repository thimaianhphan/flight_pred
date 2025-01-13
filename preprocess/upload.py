import os
from azure.storage.blob import BlobServiceClient
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

def upload_folder_to_azure(container_client, folder_name, parquet_folder):
    folder_path = os.path.join(folder_name, parquet_folder)
    try:  
        for root, _, files in os.walk(folder_path):
            for file_name in files:
                if file_name.endswith('.parquet'):
                    local_file_path = os.path.join(root, file_name)
                    blob_path = os.path.join(parquet_folder, file_name)
                    
                    with open(local_file_path, 'rb') as data:
                        container_client.upload_blob(name=blob_path, data=data, timeout=1000)   
    except Exception as e:
        print(e)
    
def upload_to_azure(folder_name):
    connection_string = 'YOUR CONNECTION STRING'                        
    container_name = 'YOUR CONTAINER NAME'
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    
    parquet_folders = [entry.name for entry in os.scandir(folder_name) if entry.is_dir()]
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        with tqdm(total=len(parquet_folders), desc='Uploading files') as bar:
            futures = [executor.submit(upload_folder_to_azure, container_client, folder_name, parquet_folder) 
                    for parquet_folder in parquet_folders]
            for future in as_completed(futures):
                bar.update(1)
                       