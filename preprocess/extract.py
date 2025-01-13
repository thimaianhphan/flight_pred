import os
import zipfile
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

def extract_zip(file_path, folder_name):
    try: 
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(folder_name)     
        os.remove(file_path)    
    except zipfile.BadZipFile:
        print(f'Error: {file_name} is a bad ZIP file')
    except Exception as e:
        print(f'Error extracting {file_name}: {e}')
            
            
def extract_files(folder_name):
    zip_files = [f for f in os.listdir(folder_name) if f.endswith('.zip') or f.endswith('.ZIP')]
    with ThreadPoolExecutor(max_workers=8) as executor:
        with tqdm(total=len(zip_files), desc='Extracting files') as bar:
            extract_futures = [executor.submit(extract_zip, os.path.join(folder_name, f), folder_name) for f in zip_files]
            for future in as_completed(extract_futures):
                bar.update(1)      