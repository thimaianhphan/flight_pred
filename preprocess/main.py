from convert import convert_to_parquet, convert_file2parquet_after2018
from download import download_files
from extract import extract_files
from upload import upload_to_azure
import os

def main():
    folder_name = 'dataset'
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    print(os.listdir(folder_name))
    
    download_files(folder_name)
    extract_files(folder_name)
    convert_to_parquet(folder_name)
    upload_to_azure(folder_name)
    
if __name__ == '__main__':
    main()    