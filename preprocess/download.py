import requests
from bs4 import BeautifulSoup
import os
import zipfile
import dask
import dask.dataframe as dd
import dask.bag as db
from azure.storage.blob import BlobServiceClient
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


def fetch_data_links():
    url = 'https://www.bts.gov/browse-statistical-products-and-data/bts-publications/airline-service-quality-performance-234-time'
    base_url = 'https://www.bts.gov'
    response = requests.get(url)

    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a', href=True)
    
    data_links = []
    years = ['.2016', '.2017', '.2018', '.2019', '.2021', '.2022', '.2023', '.2024']
    for link in links:
        if 'ONTIME.TD' in link['href'] and any(year in link['href'] for year in years):
            if base_url in link['href']:
                data_links.append(link['href'])
                continue
            data_links.append(base_url + link['href'])
    
    return data_links        

def download_file(link, folder_name):
    file_name = link.split('/')[-1]
    file_path = os.path.join(folder_name, file_name)
   
    with requests.get(link) as r:
        with open(file_path, 'wb') as f:
            f.write(r.content)
    
    return file_path

def download_files(folder_name):
    data_links = fetch_data_links()
    with ThreadPoolExecutor(max_workers=8) as executor:
        with tqdm(total=len(data_links), desc='Downloading files') as bar:
            download_futures = [executor.submit(download_file, link, folder_name) for link in data_links]
            for future in as_completed(download_futures):
                bar.update(1)
                       
  
    
