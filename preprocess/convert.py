import os
import dask
import dask.dataframe as dd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

def convert_file2parquet_before2018(asc_file):
    try: 
        columns = ['carrier_code', 'carrier_id', 'departure_airport', 'arrival_airport', 'date_of_flight', 'day_of_week',
                   'difference_oag_crs_departure_time', 'difference_oag_crs_arrival_time', 'departure_delay', 'arrival_delay', 
                   'elapsed_time_difference', 'flight_number', 'origin_airport_id', 'destination_airport_id',
                   'cancel_code', 'carrier_delay', 'weather_delay', 'nas_delay', 'security_delay', 'late_aircraft_delay',
                   'ground_time_from_gate', 'longest_gate_time', 'num_diverted_landings', 'diverted_airport_1','gate_time_diverted_1', 
                   'longest_gate_time_diverted_1', 'diverted_airport_code_2', 'gate_time_diverted_2', 'longest_gate_time_diverted_2', 
                   'diverted_airport_code_3', 'gate_time_diverted_3', 'longest_gate_time_diverted_3','diverted_airport_code_4',
                   'gate_time_diverted_4', 'longest_gate_time_diverted_4', 'diverted_airport_code_5', 'gate_time_diverted_5', 
                   'longest_gate_time_diverted_5'
        ]      
        
        dtypes = {  
                0: 'object',
                1: 'Int64',
                2: 'object',
                3: 'object',
                4: 'int64',
                5: 'int64',
                12: 'float64',
                13: 'float64',
                16: 'float64',
                17: 'float64',
                18: 'float64',
                22: 'int64',
                23: 'int64',
                24: 'int64',
                25: 'object',
                26: 'int64',
                27: 'int64',
                28: 'int64',
                29: 'int64',
                30: 'int64',
                32: 'int64',
                33: 'int64',
                34: 'int64',
                35: 'object',
                37: 'float64',
                38: 'float64',
                41: 'object',
                42: 'float64',
                45: 'float64',
                46: 'object',
                49: 'float64',
                50: 'float64',
                53: 'object',
                54: 'float64',
                57: 'float64',
                58: 'object',
                61: 'float64',
                62: 'float64'
            }
        
        columns_to_keep = [0, 1, 2, 3, 4, 5, 12, 13, 16, 17, 18, 22, 23, 24, 25, 26, 27, 28, 29, 30, 32, 33, 34, 35, 37, 38, 41, 42, 45, 46, 49, 50, 53, 54, 57, 58, 61, 62]
        df = dd.read_csv(asc_file, delimiter='|', low_memory=False, header=None, usecols=columns_to_keep, dtype=dtypes) 
        
        df.columns = columns
        df['date_of_flight'] = dd.to_datetime(df['date_of_flight'].astype(str), format='%Y%m%d') 
        
        parquet_folder = asc_file.replace('.asc', '.parquet')
        df.to_parquet(parquet_folder, compression='snappy')  
    except Exception as e:
        pass 
    
 
    
def convert_file2parquet_after2018(asc_file):
    try:
        columns = ['carrier_code', 'carrier_id', 'departure_airport', 'arrival_airport', 'date_of_flight', 'day_of_week','difference_oag_crs_departure', 
                   'difference_oag_crs_arrival_time', 'departure_delay', 'arrival_delay', 'elapsed_time_difference', 
                   'flight_number', 'origin_airport_id', 'destination_airport_id','cancel_code', 'carrier_delay', 'weather_delay', 
                   'nas_delay', 'security_delay', 'late_aircraft_delay','ground_time_from_gate', 'longest_gate_time', 'num_diverted_landings', 
                   'diverted_airport_1','gate_time_diverted_1', 'longest_gate_time_diverted_1', 'diverted_airport_code_2', 'gate_time_diverted_2', 
                   'longest_gate_time_diverted_2', 'diverted_airport_code_3', 'gate_time_diverted_3', 'longest_gate_time_diverted_3',
                   'diverted_airport_code_4','gate_time_diverted_4', 'longest_gate_time_diverted_4', 'diverted_airport_code_5', 'gate_time_diverted_5', 
                   'longest_gate_time_diverted_5'
            ]
        
        columns_to_keep = [0, 1, 6, 7, 8, 9, 16, 17, 20, 21, 22, 23, 27, 28, 29, 30, 31, 32, 33, 34, 35, 37, 38, 39, 41, 42, 45, 47, 48, 51, 53, 54, 57, 59, 60, 63, 65, 66]
        
        dtypes = {
            0: 'object',
            1: 'Int64',
            6: 'object',
            7: 'object',
            8: 'int64',
            9: 'int64',
            16: 'float64',
            17: 'float64',
            20: 'float64',
            21: 'float64',
            22: 'float64',
            23: 'int64',
            27: 'int64',
            28: 'int64',
            29: 'object',
            30: 'int64',
            31: 'int64',
            32: 'int64',
            33: 'int64',
            34: 'int64',
            35: 'int64',
            37: 'int64',
            38: 'int64',
            39: 'object',
            41: 'float64',
            42: 'float64',
            45: 'object',
            47: 'float64',
            48: 'float64',
            51: 'object',
            53: 'float64',
            54: 'float64',
            57: 'object',
            59: 'float64',
            60: 'float64',
            63: 'object',
            65: 'float64',
            66: 'float64',
        }
        
        try:
            df = dd.read_csv(asc_file, delimiter='|', low_memory=False, header=None, usecols=columns_to_keep, dtype=dtypes)
        except Exception as e:
            print(e)
        
        df.columns = columns
        df['date_of_flight'] = dd.to_datetime(df['date_of_flight'].astype(str), format='%Y%m%d') 
        
        parquet_file = asc_file.replace('.asc', '.parquet')
        parquet_folder = asc_file.replace('.asc', '.parquet') 
        try: 
            df.to_parquet(parquet_folder, compression='snappy')
        except Exception as e:
            print(e)     
    except Exception as e:
        pass
    
def convert_to_parquet(folder_name):
    asc_files = [f for f in os.listdir(folder_name) if f.endswith('.asc')]    
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        with tqdm(total=len(asc_files), desc='Converting files') as bar:
            futures = []
            for asc_file in asc_files:
                asc_file = os.path.join(folder_name, asc_file)
                if '.2016' in asc_file or '.2017' in asc_file:
                    futures.append(executor.submit(convert_file2parquet_before2018, asc_file))
                else:
                    futures.append(executor.submit(convert_file2parquet_after2018, asc_file)) 
            for future in as_completed(futures):
                bar.update(1)