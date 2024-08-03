import os
import gzip
import shutil
import requests
from datetime import datetime

def create_directory_structure(base_dir, sub_dir):
    full_path = os.path.join(base_dir, sub_dir)
    os.makedirs(full_path, exist_ok=True)
    return full_path

def download_file(url, output_dir):
    filename = os.path.basename(url)
    date_str = datetime.now().strftime("%Y%m%d")
    directory_path = create_directory_structure(output_dir, date_str)
    output_path = os.path.join(directory_path, filename)
    
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(output_path, 'wb') as f:
            f.write(response.raw.read())
    else:
        raise Exception(f"Failed to download file from {url}")
    return output_path

def unzip_file(input_path, output_dir):
    filename = os.path.basename(input_path).replace('.gz', '')
    date_str = datetime.now().strftime("%Y%m%d")
    directory_path = create_directory_structure(output_dir, date_str)
    output_path = os.path.join(directory_path, filename)
    
    with gzip.open(input_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    return output_path

def append_date_to_filename(filepath):
    date_str = datetime.now().strftime("%Y%m%d")
    dir_name, base_name = os.path.split(filepath)
    new_filename = f"{os.path.splitext(base_name)[0]}_{date_str}.csv"
    new_filepath = os.path.join(dir_name, new_filename)
    os.rename(filepath, new_filepath)
    return new_filepath
