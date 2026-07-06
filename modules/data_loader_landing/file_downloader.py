import urllib.request
import os
import shutil

# copy file from url to local path
def download_file(url: str, dir_path: str, local_path: str):

    os.makedirs(dir_path, exist_ok=True)

    with urllib.request.urlopen(url) as response, open(local_path, 'wb') as out_file:
        shutil.copyfileobj(response, out_file)