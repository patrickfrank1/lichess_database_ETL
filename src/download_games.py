from multiprocessing.pool import ThreadPool
from urllib.request import urlopen
from urllib.error import HTTPError
from retry import retry
from pathlib import Path
import shutil
import glob
import time
import os



@retry(HTTPError, tries=-1, delay=60)
def urlopen_retry(url):
    return urlopen(url)

def download_file(url, years_to_download=None, chunk_size=16*1024):
    DAG_PATH = os.path.realpath(__file__)
    DAG_PATH = '/' + '/'.join(DAG_PATH.split('/')[1:-1]) + '/'
    print(DAG_PATH)
    #Path('../lichess_data').mkdir(exist_ok=True)
    filename = url.split("/")[-1]
    year = int(filename.split("_")[-1][:4])
    downloaded = [i.replace("./","") for i in glob.glob("./lichess*")]
    if years_to_download and year not in years_to_download:
        return (url, False)
    if filename not in downloaded: 
        #if int(filename.split("-")[-1][:2]) % 2 == 0:   #download even months only (save disk space)
        print(f"downloading {filename}...")
        response = urlopen_retry(url)
        with open(DAG_PATH + filename, 'wb') as local_f:
            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                local_f.write(chunk)
        return (url, True)
    else:
        return (url, False)

if __name__ == "__main__":
    urls = []
    with open("download_links.txt","r") as url_f:
        years_to_download = [2013, 2018, 2019]    #limited to 2018/2019 to save disk space
        for line in url_f:
            line = line.replace("\n","")
            urls.append(line)
    with ThreadPool() as p:
        results = [p.apply_async(download_file, (url, years_to_download)) for url in urls]
        for r in results:
            print(r.get())


