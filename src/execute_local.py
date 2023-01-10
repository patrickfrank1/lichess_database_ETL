import os
from multiprocessing.pool import ThreadPool

from download_games import download_file
from process_file_local import process_file
from database_util import *

def download_and_process_file(url, years_to_download):
    #_, success = download_file(url, years_to_download)
    success = True
    if success:
        process_file(url)
        return (url, True)
    return (url, False)


if __name__ == "__main__":
    DAG_PATH = os.path.realpath(__file__)
    DAG_PATH = '/' + '/'.join(DAG_PATH.split('/')[1:-1]) + '/'

    # download files
    urls = []
    with open(os.getenv('DOWNLOAD_LINKS', 'extract_links.txt'),"r") as url_f:
        years_to_download = [2013, 2014, 2015, 2016, 2017, 2018]    #limited to 2018/2019 to save disk space
        for line in url_f:
            line = line.replace("\n","")
            urls.append(line)
    urls = reversed(urls)   #read the links from oldest to newest
    DB_NAME = os.getenv('POSTGRES_DB', 'lichess_games') #env variables come from docker-compose.yml
    DB_USER = os.getenv('POSTGRES_USER','postgres')
    DB_PASSWORD = os.getenv('POSTGRES_PASSWORD','postgres')
    HOSTNAME = os.getenv('HOSTNAME','localhost')
    PORT = os.getenv('POSTGRES_PORT', '5432')
    connect_string = "host=" + HOSTNAME + " dbname=" + DB_NAME + " user=" + DB_USER + " password=" + DB_PASSWORD \
            + " port=" + PORT
    conn = psycopg2.connect(connect_string)
    _ = initialize_tables(conn)
    with ThreadPool(processes=2) as p:
        results = [p.apply_async(process_file, (url,)) for url in urls]
        for r in results:
            print(r.get())