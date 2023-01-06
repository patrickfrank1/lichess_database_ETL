import os
from multiprocessing.pool import ThreadPool

from download_games import download_file
from process_file_local import process_file

def download_and_process_file(url, years_to_download):
    _, success = download_file(url, years_to_download)
    if success:
        process_file(url)
        filepath = DAG_PATH + url.split("/")[-1]
        os.remove(filepath)
        return (url, True)
    return (url, False)


if __name__ == "__main__":
    DAG_PATH = os.path.realpath(__file__)
    DAG_PATH = '/' + '/'.join(DAG_PATH.split('/')[1:-1]) + '/'

    # download files
    urls = []
    with open("download_links.txt","r") as url_f:
        years_to_download = [2013, 2014, 2015, 2016, 2017, 2018, 2019]    #limited to 2018/2019 to save disk space
        for line in url_f:
            line = line.replace("\n","")
            urls.append(line)
    urls = reversed(urls)   #read the links from oldest to newest
    with ThreadPool(processes=14) as p:
        results = [p.apply_async(download_and_process_file, (url, years_to_download)) for url in urls]
        for r in results:
            print(r.get())

# for 2013 games: 900mb for 3.3m games --> too much, reduce footprint!