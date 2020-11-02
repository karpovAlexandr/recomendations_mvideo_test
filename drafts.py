# -*- coding: utf-8 -*-


# Файл черновиков

import csv
import multiprocessing

import pandas as pd

from decorators import timer
import handlers

test_file = 'test_1.csv'
prod_file = 'recommends.csv'

TEST_RUN = {
    'sku': "fVFCOZ5AFF",
    'coef': 0,
    'csv_file': test_file,
}

PROD_RUN = {
    'sku': "fVFCOZ5AFF",
    'coef': 0,
    'csv_file': prod_file,
}


def process():
    """функция пустышка"""
    pass


@timer
def file_reader_chunk_jobs(sku, coef, csv_file):
    print('вариант с джобами и чанками')
    # https://www.blopig.com/blog/2016/08/processing-large-files-using-python/
    jobs = []
    chunksize = 10 ** 5
    count = 0
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    for chunk in pd.read_csv(csv_file, chunksize=chunksize, names=('sku', 'rec', 'coef')):

        for line in chunk.values:
            jobs.append(pool.apply_async(process, args=(line,)))

        for job in jobs:
            job.get()
        count += 1
    pool.close()


class MultiprocessingFRVer2(handlers.FileReader):
    def __init__(self, sku, coef, csv_file):
        super().__init__(sku, coef, csv_file)
        self.matches = multiprocessing.Queue()
        self.match_list = set()
        self.proc_num = multiprocessing.cpu_count()
        self.chunksize = 10 ** 7

    @timer
    def run(self):
        print("многопроцессорный способ вариант с чанками:")

        for chunk in pd.read_csv(self.csv_file, chunksize=self.chunksize, names=('sku', 'rec', 'coef',)):
            processes = []
            for i in range(self.proc_num):
                processes.append(
                    handlers.FileReaderProcess(
                        dataset=chunk.values,
                        sku=self.sku,
                        coef=self.coef,
                        matches=self.matches)
                )
            for proc in processes:
                proc.start()

            for proc in processes:
                proc.join()

        while True:
            if self.matches.empty():
                print(self.match_list)
                return False
            self.match_list.add(self.matches.get())


class SingeProcessFileReader(handlers.FileReader):
    """Обработчик файла строка за строкой, в одном процессе"""

    @timer
    def run(self):
        with open(self.csv_file, 'r', newline='') as csv_file:
            csv_data = csv.reader(csv_file)
            count = 0

            for row in csv_data:
                if len(row) < 3:
                    return

                self.recommend_collector(row) and self.matches.append(row[0])
                count += 1

            return self.matches
