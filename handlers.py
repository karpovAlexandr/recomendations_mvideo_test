import csv
import multiprocessing

from decorators import timer

OUTPUT_FILE = 'recommends.csv'


class FileReader(multiprocessing.Process):
    def __init__(self, dataset, sku, coef, matches, number):
        super().__init__()
        self.dataset = dataset
        self.sku = sku
        self.coef = coef
        self.matches = matches
        self.count = 0
        self.number = number

    def run(self):
        for row in self.dataset:
            self.is_it(row)
            self.count += 1

    def is_it(self, row):
        if len(row) < 3:
            return False
        if row[1] == self.sku and float(row[2]) >= self.coef:
            self.matches.put(row[0])


@timer
def recommends_reader(sku, coef, csv_file=OUTPUT_FILE):
    """
    :param sku: идентификатор товарной позиции
    :param coef: показатель совпадения
    :param csv_file: файл с коэффициентами рекомендаций
    :return: список рекомендация для sku, с коэффициентом совпадений >= coef
    """

    with open(csv_file, 'r', newline='') as csv_file:
        csv_data = csv.reader(csv_file)
        matches = []
        for row in csv_data:
            if row[1] == sku and float(row[2]) >= coef:
                matches.append(row[0])
        return matches


def recommends_reader_mp(sku, coef, csv_file='recommends.csv'):
    print("многопроцессорный способ:")
    matches = multiprocessing.Queue()
    match_list = []
    with open(csv_file, 'r', newline='') as csv_file:
        csv_data = csv.reader(csv_file)
        proc_num = 4

        for i in range(proc_num):
            proc = FileReader(dataset=csv_data, sku=sku, coef=coef, number=i, matches=matches)
            proc.start()
        for i in range(proc_num):
            proc.join()

    while True:
        if matches.empty():
            return match_list
        match_list.append(matches.get())
