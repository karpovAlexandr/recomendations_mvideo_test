import csv
import multiprocessing

from decorators import timer

OUTPUT_FILE = 'recommends.csv'


class FileReader:
    """Базовый класс для обработчика файла"""

    def __init__(self, sku, coef, csv_file=OUTPUT_FILE):
        self.sku = sku
        self.coef = coef
        self.csv_file = csv_file
        self.matches = []

    def recommend_collector(self, line):
        return True if line[1] == self.sku and float(line[2]) >= self.coef else False


class SingeProcessFileReader(FileReader):
    @timer
    def run(self):
        print("однопроцессорный способ:")

        with open(self.csv_file, 'r', newline='') as csv_file:
            csv_data = csv.reader(csv_file)
            count = 0

            for row in csv_data:
                self.recommend_collector(row) and self.matches.append(row[0])
                count += 1
            print(self.matches)
            return self.matches


class FileReaderProcess(multiprocessing.Process):
    def __init__(self, dataset, sku, coef, matches):
        super().__init__()
        self.dataset = dataset
        self.sku = sku
        self.coef = coef
        self.matches = matches
        self.count = 0

    def run(self):
        for row in self.dataset:
            self.process(row)
            self.count += 1

    def process(self, row):
        if len(row) < 3:
            # если попадается невалидная строка, то мы ее пропускаем (при желании, можно отдавать её в логгер)
            # print(f'problem with {row}', flush=True)
            return
        if row[1] == self.sku and float(row[2]) >= self.coef:
            self.matches.put(row[0])


class MultiprocessingFileReader(FileReader):
    def __init__(self, sku, coef, csv_file=OUTPUT_FILE):
        super().__init__(sku, coef, csv_file)
        self.matches = multiprocessing.Queue()
        self.match_list = []
        self.proc_num = 4

    @timer
    def run(self):
        print("многопроцессорный способ:")
        with open(self.csv_file, 'r', newline='') as csv_file:
            csv_data = csv.reader(csv_file)

            for i in range(self.proc_num):
                proc = FileReaderProcess(dataset=csv_data, sku=self.sku, coef=self.coef, matches=self.matches)
                proc.start()

            for i in range(self.proc_num):
                proc.join()

        while not self.matches.empty():
            self.match_list.append(self.matches.get())
        return self.match_list


if __name__ == '__main__':
    pass
