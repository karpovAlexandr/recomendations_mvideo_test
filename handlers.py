import csv
import time


def timer(func):
    def surrogate(*args, **kwargs):
        started_at = time.time()

        result = func(*args, **kwargs)

        ended_at = time.time()
        elapsed = round(ended_at - started_at, 4)
        print(f'Время работы - {elapsed} сек.')

        return result

    return surrogate


@timer
def recommends_reader(sku, coef, csv_file='recommends.csv'):
    with open(csv_file, 'r', newline='') as csv_file:
        csv_data = csv.reader(csv_file)
        matches = []
        for row in csv_data:
            if row[1] == sku and float(row[2]) >= coef:
                matches.append(row[0])
        return matches
