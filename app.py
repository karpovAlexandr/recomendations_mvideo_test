# -*- coding: utf-8 -*-


from flask import Flask, request, jsonify

from handlers import MultiprocessingFileReader

app = Flask(__name__)


@app.route('/recommendations', methods=["GET"])
def get_recommendations():
    """
        GET эндпоинт для получения списка рекомендаций
        sku - идентификатор товарной позиции
        coef - показатель совпадения, по умолчанию 0
        :return - список рекомендаций в формате json

        {
            "sku" : {идентификатор товарной позиции} => string,
            "recommends_coef" : {показатель совпадения} => number,
            "recommends_list" : {список подходящих товаров} => array
        }
    """

    sku = request.args.get('sku', type=str)
    coef = request.args.get('coef', type=float)

    if coef is None:
        coef = 0
    try:
        recommends = MultiprocessingFileReader(sku=sku, coef=coef)
        recommends_list = recommends.run()
    except FileNotFoundError as err:
        recommends_list = None
        print(f'для работы endpoint\'a, нужно приложить файл с названием \'recommends.csv\' в папку с проектом\n'
              f'{err}')

    return jsonify(
        sku=sku,
        recommends_coef=coef,
        recommends_list=recommends_list
    )


if __name__ == '__main__':
    app.run()
