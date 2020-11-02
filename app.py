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
            "sku" : {идентификатор товарной позиции} => str,
            "recommends_coef" : {показатель совпадения} => int,
            "recommends_list" : {список подходящих товаров} => json
        }

    """

    sku = request.args.get('sku', type=str)
    coef = request.args.get('coef', type=float)

    if coef is None:
        coef = 0

    recommends = MultiprocessingFileReader(sku=sku, coef=coef)
    recommends_list = recommends.run()

    return jsonify(
        sku=sku,
        recommends_coef=coef,
        recommends_list=recommends_list
    )


if __name__ == '__main__':
    app.run()
