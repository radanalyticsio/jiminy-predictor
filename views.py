from flask import json
import flask.views as fv


class ServerInfo(fv.MethodView):
    info = {
        'application': {
            'name': 'recommender-rest-skeleton',
            'version': '0.0.0'
        }
    }

    def get(self):
        return json.jsonify(self.info)


class PredictSingle(fv.MethodView):
    def __init__(self, model):
        super(PredictSingle, self).__init__()
        self._model = model

    def get(self, user_id, product_id):
        prediction = self._model.predictSingle(user_id=user_id,  product_id=product_id)
        info = {
            'prediction': {
                'product_id': product_id,
                'user_id': product_id,
                'prediction': prediction
            }
        }
        return json.jsonify(info)
