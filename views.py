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
