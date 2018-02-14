import errors
from flask import json
from flask import request as fr
import flask.views as fv
import uuid


class ServerInfo(fv.MethodView):
    info = {
        'application': {
            'name': 'jiminy-recommender',
            'version': '0.0.0'
        }
    }

    def get(self):
        return json.jsonify(self.info)


class PredictionsRatings(fv.MethodView):
    def __init__(self, storage, request_q):
        self.storage = storage
        self.request_q = request_q

    def post(self):
        data = json.loads(fr.data)

        r_dict = {
            'user': data['user'],
            'id': uuid.uuid4().hex,
            'products': [
                {'id': p_id, 'rating': 0.0} for p_id in data['products']
            ]
        }

        try:
            self.storage.store(r_dict)
            self.request_q.put(r_dict)
            resp = json.jsonify(prediction=r_dict)
            resp.status = '201'
            resp.headers.add('Location',
                             '/predictions/ratings/{}'.format(r_dict['id']))
        except errors.PredictionExists:
            resp = errors.single_error_response(
                '500', 'Server Error',
                'A prediction with that ID already exists')
        return resp


class PredictionsRanks(fv.MethodView):
    def __init__(self, storage, request_q):
        self.storage = storage
        self.request_q = request_q

    def post(self):
        data = json.loads(fr.data)

        r_dict = {
            'user': data['user'],
            'id': uuid.uuid4().hex,
            'topk': data['topk'],
            'products': []
        }

        try:
            self.storage.store(r_dict)
            self.request_q.put(r_dict)
            resp = json.jsonify(prediction=r_dict)
            resp.status = '201'
            resp.headers.add('Location',
                             '/predictions/ranks/{}'.format(r_dict['id']))
        except errors.PredictionExists:
            resp = errors.single_error_response(
                '500', 'Server Error',
                'A prediction with that ID already exists')
        return resp


class PredictionDetail(fv.MethodView):
    def __init__(self, storage):
        self.storage = storage

    def get(self, p_id):
        try:
            data = self.storage.get(p_id)
            resp = json.jsonify(data)
        except errors.PredictionNotFound:
            resp = errors.single_error_response(
                '400', 'Not Found', 'That prediction does not exist')
        return resp
