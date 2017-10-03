"""errors module

Contains helpers for generating REST based error response and exception
classes.
"""
from flask import json


def single_error_response(status, title, details):
    err = {'errors': [{'status': status, 'title': title, 'details': details}]}
    resp = json.jsonify(err)
    resp.status = status
    return resp


class PredictionExists(Exception):
    pass


class PredictionNotFound(Exception):
    pass
