"""caches module

This module contains functions and classes to help with accessing and upating
the prediction cache. This cache holds the previously requested prediction
results.
"""
import abc
import httplib
import json
import threading as t

import os

import errors


def get_arg(env, default):
    return os.getenv(env) if os.getenv(env, '') is not '' else default


def factory():
    CACHE_TYPE = get_arg('CACHE_TYPE', 'memory')
    CACHE_URL = get_arg('CACHE_URL', '')
    CACHE_NAME = get_arg('CACHE_NAME', '')

    if CACHE_TYPE == 'jdg' or CACHE_TYPE == 'infinispan':
        return InfinispanCache(url=CACHE_URL, cache_name=CACHE_NAME)
    else:
        return MemoryCache()


def updater(response_q, storage):
    """update the cache with predictions

    This function is meant to be used as a thread target. It will listen for
    responses from the prediction process on the response_q queue. As
    responses are registered, the storage cache will be updated.

    Arguments:
    response_q -- A queue of prediction responses
    storage -- A Cache object for storing predictions
    """
    while True:
        resp = response_q.get()
        if resp == 'stop':
            break
        elif resp == 'invalidate':
            storage.invalidate()
        storage.update(resp)


class Cache():
    """an abstract base for storage caches

    Children of this class need to be thread safe.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def store(self, prediction):
        """store a new record

        raises PredictionExists if the id is already in the cache
        """
        pass

    @abc.abstractmethod
    def get(self, p_id):
        """get a record by id

        raises PredictionNotFound if the id is not in the cache
        """
        pass

    @abc.abstractmethod
    def update(self, prediction):
        """update an existing record

        raises PredictionNotFound if the id is not in the cache
        """
        pass

    @abc.abstractmethod
    def invalidate(self):
        """invalidates the entire cache (e.g. when a new model is loaded)

        """
        pass


class MemoryCache(Cache):
    """a memory backed cache

    This cache will not retain information on restart, please use it
    responsibly.
    """

    def __init__(self):
        self.data = {}
        self.lock = t.Lock()

    def store(self, prediction):
        exists = False
        self.lock.acquire()
        if prediction['id'] not in self.data:
            self.data[prediction['id']] = prediction
        else:
            exists = True
        self.lock.release()
        if exists:
            raise errors.PredictionExists

    def get(self, p_id):
        self.lock.acquire()
        ret = self.data.get(p_id)
        self.lock.release()
        if ret is None:
            raise errors.PredictionNotFound
        return ret

    def update(self, prediction):
        exists = True
        self.lock.acquire()
        if prediction['id'] in self.data:
            self.data[prediction['id']] = prediction
        else:
            exists = False
        self.lock.release()
        if not exists:
            raise errors.PredictionNotFound

    def invalidate(self):
        self.lock.acquire()
        self.data = {}
        self.lock.release()


class InfinispanCache(Cache):

    def __init__(self, url, cache_name):
        self._url = url
        self._cache_name = cache_name
        self.invalidate()  # invalidate cache, just in case we have stale persisted JDG entries

    def store(self, prediction):
        conn = httplib.HTTPConnection(self._url)
        conn.request(method="POST", url=self._format(prediction['id']), body=json.dumps(prediction),
                     headers={"Content-Type": "application/json"})

    def get(self, p_id):
        conn = httplib.HTTPConnection(self._url)
        url = "/rest/{}/{}".format(self._cache_name, p_id)
        conn.request(method="GET", url=url)
        response = conn.getresponse()
        return json.loads(response.read())

    def update(self, prediction):
        conn = httplib.HTTPConnection(self._url)
        conn.request(method="PUT", url=self._format(prediction['id']), body=json.dumps(prediction),
                     headers={"Content-Type": "application/json"})

    def _format(self, key):
        return "/rest/{}/{}".format(self._cache_name, key)

    def invalidate(self):
        conn = httplib.HTTPConnection(self._url)
        url = "/rest/{}".format(self._cache_name)
        conn.request(method="DELETE", url=url)
