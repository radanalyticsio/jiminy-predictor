"""caches module

This module contains functions and classes to help with accessing and upating
the prediction cache. This cache holds the previously requested prediction
results.
"""
import abc
import threading as t
import pickle

import errors

from infinispan.remotecache import RemoteCache


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


class InfinispanCache(Cache):
    def __init__(self, host='127.0.0.1', port=11222, cache_name=''):
        self._remote_cache = RemoteCache(host=host, port=port, cache_name=cache_name)
        self.lock = t.Lock()

    def store(self, prediction):
        exists = False
        self.lock.acquire()
        if not self._remote_cache.contains_key(prediction['id']):
            self._remote_cache.put(prediction['id'], pickle.dumps(prediction))
        else:
            exists = True
        self.lock.release()
        if exists:
            raise errors.PredictionExists

    def get(self, p_id):
        self.lock.acquire()
        ret = self._remote_cache.get(p_id)
        self.lock.release()
        if ret is None:
            raise errors.PredictionNotFound
        return pickle.loads(ret)

    def update(self, prediction):
        exists = True
        self.lock.acquire()
        if self._remote_cache.contains_key(prediction['id']):
            self._remote_cache.put(prediction['id'], pickle.dumps(prediction))
        else:
            exists = False
        self.lock.release()
        if not exists:
            raise errors.PredictionNotFound


def factory(engine):
    if engine.lower() == 'infinispan':
        print "using InfinispanCache"
        return InfinispanCache()
    else:
        print "using MemoryCache"
        return MemoryCache()
