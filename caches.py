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
    CACHE_HOST = get_arg('CACHE_HOST', '')
    CACHE_PORT = get_arg('CACHE_PORT', '')
    CACHE_NAME = get_arg('CACHE_NAME', '')

    if CACHE_TYPE == 'jdg' or CACHE_TYPE == 'infinispan':
        return InfinispanCache(host=CACHE_HOST, name=CACHE_NAME, port=CACHE_PORT)
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
        else:
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
    """
    A JDG/Infinispan backend cache store (using the JDG REST API)
    """

    def __init__(self, host, name, port):
        self._host = host
        self._name = name
        self._port = port
        self.invalidate()  # invalidate cache, just in case we have stale persisted JDG entries

    def _connect(self):
        return httplib.HTTPConnection(self._host, self._port)

    def store(self, prediction):
        try:
            conn = self._connect()
            conn.request(method="POST", url=self._format(prediction['id']), body=json.dumps(prediction),
                         headers={"Content-Type": "application/json"})
            response = conn.getresponse()
            conn.close()

            # if a prediction with this id already exists, raise an error
            if response.status == 409:
                raise errors.PredictionExists
            # raise a cache error if any status other than OK is returned by JDG
            elif response.status != 200:
                raise errors.CacheError
        except httplib.HTTPException:
            print "Error connecting to JDG/Infinispan cache store"

    def get(self, p_id):
        try:
            conn = self._connect()
            conn.request(method="GET", url=self._format(p_id))
            response = conn.getresponse()
            # raise an error if trying to get a prediction that isn't cached
            if response.status == 404:
                raise errors.PredictionNotFound
            # raise a cache error if any status other than OK is returned by JDG
            elif response.status != 200:
                raise errors.CacheError
            result = json.loads(response.read())
            conn.close()
            return result
        except httplib.HTTPException:
            print "Error connecting to JDG/Infinispan cache store"

    def update(self, prediction):
        try:
            conn = self._connect()
            conn.request(method="PUT", url=self._format(prediction['id']), body=json.dumps(prediction),
                         headers={"Content-Type": "application/json"})
            conn.close()
            response = conn.getresponse()
            # raise an error if trying to update an entry that doesn't exist
            if response.status == 404:
                raise errors.PredictionNotFound
            # raise a cache error if any status other than OK is returned by JDG
            elif response.status != 200:
                raise errors.CacheError
        except httplib.HTTPException:
            print "Error connecting to JDG/Infinispan cache store"

    def _format(self, key):
        return "/rest/{}/{}".format(self._name, key)

    def invalidate(self):
        try:
            conn = self._connect()
            url = "/rest/{}".format(self._name)
            print self._host
            print url
            conn.request(method="DELETE", url=url)
            response = conn.getresponse()
            conn.close()
            # raise a cache error if any status other than OK is returned by JDG
            if response.status != 200:
                raise errors.CacheError
        except httplib.HTTPException:
            print "Error connecting to JDG/Infinispan cache store"
