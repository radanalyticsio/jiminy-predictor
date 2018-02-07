from abc import ABCMeta, abstractmethod
import urlparse
import time

import pymongo
from pymongo import MongoClient
from pyspark.mllib.common import _py2java

from model import Model
from pyspark.mllib.recommendation import MatrixFactorizationModel

import logger

class ModelFactory:
    @staticmethod
    def fromURL(sc, url):
        _parsed_url = urlparse.urlparse(url)
        if _parsed_url[0].lower() == 'mongodb':
            return MongoModelReader(sc=sc, uri=url)


class ModelReader:
    """
    Abstract class to read model from a model store.
    Implement backend specific model stores as a subclass.
    """
    __metaclass__ = ABCMeta

    def __init__(self, sc, uri):
        """

        :param sc: A Spark context
        :param uri: The connection URI
        """
        self._sc = sc
        self._url = uri
        self._logger = logger.get_logger()

    @abstractmethod
    def read(self, version):
        """
        Read a specific model version from the model store

        :param version: unique model identifier
        :return: A `Model` instance
        """
        pass

    @abstractmethod
    def readLatest(self):
        """
        Read the latest model from the model store

        :return: A `Model` instance
        """
        pass

    def instantiate(self, rank, version, userFeatures, productFeatures):
        """
        Instantiate an ALS `Model` from the model store data

        :param rank: ALS rank
        :param version: `Model` version
        :param userFeatures: A list of user features
        :param productFeatures: A list of product features
        :return: A `Model` instance
        """

        start_time = time.time()

        jvm = self._sc._gateway.jvm
        als_model = jvm.io.radanalytics.als.ALSSerializer.instantiateModel(rank, userFeatures,
                                                                           productFeatures)
        wrapper = jvm.org.apache.spark.mllib.api.python.MatrixFactorizationModelWrapper(als_model)
        model = Model(sc=self._sc, als_model=MatrixFactorizationModel(wrapper), version=version, data_version=1)

        elapsed_time = time.time() - start_time

        self._logger.info("Model version {0} took {1} seconds to instantiate".format(version, elapsed_time))

        return model


class MongoModelReader(ModelReader):
    def __init__(self, sc, uri):
        super(MongoModelReader, self).__init__(sc=sc, uri=uri)
        client = MongoClient(self._url)
        self._db = client.models
        self._logger.info("Using MongoDB as the model store")

    def read(self, version):
        start_time = time.time()

        data = list(self._db.models.find({'id': version}))

        rank = data[0]['rank']

        userFactors = _py2java(self._sc, self._sc.parallelize(
            self.extractFeatures(list(self._db.userFactors.find({'model_id': version})))))

        productFactors = _py2java(self._sc, self._sc.parallelize(
            self.extractFeatures(list(self._db.productFactors.find({'model_id': version})))))

        elapsed_time = time.time() - start_time

        self._logger.info("Model version {0} read in {1} seconds".format(version, elapsed_time))

        return self.instantiate(rank=rank,
                                version=version,
                                userFeatures=userFactors,
                                productFeatures=productFactors)

    def readLatest(self):
        start_time = time.time()

        data = list(self._db.models.find().sort('created', pymongo.DESCENDING))

        version = data[0]['id']

        rank = data[0]['rank']

        userFactors = _py2java(self._sc, self._sc.parallelize(
            self.extractFeatures(list(self._db.userFactors.find({'model_id': version})))))

        productFactors = _py2java(self._sc, self._sc.parallelize(
            self.extractFeatures(list(self._db.productFactors.find({'model_id': version})))))

        elapsed_time = time.time() - start_time

        self._logger.info("Model version {0} read in {1} seconds".format(version, elapsed_time))

        return self.instantiate(rank=rank,
                                version=version,
                                userFeatures=userFactors,
                                productFeatures=productFactors)

    def latestId(self):
        data = list(self._db.models.find().sort('created', pymongo.DESCENDING))

        return data[0]['id']

    @staticmethod
    def extractFeatures(data):
        return [(item['id'], item['features'],) for item in data]


class ParquetModelReader(ModelReader):
    def __init__(self, sc, uri):
        super(ParquetModelReader, self).__init__(sc=sc, uri=uri)

    def read(self, version):
        als_model = MatrixFactorizationModel.load(self._sc, self._url)
        model = Model(sc=self._sc, als_model=als_model, version=version, data_version=1)
        return model

    def readLatest(self):
        pass
