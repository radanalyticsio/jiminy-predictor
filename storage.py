from abc import ABCMeta, abstractmethod
import urlparse

import pymongo
from pymongo import MongoClient
from model import Model
from pyspark.mllib.recommendation import MatrixFactorizationModel


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
        jvm = self._sc._gateway.jvm
        als_model = jvm.org.ruivieira.spark.als.ALSSerialiser.instantiateModel(self._sc._jsc, rank, userFeatures,
                                                                               productFeatures)
        wrapper = jvm.org.apache.spark.mllib.api.python.MatrixFactorizationModelWrapper(als_model)
        model = Model(sc=self._sc, als_model=MatrixFactorizationModel(wrapper), version=version, data_version=1)
        return model


class MongoModelReader(ModelReader):
    def __init__(self, sc, uri):
        super(MongoModelReader, self).__init__(sc=sc, uri=uri)
        client = MongoClient()
        self._db = client.models

    def read(self, version):
        data = list(self._db.models.find({'id': version}))

        features = self.extractFeatures(data=data)

        return self.instantiate(rank=2,
                                version=version,
                                userFeatures=features['userFeatures'],
                                productFeatures=features['productFeatures'])

    def readLatest(self):
        data = list(self._db.models.find().sort('created', pymongo.DESCENDING))

        features = MongoModelReader.extractFeatures(data=data)

        # TODO: get version from db
        return self.instantiate(rank=2,
                                version=1,
                                userFeatures=features['userFeatures'],
                                productFeatures=features['productFeatures'])

    @staticmethod
    def extractFeatures(data):
        _userFeatures = []

        for item in data[0]['userFeatures']:
            _userFeatures.append((item['id'], item['features'],))

        _productFeatures = []

        for item in data[0]['productFeatures']:
            _productFeatures.append((item['id'], item['features'],))

        return {'userFeatures': _userFeatures, 'productFeatures': _productFeatures}


class ParquetModelReader(ModelReader):
    def __init__(self, sc, uri):
        super(ParquetModelReader, self).__init__(sc=sc, uri=uri)

    def read(self, version):
        als_model = MatrixFactorizationModel.load(self._sc, self._url)
        model = Model(sc=self._sc, als_model=als_model, version=version, data_version=1)
        return model

    def readLatest(self):
        pass

