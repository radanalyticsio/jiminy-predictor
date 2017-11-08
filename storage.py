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
            return MongoModelReader(sc=sc, url=url)


class ModelReader:
    __metaclass__ = ABCMeta

    def __init__(self, sc, url):
        self._sc = sc
        self._url = url

    @abstractmethod
    def read(self, version):
        pass

    @abstractmethod
    def readLatest(self):
        pass

    def instantiate(self, rank, version, userFeatures, productFeatures):
        jvm = self._sc._gateway.jvm
        als_model = jvm.org.ruivieira.spark.als.ALSSerialiser.instantiateModel(self._sc._jsc, rank, userFeatures,
                                                                               productFeatures)
        wrapper = jvm.org.apache.spark.mllib.api.python.MatrixFactorizationModelWrapper(als_model)
        model = Model(sc=self._sc, als_model=MatrixFactorizationModel(wrapper), version=version, data_version=1)
        return model


class MongoModelReader(ModelReader):
    def readLatest(self):
        data = list(self._db.models.find().sort('created', pymongo.DESCENDING))

        features = MongoModelReader.extractFeatures(data=data)

        # TODO: get version from db
        return self.instantiate(rank=2,
                                version=1,
                                userFeatures=features['userFeatures'],
                                productFeatures=features['productFeatures'])

    def __init__(self, sc, url):
        super(MongoModelReader, self).__init__(sc=sc, url=url)
        client = MongoClient()
        self._db = client.models

    def read(self, version):

        data = list(self._db.models.find({'id': version}))

        features = self.extractFeatures(data=data)

        return self.instantiate(rank=2,
                                version=version,
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
    def readLatest(self):
        pass

    def __init__(self, sc, url):
        super(ParquetModelReader, self).__init__(sc=sc, url=url)

    def read(self, version):
        als_model = MatrixFactorizationModel.load(self._sc, self._url)
        model = Model(sc=self._sc, als_model=als_model, version=version, data_version=1)
        return model
