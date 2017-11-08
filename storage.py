from abc import ABCMeta, abstractmethod
import urlparse
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


class MongoModelReader(ModelReader):
    def __init__(self, sc, url):
        super(MongoModelReader, self).__init__(sc=sc, url=url)
        client = MongoClient()
        self._db = client.models

    def read(self, version):

        m = list(self._db.models.find({'id': version}))

        print m

        _userFeatures = []

        for item in m[0]['userFeatures']:
            _userFeatures.append((item['id'], item['features'],))

        _productFeatures = []

        for item in m[0]['productFeatures']:
            _productFeatures.append((item['id'], item['features'],))

        jvm = self._sc._gateway.jvm
        als_model = jvm.org.ruivieira.spark.als.ALSSerialiser.instantiateModel(self._sc._jsc, 2, _userFeatures, _productFeatures)
        wrapper = jvm.org.apache.spark.mllib.api.python.MatrixFactorizationModelWrapper(als_model)
        model = Model(sc=self._sc, als_model=MatrixFactorizationModel(wrapper), version=version, data_version=1)
        return model


class ParquetModelReader(ModelReader):
    def _connect(self):
        pass

    def __init__(self, sc, url):
        super(ParquetModelReader, self).__init__(sc=sc, url=url)

    def read(self, version):
        als_model = MatrixFactorizationModel.load(self._sc, self._url)
        model = Model(sc=self._sc, als_model=als_model, version=version, data_version=1)
        return model
