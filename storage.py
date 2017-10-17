from abc import ABCMeta, abstractmethod
from model import Model

from pyspark.mllib.recommendation import MatrixFactorizationModel


class ModelReader:
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    def read(self):
        pass


class MongoModelReader(ModelReader):
    def __init__(self):
        super(MongoModelReader, self).__init__()

    def read(self):
        pass


class ParquetModelReader(ModelReader):
    def __init__(self, sc, path, version, data_version):
        super(ParquetModelReader, self).__init__()
        self._sc = sc
        self._path = path
        self._version = version
        self._data_version = data_version

    def read(self):
        als_model = MatrixFactorizationModel.load(self._sc, self._path)
        model = Model(sc=self._sc, als_model=als_model, version=self._version, data_version=self._data_version)
        return model
