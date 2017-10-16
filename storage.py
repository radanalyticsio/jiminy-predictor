from abc import ABCMeta, abstractmethod

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
    def __init__(self, sc, path):
        super(ParquetModelReader, self).__init__()
        self._sc = sc
        self._path = path

    def read(self):
        return MatrixFactorizationModel.load(self._sc, self._path)
