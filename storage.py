from abc import ABCMeta, abstractmethod
from model import Model
import pymongo
from pymongo import MongoClient
from pyspark.mllib.common import _py2java
from pyspark.mllib.recommendation import MatrixFactorizationModel
import urlparse


class ModelFactory:
    """Model store factory for concrete backends

    Returns a concrete instance of a model store backend using a connection URL.
    """
    @staticmethod
    def fromURL(sc, url):
        """Parse a model store connection URL and returns the appropriate model store backend

        :param sc: A Spark context
        :type sc: SparkContext
        :param url: The model store connection URL
        :type url: str
        :return: ModelReader
        """
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
        als_model = jvm.io.radanalytics.als.ALSSerializer.instantiateModel(rank, userFeatures,
                                                                           productFeatures)
        wrapper = jvm.org.apache.spark.mllib.api.python.MatrixFactorizationModelWrapper(als_model)
        model = Model(sc=self._sc, als_model=MatrixFactorizationModel(wrapper), version=version, data_version=1)

        return model


class MongoModelReader(ModelReader):
    """This class allows reading serialized ALS models from a MongoDB backend
    """
    def __init__(self, sc, uri):
        super(MongoModelReader, self).__init__(sc=sc, uri=uri)
        client = MongoClient(self._url)
        self._db = client.models

    def read(self, version):
        # read a serialized model with a specific version id
        data = list(self._db.models.find({'id': version}))

        # read the model's rank metadata
        rank = data[0]['rank']

        # transform the read latent factor from list to a Spark's RDD
        userFactors = _py2java(self._sc, self._sc.parallelize(
            self.extractFeatures(list(self._db.userFactors.find({'model_id': version})))))

        productFactors = _py2java(self._sc, self._sc.parallelize(
            self.extractFeatures(list(self._db.productFactors.find({'model_id': version})))))

        # instantiate a Spark's `MatrixFactorizationModel` from the latent factors RDDs
        return self.instantiate(rank=rank,
                                version=version,
                                userFeatures=userFactors,
                                productFeatures=productFactors)

    def readLatest(self):
        data = list(self._db.models.find().sort('created', pymongo.DESCENDING))

        version = data[0]['id']

        rank = data[0]['rank']

        userFactors = _py2java(self._sc, self._sc.parallelize(
            self.extractFeatures(list(self._db.userFactors.find({'model_id': version})))))

        productFactors = _py2java(self._sc, self._sc.parallelize(
            self.extractFeatures(list(self._db.productFactors.find({'model_id': version})))))

        return self.instantiate(rank=rank,
                                version=version,
                                userFeatures=userFactors,
                                productFeatures=productFactors)

    def latestId(self):
        """
        Reads the most recent model's id from the MongoDB model store
        :return: A model version
        :rtype: int
        """
        data = list(self._db.models.find().sort('created', pymongo.DESCENDING))

        return data[0]['id']

    @staticmethod
    def extractFeatures(data):
        """
        Format the read latent features into a list of `id`, `feature` tuples
        :param data: Latent feature read from MongoDB
        :return: List[Tuple[int, List[int]]]
        """
        return [(item['id'], item['features'],) for item in data]


class ParquetModelReader(ModelReader):
    """This class allows reading serialized ALS models from a Parquet file
    """
    def __init__(self, sc, uri):
        super(ParquetModelReader, self).__init__(sc=sc, uri=uri)

    def read(self, version):
        als_model = MatrixFactorizationModel.load(self._sc, self._url)
        model = Model(sc=self._sc, als_model=als_model, version=version, data_version=1)
        return model

    def readLatest(self):
        pass
