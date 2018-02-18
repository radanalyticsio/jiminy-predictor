class Model:
    """
    ALS model wrapper class.

    Adds extra functionality and metadata to Spark's `als_model`.
    """
    def __init__(self, sc, als_model, version, data_version):
        """
        Instantiate the ALS model wrapper

        :param sc: A Spark context
        :type sc: SparkContext
        :param als_model: A Spark ALS model
        :type als_model: MatrixFactorizationModel
        :param version: The model's unique version
        :type version: int
        :param data_version: The rating's data version
        :type data_version: int
        """
        self._sc = sc
        self._als_model = als_model
        self._version = version
        self._data_version = data_version
        self._products = self._als_model.productFeatures()\
            .map(lambda x: x[0]).cache()
        self._users = self._als_model.userFeatures() \
            .map(lambda x: x[0]).cache()

    @property
    def als(self):
        """Getter method for the `MatrixFactorizationModel` Spark class
        """
        return self._als_model

    @property
    def version(self):
        """Getter method for the model's version
        """
        return self._version

    def valid_user(self, user_id):
        return self._users.filter(lambda x: x == user_id).count() > 0

    def valid_product(self, product_id):
        return self._products.filter(lambda x: x == product_id).count() > 0
