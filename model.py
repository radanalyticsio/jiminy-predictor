class Model:
    def __init__(self, sc, als_model, version, data_version):
        self._sc = sc
        self._als_model = als_model
        self._version = version
        self._data_version = data_version

    @property
    def als(self):
        return self._als_model

    @property
    def version(self):
        return self._version
