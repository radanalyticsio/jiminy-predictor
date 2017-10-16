
class Model:
    def __init__(self, als_model):
        self._als_model = als_model

    def predictSingle(self, user_id, product_id):
        return self._als_model.predict(user=user_id, product=product_id)
