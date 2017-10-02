import flask
import views
import recommender
from pyspark import SparkConf, SparkContext

# initialise Spark
conf = SparkConf().setAppName("recommender").setMaster('local[*]')
sc = SparkContext(conf=conf)

# load an ALS model from disk
model = recommender.Model.readFromFile(sc, './models/trained_model')


def main():
    app = flask.Flask(__name__)
    app.config['SECRET_KEY'] = 'secret!'
    app.add_url_rule('/', view_func=views.ServerInfo.as_view('server'))

    # get a prediction for a single user, single product
    app.add_url_rule('/predict/user/<int:user_id>/product/<int:product_id>',
                     view_func=views.PredictSingle.as_view('predict_single', model), methods=['GET'])

    app.run(host='0.0.0.0', port=8080)


if __name__ == '__main__':
    main()
