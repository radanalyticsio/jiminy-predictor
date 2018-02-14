"""main application file for jiminy-recommender"""
import caches
import flask
import multiprocessing as mp
import os
import predictions
import threading as t
import views


def main():
    """start the http service"""

    # add the ALS type conversion Scala helper jar
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--jars ./libs/spark-als-serializer_2.11-0.2.jar pyspark-shell'  # noqa: E501

    # create the flask app object
    app = flask.Flask(__name__)
    # change this value for production environments
    app.config['SECRET_KEY'] = 'secret!'

    # queues for ipc with the prediction process
    request_q = mp.Queue()
    response_q = mp.Queue()

    # start the prediction process
    process = mp.Process(target=predictions.loop,
                         args=(request_q, response_q))
    process.start()

    # waiting for processing loop to become active
    response_q.get()

    # initialize a cache store
    # (uses environment variables for connection information)
    storage = caches.factory()

    # create and start the cache updater thread
    thread = t.Thread(target=caches.updater, args=(response_q, storage))
    thread.start()

    # configure routes and start the service
    app.add_url_rule('/', view_func=views.ServerInfo.as_view('server'))
    app.add_url_rule('/predictions/ratings',
                     view_func=views.PredictionsRatings.as_view(
                         'rating_prediction',
                         storage,
                         request_q))
    app.add_url_rule('/predictions/ratings/<string:p_id>',
                     view_func=views.PredictionDetail.as_view(
                         'rating_predictions',
                         storage))

    app.add_url_rule('/predictions/ranks',
                     view_func=views.PredictionsRanks.as_view(
                         'rank_prediction',
                         storage,
                         request_q))

    app.add_url_rule('/predictions/ranks/<string:p_id>',
                     view_func=views.PredictionDetail.as_view(
                         'rank_predictions',
                         storage))

    app.run(host='0.0.0.0', port=8080)
    request_q.put('stop')
    response_q.put('stop')


if __name__ == '__main__':
    main()
