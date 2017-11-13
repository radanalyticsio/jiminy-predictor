"""predictions module

Predictions contains the functions and classes related to the Apache Spark
based prediction routines.
"""

import storage
import os
from pyspark import sql as pysql


def loop(request_q, response_q):
    """processing loop for predictions

    This function is meant to be used as the main loop for a process, it
    will wait for new requests on the request_q queue and write responses on
    the response_q queue.
    """

    # Get the model store backend, e.g. MODEL_STORE_URI=mongodb://localhost:27017
    MODEL_STORE_URI = os.environ['MODEL_STORE_URI']

    # just leaving these here for future reference (elmiko)

    spark = pysql.SparkSession.builder.appName("JiminyRec").getOrCreate()
    sc = spark.sparkContext

    # load the latest model from the model store
    model = storage.ModelFactory.fromURL(sc, MODEL_STORE_URI).readLatest()

    response_q.put('ready')  # let the main process know we are ready to start

    while True:
        req = request_q.get()
        if req == 'stop':
            break
        resp = req

        if req['type'] == 'rank':
            recommendations = model.als.recommendProducts(req['user'], req['topk'])
            resp.update(products=
                        [{'id': recommendation[1], 'rating': recommendation[2]} for recommendation in recommendations])
            response_q.put(resp)

        elif req['type'] == 'rating':
            # make predictions
            items = sc.parallelize([(req['user'], p['id']) for p in req['products']])
            predictions = model.als.predictAll(items).map(lambda x: (x[1], x[2])).collect()

            resp.update(products=
                        [{'id': item[0], 'rating': item[1]} for item in predictions])
            response_q.put(resp)
