"""predictions module

Predictions contains the functions and classes related to the Apache Spark
based prediction routines.
"""
from pyspark.mllib.recommendation import MatrixFactorizationModel


def loop(request_q, response_q):
    """processing loop for predictions

    This function is meant to be used as the main loop for a process, it
    will wait for new requests on the request_q queue and write responses on
    the response_q queue.
    """
    # just leaving these here for future reference (elmiko)
    from pyspark import sql as pysql
    spark = pysql.SparkSession.builder.appName("JiminyRec").getOrCreate()
    sc = spark.sparkContext

    # load a pre-trained model
    model = MatrixFactorizationModel.load(sc, './models/trained_model')

    response_q.put('ready')  # let the main process know we are ready to start

    while True:
        req = request_q.get()
        if req == 'stop':
            break
        resp = req
        # make predictions
        items = sc.parallelize([(req['user'], p['id']) for p in req['products']])
        predictions = model.predictAll(items).map(lambda x: (x[1], x[2])).collect()
        print(predictions)

        resp.update(products=
                    [{'id': item[0], 'rating': item[1]} for item in predictions])
        response_q.put(resp)
