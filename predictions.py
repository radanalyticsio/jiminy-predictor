"""predictions module

Predictions contains the functions and classes related to the Apache Spark
based prediction routines.
"""


def loop(request_q, response_q):
    """processing loop for predictions

    This function is meant to be used as the main loop for a process, it
    will wait for new requests on the request_q queue and write responses on
    the response_q queue.
    """
    # just leaving these here for future reference (elmiko)
    # from pyspark import sql as pysql
    # spark = pysql.SparkSession.builder.appName("JiminyRec").getOrCreate()
    # sc = spark.sparkContext

    response_q.put('ready')  # let the main process know we are ready to start

    while True:
        req = request_q.get()
        if req == 'stop':
            break
        resp = req
        resp.update(products=
                    [{'id': p['id'], 'rating': 5.0} for p in req['products']])
        response_q.put(resp)
