"""predictions module

Predictions contains the functions and classes related to the Apache Spark
based prediction routines.
"""

import datetime
import logger
import os
import os.path
from pyspark import sql as pysql
import storage
from utils import get_arg


def loop(request_q, response_q):
    """processing loop for predictions

    This function is meant to be used as the main loop for a process, it
    will wait for new requests on the request_q queue and write responses on
    the response_q queue.
    """

    # get the model store backend
    # if none provided the default is `mongodb://localhost:27017`
    MODEL_STORE_URI = get_arg('MODEL_STORE_URI', 'mongodb://localhost:27017')

    # get the minimum interval (in miliseconds) between model store checks for
    # updated models. default is one minute (at the limit, for a check at every
    # request, set to `0`).
    MODEL_STORE_CHECK_RATE = get_arg('MODEL_STORE_CHECK_RATE', 60000)

    # just leaving these here for future reference (elmiko)

    spark = pysql.SparkSession.builder.appName("JiminyRec").getOrCreate()

    # load the local jar file we will need
    localjar = os.path.join(os.environ['PWD'],
                            'libs',
                            'spark-als-serializer_2.11-0.2.jar')
    loader = spark._jvm.Thread.currentThread().getContextClassLoader()
    url = spark._jvm.java.net.URL('file:' + localjar)
    loader.addURL(url)
    # get the SparkContext singleton from the JVM (not the pyspark API)
    context = spark._jvm.org.apache.spark.SparkContext.getOrCreate()
    context.addJar(localjar)
    print('------------------- loading jar -------------------------------')
    print(url)

    # get a context (from the pyspark API) to do some work
    sc = spark.sparkContext

    # load the latest model from the model store
    model_reader = storage.ModelFactory.fromURL(sc=sc, url=MODEL_STORE_URI)

    model = model_reader.readLatest()

    # Last time the model store was checked
    last_model_check = datetime.datetime.now()

    response_q.put('ready')  # let the main process know we are ready to start

    # acquire logger
    _logger = logger.get_logger()

    while True:

        # calculate how much time elapsed since the last model check
        current_time = datetime.datetime.now()
        model_check_delta = current_time - last_model_check
        # if the model check was performed longer than the check rate threshold
        if model_check_delta.total_seconds() * 1000 >= MODEL_STORE_CHECK_RATE:
            # check for new models in the model store
            latest_id = model_reader.latestId()
            if model.version != latest_id:
                model = model_reader.read(version=latest_id)
                # invalidade the cache, since we are using a new model
                response_q.put('invalidate')
            last_model_check = current_time

        req = request_q.get()
        # stop the processing loop
        if req == 'stop':
            break
        resp = req

        # preform a top-k rating for the specified user prediction
        if 'topk' in req:  # make rank predictions
            # check if we have a valid user
            if model.valid_user(req['user']):
                recommendations = model.als.recommendProducts(int(req['user']),
                                                              int(req['topk']))
                # update the cache store
                resp.update(products=[
                    {'id': recommendation[1], 'rating': recommendation[2]}
                    for recommendation in recommendations
                ])
            else:
                _logger.error("Requesting rankings for invalid user id={}"
                              .format(req['user']))
                resp.update(products=[])
            response_q.put(resp)

        else:
            # make rating predictions
            items = sc.parallelize(
                [(req['user'], p['id']) for p in req['products']])
            predictions = model.als.predictAll(items).map(
                lambda x: (x[1], x[2])).collect()
            # update the cache store
            resp.update(products=[
                {'id': item[0], 'rating': item[1]}
                for item in predictions
            ])
            response_q.put(resp)
