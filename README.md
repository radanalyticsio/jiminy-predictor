# Jiminy Predictor

This is the prediction service for the Jiminy project.

## Setup

To setup the predictor we will assume that model store is running as a MongoDB with
a `models` database created and that it has been populated with at least one model using the [jiminy modeller](https://github.com/radanalyticsio/jiminy-modeler).

To create the predictor service in OpenShift, first install the [oshinko](https://radanalytics.io/get-started) 
tools with

```bash
oc create -f https://radanalytics.io/resources.yaml
```

The predictor can then instantiated by running

```
oc new-app --template oshinko-pyspark-build-dc \
  -p GIT_URI=https://github.com/radanalyticsio/jiminy-predictor \
  -e MODEL_STORE_URI=mongodb://mongo:mongo@mongodb/models \
  -p APP_FILE=app.py \
  -p APPLICATION_NAME=predictor
```

To access the REST web server from outside OpenShift, you can run

```bash
oc expose svc/predictor
```

Additionaly, you can configure the frequency at which the service checks for new models
from the model store by passing the minimum interval (in milliseconds) as the `MODEL_STORE_CHECK_RATE`
environment variable. The default value is 60,000 (1 minute) but can be configured as

```bash
oc new-app --template oshinko-pyspark-build-dc \
  # ...
  -e MODEL_STORE_CHECK_RATE=10000 \
  # ...
```

## REST endpoints

The predictor service provides two main endpoints, one for retrieving ad-hoc
rating predictions (given a list of users/products) and a top-k rated products 
(given a user). The [OpenAPI](https://github.com/OAI/OpenAPI-Specification) specification
can be found in [docs/openapi.yml]([docs/openapi.yml).

### Ratings

To request a prediction for specific user/product pairs you can issue a `POST`
request to

```bash
/prediction/ratings
```

with the required pairs specified in the payload. For instance, to request
the rating predictions for a user with id `100` regarding products with id `200`
and `201` we issue

```bash
curl --request POST \
  --url http://0.0.0.0:8080/predictions/ratings \
  --header 'content-type: application/json' \
  --data '{"user": 100, "products": [200, 201]}'
```

This will return a unique prediction id for this prediction as

```json
{
  "prediction": {
    "id": "944c0d286e4641168f0cfef062f42ab8",
    "products": [
      {
        "id": 200,
        "rating": 0.0
      },
      {
        "id": 201,
        "rating": 0.0
      }
    ],
    "user": 100
  }
}
```

This prediction id can then be used to retrieve
the prediction values by issuing a `GET` request to

```bash
/prediction/ratings/{id}
```

In this example this translates to

```bash
curl --request GET \
  --url http://0.0.0.0:8080/predictions/ratings/944c0d286e4641168f0cfef062f42ab8
```

Resulting in the server response

```json
{
  "id": "944c0d286e4641168f0cfef062f42ab8",
  "products": [
    {
      "id": 200,
      "rating": 3.1720593237252634
    },
    {
      "id": 201,
      "rating": 3.2489033583433855
    }
  ],
  "user": 100
}
```

### Rankings

The rankings endpoint allows to retrieve the `k` highest rated products for a
given user. Similarly to the ratings prediction, we issue a `POST` request to
create a prediction resource, this time to the endpoint

```bash
/prediction/ranks
```

specifying in the payload the user we the predictions for and the number of predictions
we need. For instance, to get the `25` highest ranked products for user `100`, we issue:

```bash
curl --request POST \
   --url http://0.0.0.0:8080/predictions/ranks \
   --header 'content-type: application/json' \
   --data '{"user": 100, "topk": 25}'
```

We will get in return the unique prediction id as

```json
{
  "prediction": {
    "id": "0e583ad221be42758456bd8a9e1fc88a",
    "products": [],
    "topk": 25,
    "user": 100
  }
}
```

To retrieve the actual top 25 rank for this user, we use the returned `id` and issue a `GET` request to the
endpoint:

```bash
/prediction/ranks/{id}
```

In this case, this would be

```bash
curl --request GET \
   --url http://0.0.0.0:8080/predictions/ranks/0e583ad221be42758456bd8a9e1fc88a
```

The resulting response would then include all `25` top rated products for user `100`:

```json
{
  "id": "0e583ad221be42758456bd8a9e1fc88a",
  "products": [
    {
      "id": 98063,
      "rating": 4.978120923261526
    },
    {
      "id": 128812,
      "rating": 4.894847467554492
    },
    {
      "id": 130347,
      "rating": 4.884188147789942
    },
    {
      "id": 99760,
      "rating": 4.702273918596656
    },
    {
      "id": 105968,
      "rating": 4.682071695583521
    },
    {
      "id": 95021,
      "rating": 4.422838638920666
    },
    // ...
}
```