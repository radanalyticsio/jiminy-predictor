# Jiminy Recommender

This is the recommendation engine for the Jiminy project.

## Model reading

The model is read from the backend specified by the environment variable `$MODEL_STORE_URI`. For instance

```bash
export $MODEL_STORE_URI="mongodb://user:passwrd@127.0.0.1:27017"
```