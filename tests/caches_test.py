"""cache_tests

This module contains test for cache store implementations
"""
from caches import MemoryCache
from nose.tools import assert_dict_equal, raises
import unittest

from errors import PredictionNotFound, PredictionExists


class MemoryCacheTests(unittest.TestCase):
    """Test suite for memory cache backend
    """

    def setUp(self):
        """Create a `MemoryCache` instance
        """
        self.cache = MemoryCache()
        self.prediction = {'id': 1,
                           'products': [
                               {'id': 200, 'rating': 4.5},
                               {'id': 201, 'rating': 2.3},
                           ]}

    def get_test(self):
        """Test if the prediction retrieved from cache is the same as the
        stored one
        """
        self.cache.store(self.prediction)
        cached = self.cache.get(1)

        assert_dict_equal(self.prediction, cached)

    @raises(PredictionNotFound)
    def get_non_existing_test(self):
        """Fetching a non existing prediction should raise an exception
        """
        self.cache.get(2)

    @raises(PredictionExists)
    def store_existing_test(self):
        """Storing with an existing id should raise an exception
        """
        self.cache.store(self.prediction)
        self.cache.store(self.prediction)

    @raises(PredictionNotFound)
    def update_non_existing_test(self):
        """Updating a non existing id should raise an exception
        """
        self.cache.update(self.prediction)

    def store_after_invalidate_test(self):
        """Storing same id after invalidation should work
        """
        self.cache.store(self.prediction)
        self.cache.invalidate()
        self.cache.store(self.prediction)

    @raises(PredictionNotFound)
    def get_after_invalidate_test(self):
        """Get prediction after invalidate should raise an exception
        """
        self.cache.store(self.prediction)
        self.cache.invalidate()
        self.cache.get(1)
