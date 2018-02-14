"""cache_tests

This module contains test for cache store implementations
"""
from caches import MemoryCache
from nose.tools import assert_dict_equal
import unittest


class MemoryCacheTests(unittest.TestCase):
    """Test suite for memory cache backend
    """

    def setUp(self):
        """Create a `MemoryCache` instance
        """
        self.cache = MemoryCache()

    def get_test(self):
        """Test if the prediction retrieved from cache is the same as the
        stored one
        """
        prediction = {'id': 1,
                      'products': [
                          {'id': 200, 'rating': 4.5},
                          {'id': 201, 'rating': 2.3},
                      ]}

        self.cache.store(prediction)
        cached = self.cache.get(1)

        assert_dict_equal(prediction, cached)
