"""cache_tests

This module contains test for cache store implementations
"""
from caches import factory, InfinispanCache, MemoryCache
from mock import Mock
from nose.tools import assert_dict_equal, assert_true, raises
import os
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
        self.mock_invalidate = Mock()
        InfinispanCache.invalidate = self.mock_invalidate

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

    def instantiate_memory_default_test(self):
        """If no environment data is provided for the cache backend,
        use the memory store
        """
        os.environ['CACHE_TYPE'] = ''
        cache = factory()
        assert_true(isinstance(cache, MemoryCache))

    def instantiate_memory_test(self):
        """Passing the 'memory' type should instantiate a `MemoryCache`
        """
        os.environ['CACHE_TYPE'] = 'memory'
        cache = factory()
        assert_true(isinstance(cache, MemoryCache))

    def instantiate_jdg_test(self):
        """Passing the 'jdg' type should instantiate a `InfinispanCache`
        """
        os.environ['CACHE_TYPE'] = 'jdg'
        cache = factory()
        assert_true(isinstance(cache, InfinispanCache))

    def instantiate_infinispan_test(self):
        """Passing the 'infinispan' type should instantiate a `InfinispanCache`
        """
        os.environ['CACHE_TYPE'] = 'infinispan'
        cache = factory()
        assert_true(isinstance(cache, InfinispanCache))

    def instantiate_other_test(self):
        """Passing anything other than `memory`, `jdg` or `infinispan` will
        create a `MemoryCache`
        """
        os.environ['CACHE_TYPE'] = 'foobar'
        cache = factory()
        assert_true(isinstance(cache, MemoryCache))
