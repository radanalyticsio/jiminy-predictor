import unittest
from nose.tools import assert_dict_equal

from caches import MemoryCache


class MemoryCacheTests(unittest.TestCase):

    def setUp(self):
        self.cache = MemoryCache()

    def get_test(self):
        prediction = {'id': 1,
                      'products': [
                          {'id': 200, 'rating': 4.5},
                          {'id': 201, 'rating': 2.3},
                      ]}

        self.cache.store(prediction)
        cached = self.cache.get(1)

        assert_dict_equal(prediction, cached)
