"""utils_tests

This module contains test for the utils module
"""
import os
from nose.tools import assert_equal
import unittest

from utils import get_arg


class UtilsTests(unittest.TestCase):
    """Test suite for the utils model
    """

    def setUp(self):
        """Create a `MemoryCache` instance
        """
        os.environ['FOO'] = ''

    def get_args_empty_test(self):
        """Test if the returned args is empty if the env variable does not
        exist
        """
        foo = get_arg(env='FOO', default='')

        assert_equal(foo, '')

    def get_args_not_empty_test(self):
        """Test if the returned args is the expected if the env variable exists
        """
        os.environ['FOO'] = 'foo'
        foo = get_arg(env='FOO', default='')

        assert_equal(foo, 'foo')
