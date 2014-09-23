import unittest

from happybase_mock import Connection


class BaseTestCase(unittest.TestCase):

    def tearDown(self):
        # Delete all connection instances
        Connection._instances.clear()
