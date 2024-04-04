"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import unittest

from gravitino import GravitinoClient, gravitino_metalake
from gravitino.client.gravitino_admin_client import GravitinoAdminClient
from .utils import services_fixtures


@services_fixtures
class TestGravitinoClient(unittest.TestCase):
    def setUp(self):
        self.client = GravitinoAdminClient("http://localhost:8090")

    def test_version(self, *args):
        object = self.client.list_metalakes()
        print("list_metalakes count = " + object.count())


