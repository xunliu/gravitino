"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
import http
import json
import unittest

import requests

from gravitino.client.gravitino_admin_client import GravitinoAdminClient
from gravitino.dto.dto_converters import DTOConverters
from gravitino.dto.requests.metalake_updates_request import MetalakeUpdatesRequest
from gravitino.dto.responses.metalake_response import MetalakeResponse
from gravitino.exceptions import NoSuchMetalakeException
from gravitino.meta_change import MetalakeChange
from gravitino.name_identifier import NameIdentifier
from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.utils.exceptions import NotFoundError


class TestGravitinoClient(unittest.TestCase):
    def setUp(self):
        self.client = GravitinoAdminClient("http://localhost:8090")

    def test_create_metalake(self, *args):
        name = "example_name32"
        comment = "This is a sample comment"

        ident = NameIdentifier.of(name)
        properties = {"key1": "value1", "key2": "value2"}

        # 创建AdminClientBuilder实例
        admin_client_builder = GravitinoAdminClient.Builder(uri="http://localhost:8090")

        # 调用build方法获取GravitinoAdminClient实例
        gravitino_admin_client = admin_client_builder.build()

        # 调用create_metalake方法创建Metalake
        gravitinoMetalake = gravitino_admin_client.create_metalake(ident, comment, properties)

        self.assertEqual(gravitinoMetalake.name, name)
        self.assertEqual(gravitinoMetalake.comment, comment)
        self.assertEqual(gravitinoMetalake.properties.get("key1"), "value1")
        self.assertEqual(gravitinoMetalake.audit.creator, "anonymous")

    def test_alter_metalake(self):
        gravitino_admin_client = GravitinoAdminClient.Builder(uri="http://localhost:8090").build()
        new_name = "example_name21_new" #RandomNameUtils.gen_random_name("newmetaname")

        metalake_name_a = "example_name21"  # Assuming this is set or generated elsewhere in your test
        # self.client.create_metalake(NameIdentifier.parse(metalake_name_a), "metalake A comment", {})

        changes = (
            MetalakeChange.rename(new_name),
            MetalakeChange.update_comment("new metalake comment"),
        )

        metalake = gravitino_admin_client.alter_metalake(NameIdentifier.of(metalake_name_a), *changes)
        self.assertEqual(new_name, metalake.name)
        self.assertEqual("new metalake comment", metalake.comment)
        self.assertEqual("anonymous", metalake.audit.creator)  # Assuming a constant or similar attribute

        # Reload metadata via new name to check if the changes are applied
        new_metalake = gravitino_admin_client.load_metalake(NameIdentifier.of(new_name))
        self.assertEqual(new_name, new_metalake.name)
        self.assertEqual("new metalake comment", new_metalake.comment)

        # Old name does not exist
        old = NameIdentifier.of(metalake_name_a)
        with self.assertRaises(NotFoundError): #NoSuchMetalakeException):
            gravitino_admin_client.load_metalake(old)

    def test_drop_metalake(self, *args):
        name = "example_name31"
        ident = NameIdentifier.of(name)

        gravitino_admin_client = GravitinoAdminClient.Builder(uri="http://localhost:8090").build()
        self.assertTrue(gravitino_admin_client.drop_metalake(ident))

    def test_metalake_update_request_to_json(self):
        changes = (
            MetalakeChange.rename("my_metalake_new"),
            MetalakeChange.update_comment("new metalake comment"),
        )
        reqs = [DTOConverters.to_metalake_update_request(change) for change in changes]
        updates_request = MetalakeUpdatesRequest(reqs)
        valid_json = (f'{{"updates": [{{"@type": "rename", "newName": "my_metalake_new"}}, {{"@type": "updateComment", '
                      f'"newComment": "new metalake comment"}}]}}')
        self.assertEqual(updates_request.to_json(), valid_json)

    def test_from_json_metalake_response(self, *args):
        str = (b'{"code":0,"metalake":{"name":"example_name18","comment":"This is a sample comment","properties":{'
               b'"key1":"value1","key2":"value2"},"audit":{"creator":"anonymous",'
               b'"createTime":"2024-04-05T10:10:35.218Z"}}}')
        metalake_response = MetalakeResponse.from_json(str)
        self.assertEqual(metalake_response.code, 0)
        self.assertIsNotNone(metalake_response.metalake)
        self.assertEqual(metalake_response.metalake.name, "example_name18")
        self.assertEqual(metalake_response.metalake.audit.creator, "anonymous")


    def test_list_metalakes(self, *args):
        object = self.client.list_metalakes()
        print("list_metalakes count = {}".format(len(object)))