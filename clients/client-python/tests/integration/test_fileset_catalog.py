"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
import logging
from typing import Dict

from packaging.metadata import Metadata

from gravitino.api.catalog import Catalog
from gravitino.api.fileset import Fileset
from gravitino.api.fileset_change import FilesetChange
from gravitino.client.gravitino_admin_client import GravitinoAdminClient
from gravitino.client.gravitino_client import GravitinoClient
from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.dto.catalog_dto import CatalogDTO
from gravitino.name_identifier import NameIdentifier
from tests.integration.integration_test_env import IntegrationTestEnv

logger = logging.getLogger(__name__)


class TestFilesetCatalog(IntegrationTestEnv):
    catalog: Catalog = None
    metalake_name: str = "testMetalake"
    metalake_comment: str = "metalake_comment"
    metalake_properties: Dict[str, str] = {"metalake_k1": "metalake_v1"}

    catalog_name: str = "testCatalog"
    catalog_comment: str = "catalog_comment"
    catalog_properties: str = {"catalog_k1": "catalog_v1"}

    schema_name: str = "testSchema"
    schema_comment: str = "schema_comment"

    fileset_name: str = "testFileset1"
    fileset_alter_name: str = "testFilesetAlter"
    provider: str = "hadoop"

    metalake_ident: NameIdentifier = NameIdentifier.of(metalake_name)
    catalog_ident: NameIdentifier = NameIdentifier.of_catalog(metalake_name, catalog_name)
    schema_ident: NameIdentifier = NameIdentifier.of_schema(metalake_name, catalog_name, schema_name)
    fileset_ident: NameIdentifier = NameIdentifier.of_fileset(metalake_name, catalog_name, schema_name, fileset_name)
    fileset_alter_ident: NameIdentifier = NameIdentifier.of_fileset(metalake_name, catalog_name, schema_name,
                                                                    fileset_alter_name)

    gravitino_admin_client: GravitinoAdminClient = None
    gravitino_client: GravitinoClient = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.clean_test_data()

        cls.gravitino_admin_client = GravitinoAdminClient(uri="http://localhost:8090")
        # cls.gravitino_admin_client.create_metalake(ident=cls.metalake_ident,
        #                                            comment=cls.metalake_comment, properties=cls.metalake_properties)
        #
        # cls.gravitino_client = GravitinoClient(uri="http://localhost:8090", metalake_name=cls.metalake_name)
        #
        # cls.catalog = cls.gravitino_client.create_catalog(
        #     ident=cls.catalog_ident,
        #     type=CatalogDTO.Type.FILESET,
        #     provider=cls.provider,
        #     comment=cls.catalog_comment,
        #     properties={"k1": "v1"}
        # )
        #
        # cls.catalog.as_schemas().create_schema(ident=cls.schema_ident, comment="comment", properties={"k1": "v1"})

    @classmethod
    def tearDownClass(cls):
        """Clean test data"""
        cls.clean_test_data()
        super().tearDownClass()

    def tearDown(self):
        """Clean test data"""
        self.clean_test_data()

    @classmethod
    def clean_test_data(cls):
        try:
            cls.gravitino_admin_client = GravitinoAdminClient(uri="http://localhost:8090")
            gravitino_metalake = cls.gravitino_admin_client.load_metalake(ident=cls.metalake_ident)
            cls.catalog = gravitino_metalake.load_catalog(ident=cls.catalog_ident)
            cls.catalog.as_fileset_catalog().drop_fileset(ident=cls.fileset_ident)
            cls.catalog.as_fileset_catalog().drop_fileset(ident=cls.fileset_alter_ident)
            cls.catalog.as_schemas().drop_schema(ident=cls.schema_ident, cascade=True)
            gravitino_metalake.drop_catalog(ident=cls.catalog_ident)
            cls.gravitino_admin_client.drop_metalake(cls.metalake_ident)
        except Exception as e:
            logger.debug(e)

    def creata_metalake(self) -> GravitinoMetalake:
        return self.gravitino_admin_client.create_metalake(ident=self.metalake_ident,
                                                           comment=self.metalake_comment,
                                                           properties=self.metalake_properties)

    def test_a_create_metalake(self):
        metalake = self.creata_metalake()
        assert metalake.name() == self.metalake_ident.name()
        assert metalake.comment() == self.metalake_comment
        assert metalake.properties() == self.metalake_properties

        self.gravitino_admin_client.drop_metalake(ident=self.metalake_ident)

    def test_b_load_metalake(self):
        self.creata_metalake()
        metalake = self.gravitino_admin_client.load_metalake(ident=self.metalake_ident)
        assert metalake.name() == self.metalake_ident.name()
        assert metalake.comment() == self.metalake_comment
        assert metalake.properties() == self.metalake_properties
        self.gravitino_admin_client.drop_metalake(ident=self.metalake_ident)

    def test_c_alter_metalake(self):
        self.creata_metalake()
        metalake = self.gravitino_admin_client.alter_metalake(ident=self.metalake_ident)

        self.gravitino_admin_client.drop_metalake(ident=self.metalake_ident)

    def test_create_fileset(self):
        fileset = self.catalog.as_fileset_catalog().create_fileset(ident=self.fileset_ident,
                                                                   type=Fileset.Type.MANAGED,
                                                                   comment="mock comment",
                                                                   storage_location="mock location",
                                                                   properties={"k1": "v1"})
        assert fileset is not None

        fileset_list = self.catalog.as_fileset_catalog().list_filesets(self.fileset_ident.namespace())
        assert fileset_list is not None and len(fileset_list) == 1

        fileset = self.catalog.as_fileset_catalog().load_fileset(self.fileset_ident)
        assert fileset is not None
        assert fileset._name() == self.fileset_ident.name()

        # Alter fileset
        changes = (
            FilesetChange.rename(self.fileset_alter_name),
            FilesetChange.update_comment("new fileset comment"),
            FilesetChange.set_property("key1", "value1"),
            FilesetChange.remove_property("k1"),
        )
        fileset_alter = self.catalog.as_fileset_catalog().alter_fileset(self.fileset_ident, *changes)
        assert fileset_alter is not None
        assert fileset_alter.name() == self.fileset_alter_name
        assert fileset_alter.comment() == "new fileset comment"
        assert fileset_alter.properties().get("key1") == "value1"

        # Clean test data
        self.catalog.as_fileset_catalog().drop_fileset(ident=self.fileset_ident)

    def test_schema(self):
        schema = self.catalog.as_schemas().load_schema(self.schema_ident)
        assert schema.name() == self.schema_ident.name()
