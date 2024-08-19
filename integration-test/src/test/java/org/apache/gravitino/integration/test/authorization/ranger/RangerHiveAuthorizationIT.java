package org.apache.gravitino.integration.test.authorization.ranger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.AbstractIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class RangerHiveAuthorizationIT extends AbstractIT {
  private static final Logger LOG = LoggerFactory.getLogger(RangerHiveIT.class);

  public static final String metalakeName =
      GravitinoITUtils.genRandomName("RangerHiveAuthorizationIT_metalake");
  public static final String catalogName = GravitinoITUtils.genRandomName("RangerHiveAuthorizationIT_catalog");
  public static final String schemaName = GravitinoITUtils.genRandomName("RangerHiveAuthorizationIT_schema");
  public static final String tableName = GravitinoITUtils.genRandomName("RangerHiveAuthorizationIT_table");

  public static final String HIVE_COL_NAME1 = "hive_col_name1";
  public static final String HIVE_COL_NAME2 = "hive_col_name2";
  public static final String HIVE_COL_NAME3 = "hive_col_name3";

  private static GravitinoMetalake metalake;
  private static Catalog catalog;
  private static final String provider = "hive";
  private static String HIVE_METASTORE_URIS;

  @BeforeAll
  public static void setup() throws Exception {
    RangerITEnv.setup();
    containerSuite.startHiveContainer();
    HIVE_METASTORE_URIS =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), String.valueOf(true));
    configs.put(Configs.SERVICE_ADMINS.getKey(), AuthConstants.ANONYMOUS_USER);
    registerCustomConfigs(configs);
    AbstractIT.startIntegrationTest();

    createMetalake();
    createCatalog();
    createSchema();
    createHiveTable();
  }

  @AfterAll
  public static void stop() throws IOException {
    AbstractIT.client = null;
  }

  @Test
  void testManageRoles() {
    String roleName = "role#123";

    Map<String, String> properties = Maps.newHashMap();
    properties.put("k1", "v1");

    SecurableObject table1 =
        SecurableObjects.parse(String.format("%s.%s.%s", catalogName, schemaName, tableName),
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.SelectTable.allow()));
    Role role = metalake.createRole(roleName, properties, Lists.newArrayList(table1));
    LOG.info("Role created: {}", role);
  }

  private static void createMetalake() {
    GravitinoMetalake[] gravitinoMetalakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetalakes.length);

    GravitinoMetalake createdMetalake =
        client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(metalakeName);
    Assertions.assertEquals(createdMetalake, loadMetalake);

    metalake = loadMetalake;
  }

  private static void createCatalog() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(HiveConstants.METASTORE_URIS, HIVE_METASTORE_URIS);

    metalake.createCatalog(catalogName, Catalog.Type.RELATIONAL, provider, "comment", properties);
    catalog = metalake.loadCatalog(catalogName);
    LOG.info("Catalog created: {}", catalog);
  }

  private static void createSchema() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    properties.put(
        "location",
        String.format(
            "hdfs://%s:%d/user/hive/warehouse/%s.db",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT,
            schemaName.toLowerCase()));
    String comment = "comment";

    catalog.asSchemas().createSchema(schemaName, comment, properties);
    Schema loadSchema = catalog.asSchemas().loadSchema(schemaName);
    Assertions.assertEquals(schemaName.toLowerCase(), loadSchema.name());
  }

  public static void createHiveTable() {
    // Create table from Gravitino API
    Column[] columns = createColumns();
    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);

    Distribution distribution =
        Distributions.of(Strategy.EVEN, 10, NamedReference.field(HIVE_COL_NAME1));

    final SortOrder[] sortOrders =
        new SortOrder[] {
            SortOrders.of(
                NamedReference.field(HIVE_COL_NAME2),
                SortDirection.DESCENDING,
                NullOrdering.NULLS_FIRST)
        };

    Map<String, String> properties = ImmutableMap.of("key1", "val1", "key2", "val2");
    Table createdTable =
        catalog
            .asTableCatalog()
            .createTable(
                nameIdentifier,
                columns,
                "table_comment",
                properties,
                Transforms.EMPTY_TRANSFORM,
                distribution,
                sortOrders);
    LOG.info("Table created: {}", createdTable);
  }

  private static Column[] createColumns() {
    Column col1 = Column.of(HIVE_COL_NAME1, Types.ByteType.get(), "col_1_comment");
    Column col2 = Column.of(HIVE_COL_NAME2, Types.DateType.get(), "col_2_comment");
    Column col3 = Column.of(HIVE_COL_NAME3, Types.StringType.get(), "col_3_comment");
    return new Column[] {col1, col2, col3};
  }
}
