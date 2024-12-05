/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.authorization.ranger.integration.test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Schema;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.connector.properties.AuthorizationPropertiesMeta;
import org.apache.gravitino.connector.authorization.AuthorizationPluginProvider;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.container.RangerContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.spark.sql.SparkSession;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.apache.gravitino.Catalog.AUTHORIZATION_PROVIDER;
import static org.apache.gravitino.authorization.ranger.integration.test.RangerITEnv.currentFunName;
import static org.apache.gravitino.authorization.ranger.integration.test.RangerITEnv.verifyRoleInRanger;
import static org.apache.gravitino.catalog.hive.HiveConstants.IMPERSONATION_ENABLE;
import static org.apache.gravitino.integration.test.container.RangerContainer.RANGER_SERVER_PORT;

@Tag("gravitino-docker-test")
public class RangerHadoopE2EIT extends RangerBaseE2EIT {
  private static final Logger LOG = LoggerFactory.getLogger(RangerHadoopE2EIT.class);
private static final String provider = "hive";
  private HiveClientPool hiveClientPool;
  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    metalakeName = GravitinoITUtils.genRandomName("metalake").toLowerCase();
    // Enable Gravitino Authorization mode
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), String.valueOf(true));
    configs.put(Configs.SERVICE_ADMINS.getKey(), RangerITEnv.HADOOP_USER_NAME);
    configs.put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.SIMPLE.name().toLowerCase());
    configs.put("SimpleAuthUserName", AuthConstants.ANONYMOUS_USER);
    registerCustomConfigs(configs);
    super.startIntegrationTest();

    RangerITEnv.init();
    RangerITEnv.startHiveRangerContainer();
    RANGER_ADMIN_URL =
            String.format(
                    "http://%s:%d",
                    containerSuite.getRangerContainer().getContainerIpAddress(), RANGER_SERVER_PORT);
    HIVE_METASTORE_URIS =
            String.format(
                    "thrift://%s:%d",
                    containerSuite.getHiveRangerContainer().getContainerIpAddress(),
                    HiveContainer.HIVE_METASTORE_PORT);

    HiveConf hiveConf = new HiveConf();
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, HIVE_METASTORE_URIS);
    // Check if Hive client can connect to Hive metastore
    hiveClientPool = new HiveClientPool(1, hiveConf);

    generateRangerSparkSecurityXML();

    sparkSession =
            SparkSession.builder()
                    .master("local[1]")
                    .appName("Ranger Hive E2E integration test")
                    .config("hive.metastore.uris", HIVE_METASTORE_URIS)
                    .config(
                            "spark.sql.warehouse.dir",
                            String.format(
                                    "hdfs://%s:%d/user/hive/warehouse",
                                    containerSuite.getHiveRangerContainer().getContainerIpAddress(),
                                    HiveContainer.HDFS_DEFAULTFS_PORT))
                    .config("spark.sql.storeAssignmentPolicy", "LEGACY")
                    .config("mapreduce.input.fileinputformat.input.dir.recursive", "true")
                    .config(
                            "spark.sql.extensions",
                            "org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension")
                    .enableHiveSupport()
                    .getOrCreate();

    createMetalake();
    createCatalog();
    createSchema();

    RangerITEnv.cleanup();
    metalake.addUser(System.getenv(HADOOP_USER_NAME));
  }

  @AfterAll
  public void stop() {
    cleanIT();
  }

  @AfterEach
  public void clean() {
    RangerITEnv.cleanAllPolicy(RangerITEnv.RANGER_HIVE_REPO_NAME);
  }

  /**
   * Create a mock role with 3 securable objects <br>
   * 1. catalog.db1.tab1 with CREATE_TABLE privilege. <br>
   * 2. catalog.db1.tab2 with SELECT_TABLE privilege. <br>
   * 3. catalog.db1.tab3 with MODIFY_TABLE privilege. <br>
   *
   * @param roleName The name of the role must be unique in this test class
   */
  public RoleEntity mock3TableRole(String roleName) {
    SecurableObject securableObject1 =
        SecurableObjects.parse(
            String.format("catalog.%s", roleName), // use unique db name to avoid conflict
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.CreateTable.allow()));

    SecurableObject securableObject2 =
        SecurableObjects.parse(
            String.format("catalog.%s.tab2", roleName),
            SecurableObject.Type.TABLE,
            Lists.newArrayList(Privileges.SelectTable.allow()));

    SecurableObject securableObject3 =
        SecurableObjects.parse(
            String.format("catalog.%s.tab3", roleName),
            SecurableObject.Type.TABLE,
            Lists.newArrayList(Privileges.ModifyTable.allow()));

    return RoleEntity.builder()
        .withId(1L)
        .withName(roleName)
        .withAuditInfo(auditInfo)
        .withSecurableObjects(
            Lists.newArrayList(securableObject1, securableObject2, securableObject3))
        .build();
  }

  // Use the different db.table different privilege to test OnRoleCreated()
  @Test
  public void testOnRoleCreated() {
    RoleEntity role = mock3TableRole(currentFunName());
    LOG.info("Role created: {}", role);
//    Assertions.assertTrue(rangerAuthHDFSPlugin.onRoleCreated(role));
//    verifyRoleInRanger(rangerAuthHDFSPlugin, role);
  }

  @Override
  public void createCatalog() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(HiveConstants.METASTORE_URIS, HIVE_METASTORE_URIS);
    properties.put(IMPERSONATION_ENABLE, "true");

    properties.put(AUTHORIZATION_PROVIDER, AuthorizationPluginProvider.Type.Chain.getName());
    properties.put(AuthorizationPropertiesMeta.CHAIN_PLUGINS, "hive1,hdfs1");
    properties.put("authorization.chain.hive1.catalog-provider", "hive");
    properties.put("authorization.chain.hive1.provider", "ranger");
    properties.put("authorization.chain.hive1.ranger.auth.type", RangerContainer.authType);
    properties.put("authorization.chain.hive1.ranger.admin.url", RANGER_ADMIN_URL);
    properties.put("authorization.chain.hive1.ranger.username", RangerContainer.rangerUserName);
    properties.put("authorization.chain.hive1.ranger.password", RangerContainer.rangerPassword);
    properties.put("authorization.chain.hive1.ranger.service.name", RangerITEnv.RANGER_HIVE_REPO_NAME);
    properties.put("authorization.chain.hdfs1.catalog-provider", "hadoop");
    properties.put("authorization.chain.hdfs1.provider", "ranger");
    properties.put("authorization.chain.hdfs1.ranger.auth.type", RangerContainer.authType);
    properties.put("authorization.chain.hdfs1.ranger.admin.url", RANGER_ADMIN_URL);
    properties.put("authorization.chain.hdfs1.ranger.username", RangerContainer.rangerUserName);
    properties.put("authorization.chain.hdfs1.ranger.password", RangerContainer.rangerPassword);
    properties.put("authorization.chain.hdfs1.ranger.service.name", RangerITEnv.RANGER_HDFS_REPO_NAME);

    metalake.createCatalog(catalogName, Catalog.Type.RELATIONAL, provider, "comment", properties);
    catalog = metalake.loadCatalog(catalogName);
    LOG.info("Catalog created: {}", catalog);
  }

  private void createSchema() throws TException, InterruptedException {
    Map<String, String> schemaProperties = new HashMap<>();
    schemaProperties.put(
            "location",
            String.format(
                    "hdfs://%s:%d/user/hive/warehouse/%s.db",
                    containerSuite.getHiveRangerContainer().getContainerIpAddress(),
                    HiveContainer.HDFS_DEFAULTFS_PORT,
                    schemaName.toLowerCase()));
    String comment = "comment";
    catalog.asSchemas().createSchema(schemaName, comment, schemaProperties);
    Schema loadSchema = catalog.asSchemas().loadSchema(schemaName);
    Assertions.assertEquals(schemaName.toLowerCase(), loadSchema.name());
    Assertions.assertEquals(comment, loadSchema.comment());
    Assertions.assertEquals("val1", loadSchema.properties().get("key1"));
    Assertions.assertEquals("val2", loadSchema.properties().get("key2"));
    Assertions.assertNotNull(loadSchema.properties().get(HiveConstants.LOCATION));

    // Directly get database from Hive metastore to verify the schema creation
    Database database = hiveClientPool.run(client -> client.getDatabase(schemaName));
    Assertions.assertEquals(schemaName.toLowerCase(), database.getName());
    Assertions.assertEquals(comment, database.getDescription());
    Assertions.assertEquals("val1", database.getParameters().get("key1"));
    Assertions.assertEquals("val2", database.getParameters().get("key2"));
  }

  @Override
  protected void checkTableAllPrivilegesExceptForCreating() {

  }

  @Override
  protected void checkUpdateSQLWithReadWritePrivileges() {

  }

  @Override
  protected void checkUpdateSQLWithReadPrivileges() {

  }

  @Override
  protected void checkUpdateSQLWithWritePrivileges() {

  }

  @Override
  protected void checkDeleteSQLWithReadWritePrivileges() {

  }

  @Override
  protected void checkDeleteSQLWithReadPrivileges() {

  }

  @Override
  protected void checkDeleteSQLWithWritePrivileges() {

  }

  @Override
  protected void useCatalog() throws InterruptedException {

  }

  @Override
  protected void checkWithoutPrivileges() {

  }

  @Override
  protected void testAlterTable() {

  }
}
