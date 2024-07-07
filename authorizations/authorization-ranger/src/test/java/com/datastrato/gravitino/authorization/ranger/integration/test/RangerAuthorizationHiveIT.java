/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.ranger.integration.test;

import static com.datastrato.gravitino.authorization.SecurableObjects.DOT_SPLITTER;
import static com.datastrato.gravitino.authorization.ranger.RangerAuthorizationHook.MANAGED_BY_GRAVITINO;
import static com.datastrato.gravitino.authorization.ranger.RangerAuthorizationHook.POLICY_ITEM_OWNER_USER;

import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.authorization.Privileges;
import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.authorization.RoleChange;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.authorization.ranger.RangerHiveAuthorizationHook;
import com.datastrato.gravitino.authorization.ranger.RangerRef;
import com.datastrato.gravitino.connector.AuthorizationPropertiesMeta;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.RoleEntity;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// @Tag("gravitino-docker-it")
// @TestInstance(Lifecycle.PER_CLASS)
public class RangerAuthorizationHiveIT { // extends AbstractIT {
  private static final Logger LOG = LoggerFactory.getLogger(RangerAuthorizationHiveIT.class);

  //  private static final String provider = "ranger";

  //  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  //  private static String roleName = GravitinoITUtils.genRandomName("testRole");

  //  static RangerAuthorizationOperations rangerAuthorizationOperations;

  static RangerHiveAuthorizationHook rangerHiveAuthHook;

  private static CatalogEntity hiveCatalog;

  //  private static AuthorizationManager authorizationManager;
  //
  private static final String hiveServiceName = "hivedev";
  private static final String hiveType = "hive";
  private static RangerClient rangerClient;
  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
  //
  //  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  //
  @BeforeAll
  public static void setup() throws RangerServiceException {
    initRangerClient();
    ////    containerSuite.startRangerContainer();
    ////    rangerClient = containerSuite.getRangerContainer().rangerClient;
    //
    createHiveService();

    rangerHiveAuthHook =
        new RangerHiveAuthorizationHook(
            "hive",
            ImmutableMap.of(
                AuthorizationPropertiesMeta.RANGER_ADMIN_URL,
                "http://localhost:6080",
                AuthorizationPropertiesMeta.RANGER_AUTH_TYPE,
                "simple",
                AuthorizationPropertiesMeta.RANGER_USERNAME,
                "admin",
                AuthorizationPropertiesMeta.RANGER_PASSWORD,
                "rangerR0cks!",
                AuthorizationPropertiesMeta.RANGER_SERVICE_NAME,
                "hivedev"));
    //
    //    AuditInfo auditInfo1 =
    //            AuditInfo.builder()
    //                    .withCreator("RangerAuthorizationHiveIT")
    //                    .withCreateTime(Instant.now())
    //                    .build();
    //
    //    hiveCatalog =
    //            CatalogEntity.builder()
    //                    .withId(1L)
    //                    .withName("hive-catalog-test1")
    //                    .withNamespace(Namespace.of("default"))
    //                    .withType(Catalog.Type.RELATIONAL)
    //
    // .withProperties(ImmutableMap.of(AuthorizationManager.AUTHORIZATION_PROVIDER, "ranger",
    //                            AuthorizationPropertiesMeta.RANGER_ADMIN_URL,
    // "http://localhost:6080",
    //                            AuthorizationPropertiesMeta.RANGER_AUTH_TYPE, "simple",
    //                            AuthorizationPropertiesMeta.RANGER_USERNAME, "admin",
    //                            AuthorizationPropertiesMeta.RANGER_PASSWORD, "rangerR0cks!")
    //                    )
    //                    .withProvider("hive")
    //                    .withAuditInfo(auditInfo1)
    //                    .build();
    //
    //    Config config = new Config(false) {};
    //    config.set(Configs.AUTHORIZATION_LOAD_ISOLATED, false);
    //
    //    authorizationManager = new AuthorizationManager(config);
  }

  @AfterAll
  public static void cleanup() throws RangerServiceException {
    //    if (rangerClient != null) {
    //      rangerClient.deleteService(hiveServiceName);
    //    }
  }
  //
  //  @BeforeAll
  //  public static void setup() throws RangerServiceException {
  //    AuditInfo auditInfo1 =
  //            AuditInfo.builder()
  //                    .withCreator("TestAuthorizationChain")
  //                    .withCreateTime(Instant.now())
  //                    .build();
  //
  //    hiveCatalogTest =
  //            CatalogEntity.builder()
  //                    .withId(1L)
  //                    .withName("catalog-test1")
  //                    .withNamespace(Namespace.of("default"))
  //                    .withType(Catalog.Type.RELATIONAL)
  //                    .withProperties(ImmutableMap.of(AuthorizationManager.AUTHORIZATION_PROVIDER,
  // "ranger"))
  //                    .withProvider("hive")
  //                    .withAuditInfo(auditInfo1)
  //                    .build();
  //
  //    Config config = new Config(false) {};
  //    config.set(Configs.AUTHORIZATION_LOAD_ISOLATED, false);
  //
  //    authorizationManager = new AuthorizationManager(config);
  //
  ////    containerSuite.startRangerContainer();
  ////    rangerClient = containerSuite.getRangerContainer().rangerClient;
  //
  ////    RangerHttpClient client = new RangerHttpClient();
  ////    Map<String,String> filter = Collections.emptyMap();
  ////    List<RangerPolicy> policies =
  // client.findPolicies(ImmutableMap.of(SearchFilter.SERVICE_NAME, "hivedev",
  ////            SearchFilter.POLICY_NAME, "testRole_18a2b0c0",
  ////            SearchFilter.RESOURCE_PREFIX + "database",
  // "NEW_ROLE-c493a384-a33a-47e6-82b2-65e8cdc22a62")
  ////    );
  //
  //    /**
  //     * "database" -> {RangerPolicy$RangerPolicyResource@4219}
  // "RangerPolicyResource={values={NEW_ROLE-c493a384-a33a-47e6-82b2-65e8cdc22a62 }
  // isExcludes={false} isRecursive={false} }"
  //     *  key = "database"
  //     *  value = {RangerPolicy$RangerPolicyResource@4219}
  // "RangerPolicyResource={values={NEW_ROLE-c493a384-a33a-47e6-82b2-65e8cdc22a62 }
  // isExcludes={false} isRecursive={false} }"
  //     *   values = {ArrayList@4222}  size = 1
  //     *    0 = "NEW_ROLE-c493a384-a33a-47e6-82b2-65e8cdc22a62"
  //     * */
  ////    rangerAuthorizationOperations = new RangerAuthorizationOperations();
  ////    rangerAuthorizationOperations.initialize(ImmutableMap.of("provider", "ranger"));
  ////
  ////    initRangerClient();
  //
  ////    RangerPolicy policy = rangerClient.getPolicy(11);
  ////    LOG.info("Policy: {}", policy);
  //  }
  //
  //  @AfterAll
  //  public static void cleanup() throws RangerServiceException {
  ////    if (rangerClient != null) {
  ////      rangerClient.deleteService(serviceName);
  ////    }
  //  }

  private static void initRangerClient() throws RangerServiceException {
    final String username = "admin";
    // Apache Ranger Password should be minimum 8 characters with min one alphabet and one numeric.
    final String password = "rangerR0cks!";
    /* for kerberos authentication:
    authType = "kerberos"
    username = principal
    password = path of the keytab file */
    final String authType = "simple";
    String rangerUrl = String.format("http://localhost:%s", 6080);
    rangerClient = new RangerClient(rangerUrl, authType, username, password, null);

    //    RangerPolicy p1 = rangerClient.getPolicy(127);
    //    LOG.info(p1.getName());
  }

    public RoleEntity newColumnRole(String roleName) {
        SecurableObject securableObject1 =
                SecurableObjects.ofNamespace(
                        SecurableObject.Type.COLUMN,
                        Namespace.of("catalog", "db1", "tab1", "column1"),
                        Lists.newArrayList(Privileges.TabularSelect.allow(), Privileges.TabularDrop.allow(), Privileges.TabularAlter.allow()));

        SecurableObject securableObject2 =
                SecurableObjects.ofNamespace(
                        SecurableObject.Type.COLUMN,
                        Namespace.of("catalog", "db2", "tab2", "column2"),
                        Lists.newArrayList(Privileges.TabularSelect.allow(), Privileges.TabularDrop.allow(), Privileges.TabularAlter.allow()));

        return RoleEntity.builder()
                        .withId(1L)
                        .withName(roleName)
                        .withAuditInfo(auditInfo)
                        .withSecurableObjects(Lists.newArrayList(securableObject1, securableObject2))
                        .build();
    }

  @Test
  public void testCreateRole() {
    RoleEntity role = newColumnRole(getCurrentFuncName());

    Assertions.assertTrue(rangerHiveAuthHook.onCreateRole(role));
    assertVerifyRoleAndPolicy(role);
  }

  // This metalake role does not to create Ranger policy. Only use it to help test
  public Role metalakeRole(String roleName) {
    SecurableObject securableObject1 =
            SecurableObjects.ofNamespace(
                    SecurableObject.Type.METALAKE,
                    Namespace.of("metalake"),
                    Lists.newArrayList(Privileges.UseMetalake.allow()));
    RoleEntity role =
            RoleEntity.builder()
                    .withId(1L)
                    .withName(roleName)
                    .withAuditInfo(auditInfo)
                    .withSecurableObjects(Lists.newArrayList(securableObject1))
                    .build();
    return role;
  }

  @Test
  public void testRoleChangeAddSecurableObject() {
    SecurableObject securableObject1 =
        SecurableObjects.ofNamespace(
            SecurableObject.Type.COLUMN,
            Namespace.of("catalog", "db1", "tab1", "column1"),
            Lists.newArrayList(Privileges.TabularSelect.allow()));

    Role mockRole = metalakeRole(getCurrentFuncName());
    // add a securable object to the role
    Assertions.assertTrue(rangerHiveAuthHook.onUpdateRole(mockRole,
            RoleChange.addSecurableObject(securableObject1)));

    // construct a verify role to check if the role and Ranger policy is created correctly
    RoleEntity verifyRole =
            RoleEntity.builder()
                    .withId(1L)
                    .withName(getCurrentFuncName())
                    .withAuditInfo(auditInfo)
                    .withSecurableObjects(Lists.newArrayList(securableObject1))
                    .build();
    assertVerifyRoleAndPolicy(verifyRole);

    // add a new secuable object to the role
      SecurableObject securableObject2 =
              SecurableObjects.ofNamespace(
                      SecurableObject.Type.COLUMN,
                      Namespace.of("catalog", "db2", "tab2", "column2"),
                      Lists.newArrayList(Privileges.TabularSelect.allow(), Privileges.TabularDrop.allow()));
      Assertions.assertTrue(rangerHiveAuthHook.onUpdateRole(mockRole,
              RoleChange.addSecurableObject(securableObject2)));

      // construct a verify role have two securable object to check if the role and Ranger policy is created correctly
      RoleEntity verifyRole2 =
              RoleEntity.builder()
                      .withId(1L)
                      .withName(getCurrentFuncName())
                      .withAuditInfo(auditInfo)
                      .withSecurableObjects(Lists.newArrayList(securableObject1, securableObject2))
                      .build();
      assertVerifyRoleAndPolicy(verifyRole2);
  }

    @Test
    public void testRoleChangeRemoveSecurableObject() {
      RoleEntity role = newColumnRole(getCurrentFuncName());
      Assertions.assertTrue(rangerHiveAuthHook.onCreateRole(role));

      Role metaRole = metalakeRole(getCurrentFuncName());

        // remove a securable object from role
      List<SecurableObject> securableObjects = new ArrayList<>(role.securableObjects());
      SecurableObject securableObject0 = securableObjects.remove(0);
        Assertions.assertTrue(rangerHiveAuthHook.onUpdateRole(metaRole,
                RoleChange.removeSecurableObject(securableObject0)));

        // construct a verify role to check if the role and Ranger policy is created correctly
        RoleEntity verifyRole =
                RoleEntity.builder()
                        .withId(1L)
                        .withName(getCurrentFuncName())
                        .withAuditInfo(auditInfo)
                        .withSecurableObjects(Lists.newArrayList(securableObjects))
                        .build();
        assertVerifyRoleAndPolicy(verifyRole);

      // remove a securable object from role again
      SecurableObject securableObject1 = securableObjects.remove(0);
      Assertions.assertTrue(rangerHiveAuthHook.onUpdateRole(metaRole,
              RoleChange.removeSecurableObject(securableObject1)));

      // construct a verify role to check if the role and Ranger policy is created correctly
      RoleEntity verifyRole2 =
              RoleEntity.builder()
                      .withId(1L)
                      .withName(getCurrentFuncName())
                      .withAuditInfo(auditInfo)
                      .withSecurableObjects(Lists.newArrayList(securableObjects))
                      .build();
      assertVerifyRoleAndPolicy(verifyRole2);
    }

  @Test
  public void testRoleChangeUpdateSecurableObject() {
    RoleEntity role = newColumnRole(getCurrentFuncName());
    Assertions.assertTrue(rangerHiveAuthHook.onCreateRole(role));

    Role metaRole = metalakeRole(getCurrentFuncName());

    // update a securable object from role
    List<SecurableObject> securableObjects = new ArrayList<>(role.securableObjects());
    SecurableObject oldSecurableObject = securableObjects.remove(0);

    // Keep same namespace and type, but change privileges
    SecurableObject newSecurableObject =
            SecurableObjects.ofNamespace(oldSecurableObject.type(),
                    Namespace.of(oldSecurableObject.fullName().split("\\.")),
                    Lists.newArrayList(Privileges.TabularSelect.allow()));

    Assertions.assertTrue(rangerHiveAuthHook.onUpdateRole(metaRole,
            RoleChange.updateSecurableObject(oldSecurableObject, newSecurableObject)));

    // construct a verify role to check if the role and Ranger policy is created correctly
    RoleEntity verifyRole =
            RoleEntity.builder()
                    .withId(1L)
                    .withName(getCurrentFuncName())
                    .withAuditInfo(auditInfo)
                    .withSecurableObjects(Lists.newArrayList(newSecurableObject))
                    .build();
    assertVerifyRoleAndPolicy(verifyRole);
  }

  public void testCreateRole1() throws RangerServiceException {
    String dbName = "db1";
    String tabName = "tab1";
    String colName = "column1";
    SecurableObject securableObject1 =
        SecurableObjects.ofNamespace(
            SecurableObject.Type.COLUMN,
            Namespace.of("catalog", dbName, tabName, colName),
            Lists.newArrayList(Privileges.TabularSelect.allow(), Privileges.TabularDrop.allow()));

    String roleName = GravitinoITUtils.genRandomName("testRole1");
    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName(roleName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(securableObject1))
            .build();

    Assertions.assertTrue(rangerHiveAuthHook.onCreateRole(role));

    String policyName =
        rangerHiveAuthHook.formatPolicyName(role.name(), securableObject1.fullName());
    RangerPolicy policy = rangerClient.getPolicy(hiveServiceName, policyName);
    Assertions.assertEquals(policy.getName(), policyName);
    Assertions.assertTrue(policy.getPolicyLabels().contains(MANAGED_BY_GRAVITINO));
    Assertions.assertTrue(
        policy.getResources().get(RangerRef.RESOURCE_DATABASE).getValues().contains(dbName));
    Assertions.assertTrue(
        policy.getResources().get(RangerRef.RESOURCE_TABLE).getValues().contains(tabName));
    Assertions.assertTrue(
        policy.getResources().get(RangerRef.RESOURCE_COLUMN).getValues().contains(colName));

    Assertions.assertEquals(
        rangerHiveAuthHook.translatePrivilege(Privileges.TabularSelect.allow().name()),
        policy.getPolicyItems().get(0).getAccesses().get(0).getType());

    List<String> accessTypes = Lists.newArrayList();
    policy
        .getPolicyItems()
        .forEach(
            policyItem -> {
              Assertions.assertEquals(policyItem.getUsers().size(), 1);
              Assertions.assertEquals(policyItem.getUsers().get(0), POLICY_ITEM_OWNER_USER);
              Assertions.assertEquals(policyItem.getAccesses().size(), 1);
              accessTypes.add(policyItem.getAccesses().get(0).getType());
            });
    Assertions.assertTrue(
        accessTypes.contains(
            rangerHiveAuthHook.translatePrivilege(Privileges.TabularSelect.allow().name())));
    Assertions.assertTrue(
        accessTypes.contains(
            rangerHiveAuthHook.translatePrivilege(Privileges.TabularDrop.allow().name())));
  }

  public void assertVerifyRoleAndPolicy(RoleEntity role) {
    role.securableObjects()
            .forEach(
                    securableObject -> {
                      // Each securableObject creates a policy
                      String policyName =
                              rangerHiveAuthHook.formatPolicyName(role.name(), securableObject.fullName());
                      RangerPolicy policy = null;
                      try {
                        policy = rangerClient.getPolicy(hiveServiceName, policyName);
                      } catch (RangerServiceException e) {
                        throw new RuntimeException(e);
                      }
                      LOG.info("assertVerifyRoleAndPolicy: " + policy.toString());
                      Assertions.assertEquals(policy.getName(), policyName);
                      Assertions.assertTrue(policy.getPolicyLabels().contains(MANAGED_BY_GRAVITINO));

                      // verify namespace
                      List<String> resRole =
                              Lists.newArrayList(
                                      DOT_SPLITTER.splitToList(securableObject.fullName()));
                      resRole.remove(0); // skip catalog
                      List<String> resPolicy =
                              Lists.newArrayList(
                                      policy.getResources().get(RangerRef.RESOURCE_DATABASE).getValues().get(0),
                                      policy.getResources().get(RangerRef.RESOURCE_TABLE).getValues().get(0),
                                      policy.getResources().get(RangerRef.RESOURCE_COLUMN).getValues().get(0));
                      Assertions.assertEquals(resRole, resPolicy);

                      // verify role's privileges and policy's accesses
                      RangerPolicy finalPolicy = policy;
                      securableObject
                              .privileges()
                              .forEach(
                                      privilege -> {
                                        Assertions.assertTrue(
                                                finalPolicy.getPolicyItems().stream()
                                                        .anyMatch(
                                                                policyItem -> {
                                                                  return policyItem.getAccesses().stream()
                                                                          .anyMatch(
                                                                                  access -> {
                                                                                    return access
                                                                                            .getType()
                                                                                            .equals(
                                                                                                    rangerHiveAuthHook.translatePrivilege(
                                                                                                            privilege.name()));
                                                                                  });
                                                                }));
                                      });
                    });

    //
    //    String policyName = rangerHiveAuthHook.formatPolicyName(role.name(),
    // role.securableObjects().get(0).fullName());
    //    RangerPolicy policy = rangerClient.getPolicy(hiveServiceName, policyName);
    //    Assertions.assertEquals(policy.getName(), policyName);
    //    Assertions.assertTrue(policy.getPolicyLabels().contains(MANAGED_BY_GRAVITINO));
    //
    //    // verify namespace
    //    List<String> resRole =
    // Lists.newArrayList(SecurableObjects.DOT_SPLITTER.splitToList(role.securableObjects().get(0).fullName()));
    //    resRole.remove(0); // skip catalog
    //    List<String> resPolicy =
    // Lists.newArrayList(policy.getResources().get(RangerRef.RESOURCE_DATABASE).getValues().get(0),
    //            policy.getResources().get(RangerRef.RESOURCE_TABLE).getValues().get(0),
    //            policy.getResources().get(RangerRef.RESOURCE_COLUMN).getValues().get(0));
    //    Assertions.assertEquals(resRole, resPolicy);
    //
    //    Assertions.assertEquals(role.securableObjects().size(), policy.getPolicyItems().size());
    //    role.securableObjects().forEach(securableObject -> {
    //      securableObject.privileges().forEach(privilege -> {
    //        Assertions.assertTrue(policy.getPolicyItems().stream().anyMatch(policyItem -> {
    //          return policyItem.getAccesses().stream().anyMatch(access -> {
    //            return
    // access.getType().equals(rangerHiveAuthHook.translatePrivilege(privilege.name()));
    //          });
    //        }));
    //      });
    //    });
    //
    ////
    // Assertions.assertEquals(rangerHiveAuthHook.translatePrivilege(Privileges.TabularSelect.allow().name()),
    ////            policy.getPolicyItems().get(0).getAccesses().get(0).getType());
    //
    ////    List<String> accessTypes = Lists.newArrayList();
    //    policy.getPolicyItems().forEach(
    //            policyItem -> {
    //              Assertions.assertEquals(policyItem.getUsers().size(), 1);
    //              Assertions.assertEquals(policyItem.getUsers().get(0), POLICY_ITEM_OWNER_USER);
    //              Assertions.assertEquals(policyItem.getAccesses().size(), 1);
    //              Assertions.assertTrue(policyItem.getAccesses().stream().anyMatch(access -> {
    //                return
    // access.getType().equals(rangerHiveAuthHook.translatePrivilege(Privileges.TabularSelect.allow().name()));
    //              }));
    //            });
    ////
    // Assertions.assertTrue(accessTypes.contains(rangerHiveAuthHook.translatePrivilege(Privileges.TabularSelect.allow().name())));
    ////
    // Assertions.assertTrue(accessTypes.contains(rangerHiveAuthHook.translatePrivilege(Privileges.TabularDrop.allow().name())));
  }

  /**
   * Access Control Syntax: GRANT <privilege_type> ON <securable_object> TO <principal>
   * Access Control Sample: GRANT SELECT ON TABLE <schema-name>.<table-name> TO users
   * */
  @Test
  public void grantPrivilegeToObjectToRole() {
    //    SecurableObject securableObjectTable = SecurableObjects.ofNamespace(
    //            SecurableObject.Type.TABLE,
    //            Namespace.of("db1", "tab1", "column1"),
    //            Lists.newArrayList(Privileges.TabularSelect.allow()));
    //    rangerAuthorizationOperations.updateRole(roleName,
    //            RoleChange.addSecurableObject(securableObjectTable));

  }

  public static void createHiveService() throws RangerServiceException {
    try {
      if (null != rangerClient.getService(hiveServiceName)) {
        return;
      }
    } catch (RangerServiceException e) {
      LOG.error("Error while fetching service: {}", e.getMessage());
    }

    String usernameKey = "username";
    String usernameVal = "admin";
    String passwordKey = "password";
    String passwordVal = "admin";
    String jdbcKey = "jdbc.driverClassName";
    String jdbcVal = "org.apache.hive.jdbc.HiveDriver";
    String jdbcUrlKey = "jdbc.url";
    String jdbcUrlVal = "jdbc:hive2://172.17.0.2:10000";

    RangerService service = new RangerService();
    service.setType(hiveType);
    service.setName(hiveServiceName);
    service.setConfigs(
        ImmutableMap.<String, String>builder()
            .put(usernameKey, usernameVal)
            .put(passwordKey, passwordVal)
            .put(jdbcKey, jdbcVal)
            .put(jdbcUrlKey, jdbcUrlVal)
            .build());

    RangerService createdService = rangerClient.createService(service);
    Assertions.assertNotNull(createdService);

    Map<String, String> filter = Collections.emptyMap();
    List<RangerService> services = rangerClient.findServices(filter);
    Assertions.assertEquals(services.get(0).getName(), hiveServiceName);
    Assertions.assertEquals(services.get(0).getType(), hiveType);
    Assertions.assertEquals(services.get(0).getConfigs().get(usernameKey), usernameVal);
    Assertions.assertEquals(services.get(0).getConfigs().get(jdbcKey), jdbcVal);
    Assertions.assertEquals(services.get(0).getConfigs().get(jdbcUrlKey), jdbcUrlVal);
  }

  public static String getCurrentFuncName() {
    return Thread.currentThread().getStackTrace()[2].getMethodName();
  }
}
