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
package org.apache.gravitino.integration.test.authorization.ranger;

import static org.apache.gravitino.authorization.SecurableObjects.DOT_SPLITTER;
import static org.apache.gravitino.authorization.ranger.RangerAuthorizationPlugin.OWNER_ROLE_NAME;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.ranger.RangerAuthorizationPlugin;
import org.apache.gravitino.authorization.ranger.RangerDefines;
import org.apache.gravitino.authorization.ranger.RangerHiveAuthorizationPlugin;
import org.apache.gravitino.connector.AuthorizationPropertiesMeta;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.RangerContainer;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.utils.RandomNameUtils;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
public class RangerAuthorizationHiveIT extends RangerHiveIT {
  private static final Logger LOG = LoggerFactory.getLogger(RangerAuthorizationHiveIT.class);
  static RangerHiveAuthorizationPlugin rangerHiveAuthPlugin;
  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  @BeforeAll
  public static void setup() {
    RangerHiveIT.setup();

    rangerHiveAuthPlugin =
        new RangerHiveAuthorizationPlugin(
            "hive",
            ImmutableMap.of(
                AuthorizationPropertiesMeta.RANGER_ADMIN_URL,
                    String.format(
                        "http://%s:%d",
                        containerSuite.getRangerContainer().getContainerIpAddress(),
                        RangerContainer.RANGER_SERVER_PORT),
                AuthorizationPropertiesMeta.RANGER_AUTH_TYPE, RangerContainer.authType,
                AuthorizationPropertiesMeta.RANGER_USERNAME, RangerContainer.rangerUserName,
                AuthorizationPropertiesMeta.RANGER_PASSWORD, RangerContainer.rangerPassword,
                AuthorizationPropertiesMeta.RANGER_SERVICE_NAME,
                    RangerITEnv.RANGER_HIVE_REPO_NAME));
  }

  @AfterAll
  public static void cleanup() {
    RangerHiveIT.cleanup();
  }

  /**
   * Create a mock role with 3 securable objects <br>
   * 1. catalog.db1.tab1 with CREATE_TABLE privilege. <br>
   * 2. catalog.db1.tab2 with SELECT_TABLE privilege. <br>
   * 3. catalog.db1.tab3 with MODIFY_TABLE privilege. <br>
   *
   * @param roleName The name of the role, must be unique in this test class
   */
  public RoleEntity mock3TableRole(String roleName) {
    SecurableObject securableObject1 =
        SecurableObjects.parse(
            String.format("catalog.%s.tab1", roleName), // use unique db name to avoid conflict
            MetadataObject.Type.TABLE,
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

  /**
   * Create a mock Owner privileges role <br>
   *
   * @param callFuncName The name of the metadata, must be unique in this test class
   */
  public RoleEntity mockOwnerTableRole(String callFuncName) {
    SecurableObject securableObject =
        SecurableObjects.parse(
            String.format("catalog.%s.tab1", callFuncName), // use unique db name to avoid conflict
            MetadataObject.Type.TABLE,
            Lists.newArrayList(rangerHiveAuthPlugin.getAllPrivileges()));

    return RoleEntity.builder()
        .withId(1L)
        .withName(OWNER_ROLE_NAME)
        .withAuditInfo(auditInfo)
        .withSecurableObjects(Lists.newArrayList(securableObject))
        .build();
  }

  // Use the different db.table different privilege to test OnRoleCreated()
  @Test
  public void testOnRoleCreated() {
    RoleEntity role = mock3TableRole(currentFunName());
    Assertions.assertTrue(rangerHiveAuthPlugin.onRoleCreated(role));
    verifyRoleInRanger(role, null, null, null, null);
  }

  @Test
  public void testOnRoleDeleted() {
    // prepare create a role
    RoleEntity role = mock3TableRole(currentFunName());
    Assertions.assertTrue(rangerHiveAuthPlugin.onRoleCreated(role));

    // delete this role
    Assertions.assertTrue(rangerHiveAuthPlugin.onRoleDeleted(role));
    role.securableObjects().stream()
        .forEach(
            securableObject ->
                Assertions.assertNull(rangerHiveAuthPlugin.findManagedPolicy(securableObject)));
  }

  @Test
  public void testOnRoleAcquired() {
    // Because Ranger support wildcard to match the policy, so we need to test the policy with
    // wildcard
    String currentFunName = currentFunName();
    createHivePolicy(Lists.newArrayList("*"), RandomNameUtils.genRandomName(currentFunName));
    createHivePolicy(Lists.newArrayList("*", "*"), RandomNameUtils.genRandomName(currentFunName));
    createHivePolicy(Lists.newArrayList("db*", "*"), RandomNameUtils.genRandomName(currentFunName));
    createHivePolicy(
        Lists.newArrayList("db*", "tab*"), RandomNameUtils.genRandomName(currentFunName));
    createHivePolicy(Lists.newArrayList("db1", "*"), RandomNameUtils.genRandomName(currentFunName));
    createHivePolicy(
        Lists.newArrayList("db1", "tab*"), RandomNameUtils.genRandomName(currentFunName));

    RoleEntity role = mock3TableRole(RandomNameUtils.genRandomName(currentFunName));
    Assertions.assertTrue(rangerHiveAuthPlugin.onRoleCreated(role));

    Assertions.assertTrue(rangerHiveAuthPlugin.onRoleAcquired(role));
  }

  /** The metalake role does not to create Ranger policy. Only use it to help test */
  public RoleEntity mockCatalogRole(String roleName) {
    SecurableObject securableObject1 =
        SecurableObjects.parse(
            "catalog",
            SecurableObject.Type.CATALOG,
            Lists.newArrayList(Privileges.UseCatalog.allow()));
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
  public void testFindManagedPolicy() {
    // Because Ranger support wildcard to match the policy, so we need to test the policy with
    // wildcard
    createHivePolicy(Lists.newArrayList("*"), RandomNameUtils.genRandomName(currentFunName()));
    createHivePolicy(Lists.newArrayList("*", "*"), RandomNameUtils.genRandomName(currentFunName()));
    createHivePolicy(
        Lists.newArrayList("db*", "*"), RandomNameUtils.genRandomName(currentFunName()));
    createHivePolicy(
        Lists.newArrayList("db*", "tab*"), RandomNameUtils.genRandomName(currentFunName()));
    createHivePolicy(
        Lists.newArrayList("db3", "*"), RandomNameUtils.genRandomName(currentFunName()));
    createHivePolicy(
        Lists.newArrayList("db3", "tab*"), RandomNameUtils.genRandomName(currentFunName()));
    SecurableObject securableObject1 =
        SecurableObjects.parse(
            "catalog.db3.tab1",
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.CreateTable.allow()));
    RangerPolicy policy = rangerHiveAuthPlugin.findManagedPolicy(securableObject1);
    Assertions.assertNull(policy);

    // Add a policy for db3.tab1
    createHivePolicy(
        Lists.newArrayList("db3", "tab1"), RandomNameUtils.genRandomName(currentFunName()));
    RangerPolicy policy2 = rangerHiveAuthPlugin.findManagedPolicy(securableObject1);
    Assertions.assertNotNull(policy2);
  }

  static void createHivePolicy(List<String> metaObjects, String roleName) {
    Assertions.assertTrue(metaObjects.size() < 4);
    Map<String, RangerPolicy.RangerPolicyResource> policyResourceMap = new HashMap<>();
    for (int i = 0; i < metaObjects.size(); i++) {
      RangerPolicy.RangerPolicyResource policyResource =
          new RangerPolicy.RangerPolicyResource(metaObjects.get(i));
      policyResourceMap.put(
          i == 0
              ? RangerDefines.RESOURCE_DATABASE
              : i == 1 ? RangerDefines.RESOURCE_TABLE : RangerDefines.RESOURCE_COLUMN,
          policyResource);
    }

    RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
    policyItem.setGroups(Arrays.asList(RangerDefines.PUBLIC_GROUP));
    policyItem.setAccesses(
        Arrays.asList(
            new RangerPolicy.RangerPolicyItemAccess(RangerDefines.ACCESS_TYPE_HIVE_SELECT)));
    updateOrCreateRangerPolicy(
        RangerDefines.SERVICE_TYPE_HIVE,
        RANGER_HIVE_REPO_NAME,
        roleName,
        policyResourceMap,
        Collections.singletonList(policyItem));
  }

  @Test
  public void testRoleChangeAddSecurableObject() {
    SecurableObject securableObject1 =
        SecurableObjects.parse(
            String.format("catalog.%s.tab1", currentFunName()),
            SecurableObject.Type.TABLE,
            Lists.newArrayList(Privileges.CreateTable.allow()));

    Role mockCatalogRole = mockCatalogRole(currentFunName());
    // 1. Add a securable object to the role
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onRoleUpdated(
            mockCatalogRole, RoleChange.addSecurableObject(securableObject1)));

    // construct a verify role to check if the role and Ranger policy is created correctly
    RoleEntity verifyRole1 =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName())
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(securableObject1))
            .build();
    verifyRoleInRanger(verifyRole1, null, null, null, null);

    // 2. Multi-call Add a same entity and privilege to the role, because support idempotent
    // operation, so return true
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onRoleUpdated(
            mockCatalogRole, RoleChange.addSecurableObject(securableObject1)));
    verifyRoleInRanger(verifyRole1, null, null, null, null);

    // 3. Add a same entity but have different privilege to the role, then return false
    SecurableObject securableObject3 =
        SecurableObjects.parse(
            securableObject1.fullName(),
            SecurableObject.Type.TABLE,
            Lists.newArrayList(Privileges.SelectTable.allow(), Privileges.ModifyTable.allow()));
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onRoleUpdated(
            mockCatalogRole, RoleChange.addSecurableObject(securableObject3)));
  }

  @Test
  public void testRoleChangeRemoveSecurableObject() {
    // Prepare a role contain 3 securable objects
    String currentFunName = currentFunName();
    RoleEntity role = mock3TableRole(currentFunName());
    Assertions.assertTrue(rangerHiveAuthPlugin.onRoleCreated(role));

    Role mockCatalogRole = mockCatalogRole(currentFunName);
    // remove a securable object from role
    List<SecurableObject> securableObjects = new ArrayList<>(role.securableObjects());
    for (int i = 0; i < role.securableObjects().size(); i++) {
      SecurableObject securableObject0 = securableObjects.remove(0);
      Assertions.assertTrue(
          rangerHiveAuthPlugin.onRoleUpdated(
              mockCatalogRole, RoleChange.removeSecurableObject(securableObject0)));

      // construct a verify role to check if the role and Ranger policy is created correctly
      RoleEntity verifyRole =
          RoleEntity.builder()
              .withId(1L)
              .withName(currentFunName())
              .withAuditInfo(auditInfo)
              .withSecurableObjects(Lists.newArrayList(securableObjects))
              .build();
      verifyRoleInRanger(verifyRole, null, null, null, null);
    }
  }

  @Test
  public void testRoleChangeUpdateSecurableObject() {
    SecurableObject oldSecurableObject =
        SecurableObjects.parse(
            "catalog.db4.tab1",
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.CreateTable.allow()));
    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName())
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(oldSecurableObject))
            .build();
    Assertions.assertTrue(rangerHiveAuthPlugin.onRoleCreated(role));

    Role mockCatalogRole = mockCatalogRole(currentFunName());
    // Keep same matedata namespace and type, but change privileges
    SecurableObject newSecurableObject =
        SecurableObjects.parse(
            oldSecurableObject.fullName(),
            oldSecurableObject.type(),
            Lists.newArrayList(Privileges.SelectTable.allow()));
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onRoleUpdated(
            mockCatalogRole,
            RoleChange.updateSecurableObject(oldSecurableObject, newSecurableObject)));

    // construct a verify role to check if the role and Ranger policy is created correctly
    RoleEntity verifyRole =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName())
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(newSecurableObject))
            .build();
    verifyRoleInRanger(verifyRole, null, null, null, null);
  }

  @Test
  public void testOnGrantedRolesToUser() {
    // prepare create a role
    RoleEntity role = mock3TableRole(currentFunName());
    Assertions.assertTrue(rangerHiveAuthPlugin.onRoleCreated(role));

    // granted role to the user1
    String userName1 = "user1";
    UserEntity userEntity1 =
        UserEntity.builder()
            .withId(1L)
            .withName(userName1)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToUser(Lists.newArrayList(role), userEntity1));
    verifyRoleInRanger(role, Lists.newArrayList(userName1), null, null, null);

    // multi-call to granted role to the user1
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToUser(Lists.newArrayList(role), userEntity1));
    verifyRoleInRanger(role, Lists.newArrayList(userName1), null, null, null);

    // granted role to the user2
    String userName2 = "user2";
    UserEntity userEntity2 =
        UserEntity.builder()
            .withId(1L)
            .withName(userName2)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToUser(Lists.newArrayList(role), userEntity2));

    // Same to verify user1 and user2
    verifyRoleInRanger(role, Lists.newArrayList(userName1), null, null, null);
    verifyRoleInRanger(role, Lists.newArrayList(userName2), null, null, null);
  }

  @Test
  public void testOnRevokedRolesFromUser() {
    // prepare create a role
    RoleEntity role = mock3TableRole(currentFunName());
    Assertions.assertTrue(rangerHiveAuthPlugin.onRoleCreated(role));

    // granted role to the user1
    String userName1 = "user1";
    UserEntity userEntity1 =
        UserEntity.builder()
            .withId(1L)
            .withName(userName1)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToUser(Lists.newArrayList(role), userEntity1));
    verifyRoleInRanger(role, Lists.newArrayList(userName1), null, null, null);

    Assertions.assertTrue(
        rangerHiveAuthPlugin.onRevokedRolesFromUser(Lists.newArrayList(role), userEntity1));
    verifyRoleInRanger(role, null, Lists.newArrayList(userName1), null, null);

    // multi-call to revoked role from user1
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onRevokedRolesFromUser(Lists.newArrayList(role), userEntity1));
    verifyRoleInRanger(role, null, Lists.newArrayList(userName1), null, null);
  }

  @Test
  public void testOnGrantedRolesToGroup() {
    // prepare create a role
    RoleEntity role = mock3TableRole(currentFunName());
    Assertions.assertTrue(rangerHiveAuthPlugin.onRoleCreated(role));

    // granted role to the group1
    String groupName1 = "group1";
    GroupEntity groupEntity1 =
        GroupEntity.builder()
            .withId(1L)
            .withName(groupName1)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToGroup(Lists.newArrayList(role), groupEntity1));
    verifyRoleInRanger(role, null, null, Lists.newArrayList(groupName1), null);

    // multi-call to granted role to the group1
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToGroup(Lists.newArrayList(role), groupEntity1));
    verifyRoleInRanger(role, null, null, Lists.newArrayList(groupName1), null);

    // granted role to the user2
    String groupName2 = "group2";
    GroupEntity groupEntity2 =
        GroupEntity.builder()
            .withId(1L)
            .withName(groupName2)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToGroup(Lists.newArrayList(role), groupEntity2));

    // Same to verify group1 and group2
    verifyRoleInRanger(role, null, null, Lists.newArrayList(groupName1), null);
    verifyRoleInRanger(role, null, null, Lists.newArrayList(groupName2), null);
  }

  @Test
  public void testOnRevokedRolesFromGroup() {
    // prepare create a role
    RoleEntity role = mock3TableRole(currentFunName());
    Assertions.assertTrue(rangerHiveAuthPlugin.onRoleCreated(role));

    // granted role to the group1
    String groupName1 = "group1";
    GroupEntity groupEntity1 =
        GroupEntity.builder()
            .withId(1L)
            .withName(groupName1)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToGroup(Lists.newArrayList(role), groupEntity1));
    verifyRoleInRanger(role, null, null, Lists.newArrayList(groupName1), null);

    Assertions.assertTrue(
        rangerHiveAuthPlugin.onRevokedRolesFromGroup(Lists.newArrayList(role), groupEntity1));
    verifyRoleInRanger(role, null, null, null, Lists.newArrayList(groupName1));

    // multi-call to revoked to the user1
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onRevokedRolesFromGroup(Lists.newArrayList(role), groupEntity1));
    verifyRoleInRanger(role, null, null, null, Lists.newArrayList(groupName1));
  }

  private static class MockOwner implements Owner {
    private final String name;
    private final Type type;

    public MockOwner(String name, Type type) {
      this.name = name;
      this.type = type;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Type type() {
      return type;
    }
  }

  @Test
  public void testOnOwnerSet() {
    RoleEntity role = mockOwnerTableRole(currentFunName());

    MetadataObject metadataObject =
        MetadataObjects.parse(role.securableObjects().get(0).fullName(), MetadataObject.Type.TABLE);
    String userName1 = "user1";
    Owner owner1 = new MockOwner(userName1, Owner.Type.USER);
    Assertions.assertTrue(rangerHiveAuthPlugin.onOwnerSet(metadataObject, null, owner1));
    verifyRoleInRanger(role, Lists.newArrayList(userName1), null, null, null);

    String userName2 = "user2";
    Owner owner2 = new MockOwner(userName2, Owner.Type.USER);
    Assertions.assertTrue(rangerHiveAuthPlugin.onOwnerSet(metadataObject, owner1, owner2));
    verifyRoleInRanger(
        role, Lists.newArrayList(userName2), Lists.newArrayList(userName1), null, null);

    String groupName1 = "group1";
    Owner owner3 = new MockOwner(groupName1, Owner.Type.GROUP);
    Assertions.assertTrue(rangerHiveAuthPlugin.onOwnerSet(metadataObject, owner2, owner3));
    verifyRoleInRanger(
        role, null, Lists.newArrayList(userName1, userName2), Lists.newArrayList(groupName1), null);

    String groupName2 = "group1";
    Owner owner4 = new MockOwner(groupName2, Owner.Type.GROUP);
    Assertions.assertTrue(rangerHiveAuthPlugin.onOwnerSet(metadataObject, owner3, owner4));
    verifyRoleInRanger(
        role,
        null,
        Lists.newArrayList(userName1, userName2),
        Lists.newArrayList(groupName2),
        Lists.newArrayList(groupName1));

    LOG.info("");
  }

  @Test
  public void testCreateUser() {
    UserEntity user =
        UserEntity.builder()
            .withId(0L)
            .withName(currentFunName())
            .withAuditInfo(auditInfo)
            .withRoleIds(null)
            .withRoleNames(null)
            .build();
    Assertions.assertTrue(rangerHiveAuthPlugin.onUserAdded(user));
    Assertions.assertTrue(rangerHiveAuthPlugin.onUserAcquired(user));
    Assertions.assertTrue(rangerHiveAuthPlugin.onUserRemoved(user));
    Assertions.assertFalse(rangerHiveAuthPlugin.onUserAcquired(user));
  }

  @Test
  public void testCreateGroup() {
    GroupEntity group =
        GroupEntity.builder()
            .withId(0L)
            .withName(currentFunName())
            .withAuditInfo(auditInfo)
            .withRoleIds(null)
            .withRoleNames(null)
            .build();

    Assertions.assertTrue(rangerHiveAuthPlugin.onGroupAdded(group));
    Assertions.assertTrue(rangerHiveAuthPlugin.onGroupAcquired(group));
    Assertions.assertTrue(rangerHiveAuthPlugin.onGroupRemoved(group));
    Assertions.assertFalse(rangerHiveAuthPlugin.onGroupAcquired(group));
  }

  @Test
  public void testCombinationOperation() {
    // Create a `CreateTable` privilege role
    SecurableObject securableObject1 =
        SecurableObjects.parse(
            String.format(
                "catalog.%s.tab1", currentFunName()), // use unique db name to avoid conflict
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.CreateTable.allow()));
    RoleEntity role1 =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName())
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(securableObject1))
            .build();
    Assertions.assertTrue(rangerHiveAuthPlugin.onRoleCreated(role1));
    verifyRoleInRanger(role1, null, null, null, null);

    // Create a `SelectTable` privilege role
    SecurableObject securableObject2 =
        SecurableObjects.parse(
            securableObject1.fullName(),
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.SelectTable.allow()));
    RoleEntity role2 =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName())
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(securableObject2))
            .build();
    Assertions.assertTrue(rangerHiveAuthPlugin.onRoleCreated(role2));
    verifyRoleInRanger(role2, null, null, null, null);

    // Create a `ModifyTable` privilege role
    SecurableObject securableObject3 =
        SecurableObjects.parse(
            securableObject1.fullName(),
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.ModifyTable.allow()));
    RoleEntity role3 =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName())
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(securableObject3))
            .build();
    Assertions.assertTrue(rangerHiveAuthPlugin.onRoleCreated(role3));
    verifyRoleInRanger(role3, null, null, null, null);

    // Verify `CreateTable` and `SelectTable` and `ModifyTable` privilege role
    RoleEntity role4 =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName())
            .withAuditInfo(auditInfo)
            .withSecurableObjects(
                Lists.newArrayList(securableObject1, securableObject2, securableObject3))
            .build();
    verifyRoleInRanger(role4, null, null, null, null);

    /** Test grant to user */
    // granted role1 to the user1
    String userName1 = "user1";
    UserEntity userEntity1 =
        UserEntity.builder()
            .withId(1L)
            .withName(userName1)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToUser(Lists.newArrayList(role1), userEntity1));
    // multiple call to granted role1 to the user1 to test idempotent operation
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToUser(Lists.newArrayList(role1), userEntity1));
    verifyRoleInRanger(role1, Lists.newArrayList(userName1), null, null, null);

    // granted role1 to the user2
    String userName2 = "user2";
    UserEntity userEntity2 =
        UserEntity.builder()
            .withId(1L)
            .withName(userName2)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToUser(Lists.newArrayList(role1), userEntity2));
    verifyRoleInRanger(role1, Lists.newArrayList(userName1, userName2), null, null, null);

    // granted role1 to the user3
    String userName3 = "user3";
    UserEntity userEntity3 =
        UserEntity.builder()
            .withId(1L)
            .withName(userName3)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToUser(Lists.newArrayList(role1), userEntity3));
    verifyRoleInRanger(
        role1, Lists.newArrayList(userName1, userName2, userName3), null, null, null);

    // Same granted role2 and role3 to the user1 and user2 and user3
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToUser(Lists.newArrayList(role2, role3), userEntity1));
    verifyRoleInRanger(role2, Lists.newArrayList(userName1), null, null, null);
    verifyRoleInRanger(role3, Lists.newArrayList(userName1), null, null, null);

    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToUser(Lists.newArrayList(role2, role3), userEntity2));
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToUser(Lists.newArrayList(role2, role3), userEntity3));

    /** Test grant to group */
    // granted role1 to the group1
    String groupName1 = "group1";
    GroupEntity groupEntity1 =
        GroupEntity.builder()
            .withId(1L)
            .withName(groupName1)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToGroup(Lists.newArrayList(role1), groupEntity1));
    verifyRoleInRanger(role1, null, null, Lists.newArrayList(groupName1), null);

    // granted role1 to the group2
    String groupName2 = "group2";
    GroupEntity groupEntity2 =
        GroupEntity.builder()
            .withId(1L)
            .withName(groupName2)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToGroup(Lists.newArrayList(role1), groupEntity2));
    verifyRoleInRanger(role1, null, null, Lists.newArrayList(groupName1, groupName2), null);

    // granted role1 to the group3
    String groupName3 = "group3";
    GroupEntity groupEntity3 =
        GroupEntity.builder()
            .withId(1L)
            .withName(groupName3)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToGroup(Lists.newArrayList(role1), groupEntity3));
    verifyRoleInRanger(
        role1, null, null, Lists.newArrayList(groupName1, groupName2, groupName3), null);

    // Same granted role2 and role3 to the group1 and group2 and group3
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToGroup(Lists.newArrayList(role2, role3), groupEntity1));
    verifyRoleInRanger(role2, null, null, Lists.newArrayList(groupName1), null);
    verifyRoleInRanger(role3, null, null, Lists.newArrayList(groupName1), null);

    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToGroup(Lists.newArrayList(role2, role3), groupEntity2));
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onGrantedRolesToGroup(Lists.newArrayList(role2, role3), groupEntity3));
    verifyRoleInRanger(
        role2, null, null, Lists.newArrayList(groupName1, groupName2, groupName3), null);
    verifyRoleInRanger(
        role3, null, null, Lists.newArrayList(groupName1, groupName2, groupName3), null);

    /** Test delete role, but role have grant user or group, so we didn't delete success. */
    Assertions.assertTrue(rangerHiveAuthPlugin.onRoleDeleted(role1));
    verifyRoleInRanger(role1, null, null, null, null);
    Assertions.assertTrue(rangerHiveAuthPlugin.onRoleDeleted(role2));
    verifyRoleInRanger(role2, null, null, null, null);
    Assertions.assertTrue(rangerHiveAuthPlugin.onRoleDeleted(role3));
    verifyRoleInRanger(role3, null, null, null, null);
    role1.securableObjects().stream()
        .forEach(
            securableObject ->
                Assertions.assertNotNull(rangerHiveAuthPlugin.findManagedPolicy(securableObject)));
    role2.securableObjects().stream()
        .forEach(
            securableObject ->
                Assertions.assertNotNull(rangerHiveAuthPlugin.findManagedPolicy(securableObject)));
    role3.securableObjects().stream()
        .forEach(
            securableObject ->
                Assertions.assertNotNull(rangerHiveAuthPlugin.findManagedPolicy(securableObject)));

    /** Test revoke from user */
    // revoke role1 from the user1
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onRevokedRolesFromUser(Lists.newArrayList(role1), userEntity1));
    verifyRoleInRanger(
        role1, Lists.newArrayList(userName2, userName3), Lists.newArrayList(userName1), null, null);

    // revoke role1 from the user2
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onRevokedRolesFromUser(Lists.newArrayList(role1), userEntity2));
    verifyRoleInRanger(
        role1, Lists.newArrayList(userName3), Lists.newArrayList(userName1, userName2), null, null);

    // revoke role1 from the user3
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onRevokedRolesFromUser(Lists.newArrayList(role1), userEntity3));
    verifyRoleInRanger(
        role1, null, Lists.newArrayList(userName1, userName2, userName3), null, null);

    // Same revoke role2 and role3 from the user1 and user2 and user3
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onRevokedRolesFromUser(Lists.newArrayList(role2, role3), userEntity1));
    verifyRoleInRanger(
        role2, Lists.newArrayList(userName2, userName3), Lists.newArrayList(userName1), null, null);
    verifyRoleInRanger(
        role3, Lists.newArrayList(userName2, userName3), Lists.newArrayList(userName1), null, null);

    Assertions.assertTrue(
        rangerHiveAuthPlugin.onRevokedRolesFromUser(Lists.newArrayList(role2, role3), userEntity2));
    verifyRoleInRanger(
        role2, Lists.newArrayList(userName3), Lists.newArrayList(userName1, userName2), null, null);
    verifyRoleInRanger(
        role3, Lists.newArrayList(userName3), Lists.newArrayList(userName1, userName2), null, null);

    Assertions.assertTrue(
        rangerHiveAuthPlugin.onRevokedRolesFromUser(Lists.newArrayList(role2, role3), userEntity3));
    verifyRoleInRanger(
        role2, null, Lists.newArrayList(userName1, userName2, userName3), null, null);
    verifyRoleInRanger(
        role3, null, Lists.newArrayList(userName1, userName2, userName3), null, null);

    /** Test revoke from group */
    // revoke role1 from the group1
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onRevokedRolesFromGroup(Lists.newArrayList(role1), groupEntity1));
    verifyRoleInRanger(
        role1,
        null,
        null,
        Lists.newArrayList(groupName2, groupName3),
        Lists.newArrayList(groupName1));

    // revoke role1 from the group2
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onRevokedRolesFromGroup(Lists.newArrayList(role1), groupEntity2));
    verifyRoleInRanger(
        role1,
        null,
        null,
        Lists.newArrayList(groupName3),
        Lists.newArrayList(groupName1, groupName2));

    // revoke role1 from the group3
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onRevokedRolesFromGroup(Lists.newArrayList(role1), groupEntity3));
    verifyRoleInRanger(
        role1, null, null, null, Lists.newArrayList(groupName1, groupName2, groupName3));

    // Same revoke role2 and role3 from the group1 and group2 and group3
    Assertions.assertTrue(
        rangerHiveAuthPlugin.onRevokedRolesFromGroup(
            Lists.newArrayList(role2, role3), groupEntity1));
    verifyRoleInRanger(
        role2,
        null,
        null,
        Lists.newArrayList(groupName2, groupName3),
        Lists.newArrayList(groupName1));
    verifyRoleInRanger(
        role3,
        null,
        null,
        Lists.newArrayList(groupName2, groupName3),
        Lists.newArrayList(groupName1));

    Assertions.assertTrue(
        rangerHiveAuthPlugin.onRevokedRolesFromGroup(
            Lists.newArrayList(role2, role3), groupEntity2));
    verifyRoleInRanger(
        role2,
        null,
        null,
        Lists.newArrayList(groupName3),
        Lists.newArrayList(groupName1, groupName2));
    verifyRoleInRanger(
        role3,
        null,
        null,
        Lists.newArrayList(groupName3),
        Lists.newArrayList(groupName1, groupName2));

    Assertions.assertTrue(
        rangerHiveAuthPlugin.onRevokedRolesFromGroup(
            Lists.newArrayList(role2, role3), groupEntity3));
    verifyRoleInRanger(
        role2, null, null, null, Lists.newArrayList(groupName1, groupName2, groupName3));
    verifyRoleInRanger(
        role3, null, null, null, Lists.newArrayList(groupName1, groupName2, groupName3));

    /**
     * Test delete role, currently role didn't have grant user or group, so we can delete success.
     */
    Assertions.assertTrue(rangerHiveAuthPlugin.onRoleDeleted(role1));
    Assertions.assertTrue(rangerHiveAuthPlugin.onRoleDeleted(role2));
    Assertions.assertTrue(rangerHiveAuthPlugin.onRoleDeleted(role3));
    // because role1, role2, role3 use same Ranger policy, so we need delete all role first, then
    // verify the policy is deleted
    role1.securableObjects().stream()
        .forEach(
            securableObject ->
                Assertions.assertNull(rangerHiveAuthPlugin.findManagedPolicy(securableObject)));
    role2.securableObjects().stream()
        .forEach(
            securableObject ->
                Assertions.assertNull(rangerHiveAuthPlugin.findManagedPolicy(securableObject)));
    role3.securableObjects().stream()
        .forEach(
            securableObject ->
                Assertions.assertNull(rangerHiveAuthPlugin.findManagedPolicy(securableObject)));
  }

  /**
   * Verify the Gravitino role in Ranger service
   *
   * @param role Gravitino role
   * @param includeUsers Must contain users
   * @param excludeUser Must not contain users
   * @param includeGroup Must contain groups
   * @param excludeGroup Must not contain groups
   */
  public void verifyRoleInRanger(
      RoleEntity role,
      List<String> includeUsers,
      List<String> excludeUser,
      List<String> includeGroup,
      List<String> excludeGroup) {
    role.securableObjects()
        .forEach(
            securableObject -> {
              // Find policy by each securable Object
              String policyName =
                  rangerHiveAuthPlugin.formatPolicyName(role.name(), securableObject.fullName());
              RangerPolicy policy;
              try {
                policy = rangerClient.getPolicy(RangerITEnv.RANGER_HIVE_REPO_NAME, policyName);
              } catch (RangerServiceException e) {
                LOG.error("Failed to get policy: " + policyName);
                throw new RuntimeException(e);
              }
              LOG.info("assertVerifyRoleAndPolicy: " + policy.toString());
              Assertions.assertEquals(policy.getName(), policyName);
              Assertions.assertTrue(
                  policy
                      .getPolicyLabels()
                      .contains(RangerAuthorizationPlugin.MANAGED_BY_GRAVITINO));

              // verify namespace
              List<String> metaObjNamespaces =
                  Lists.newArrayList(DOT_SPLITTER.splitToList(securableObject.fullName()));
              metaObjNamespaces.remove(0); // skip catalog
              List<String> rolePolicies = new ArrayList<>();
              for (int i = 0; i < metaObjNamespaces.size(); i++) {
                rolePolicies.add(
                    policy
                        .getResources()
                        .get(
                            i == 0
                                ? RangerDefines.RESOURCE_DATABASE
                                : i == 1
                                    ? RangerDefines.RESOURCE_TABLE
                                    : RangerDefines.RESOURCE_COLUMN)
                        .getValues()
                        .get(0));
              }
              Assertions.assertEquals(metaObjNamespaces, rolePolicies);

              // verify Gravitino role's privileges and Ranger policy's accesses
              securableObject
                  .privileges()
                  .forEach(
                      gravitinoPrivilege -> {
                        Set<String> rangerPrivileges =
                            rangerHiveAuthPlugin.translatePrivilege(gravitinoPrivilege.name());

                        // The first verify privilege method
                        boolean contain =
                            rangerPrivileges.stream()
                                .anyMatch(
                                    rangerPrivilege -> {
                                      return policy.getPolicyItems().stream()
                                          .anyMatch(
                                              policyItem -> {
                                                return policyItem.getAccesses().stream()
                                                    .anyMatch(
                                                        access -> {
                                                          return access
                                                              .getType()
                                                              .equals(rangerPrivilege);
                                                        });
                                              });
                                    });
                        Assertions.assertTrue(contain);

                        // The second verify privilege method
                        List<String> rangerAccesses =
                            policy.getPolicyItems().stream()
                                .flatMap(policyItem -> policyItem.getAccesses().stream())
                                .map(RangerPolicy.RangerPolicyItemAccess::getType)
                                .collect(Collectors.toList());
                        Assertions.assertTrue(rangerAccesses.containsAll(rangerPrivileges));

                        if (includeUsers != null && !includeUsers.isEmpty()) {
                          contain =
                              rangerPrivileges.stream()
                                  .anyMatch(
                                      rangerPrivilege -> {
                                        return policy.getPolicyItems().stream()
                                            .anyMatch(
                                                policyItem -> {
                                                  return policyItem.getAccesses().stream()
                                                      .anyMatch(
                                                          access -> {
                                                            if (access
                                                                .getType()
                                                                .equals(rangerPrivilege)) {
                                                              return policyItem
                                                                  .getUsers()
                                                                  .containsAll(includeUsers);
                                                            }
                                                            return false;
                                                          });
                                                });
                                      });
                          // Must contain this user
                          Assertions.assertTrue(contain);
                        }

                        if (excludeUser != null && !excludeUser.isEmpty()) {
                          contain =
                              rangerPrivileges.stream()
                                  .anyMatch(
                                      rangerPrivilege -> {
                                        return policy.getPolicyItems().stream()
                                            .anyMatch(
                                                policyItem -> {
                                                  return policyItem.getAccesses().stream()
                                                      .anyMatch(
                                                          access -> {
                                                            if (access
                                                                .getType()
                                                                .equals(rangerPrivilege)) {
                                                              return policyItem.getUsers().stream()
                                                                  .anyMatch(
                                                                      user ->
                                                                          excludeUser.contains(
                                                                              user));
                                                            }
                                                            return false;
                                                          });
                                                });
                                      });
                          // Didn't contain this user
                          Assertions.assertFalse(contain);
                        }

                        if (includeGroup != null && !includeGroup.isEmpty()) {
                          contain =
                              rangerPrivileges.stream()
                                  .anyMatch(
                                      rangerPrivilege -> {
                                        return policy.getPolicyItems().stream()
                                            .anyMatch(
                                                policyItem -> {
                                                  return policyItem.getAccesses().stream()
                                                      .anyMatch(
                                                          access -> {
                                                            if (access
                                                                .getType()
                                                                .equals(rangerPrivilege)) {
                                                              return policyItem
                                                                  .getGroups()
                                                                  .containsAll(includeGroup);
                                                            }
                                                            return false;
                                                          });
                                                });
                                      });
                          // Must contain this group
                          Assertions.assertTrue(contain);
                        }

                        if (excludeGroup != null && !excludeGroup.isEmpty()) {
                          contain =
                              rangerPrivileges.stream()
                                  .anyMatch(
                                      rangerPrivilege -> {
                                        return policy.getPolicyItems().stream()
                                            .anyMatch(
                                                policyItem -> {
                                                  return policyItem.getAccesses().stream()
                                                      .anyMatch(
                                                          access -> {
                                                            if (access
                                                                .getType()
                                                                .equals(rangerPrivilege)) {
                                                              return policyItem.getUsers().stream()
                                                                  .anyMatch(
                                                                      user ->
                                                                          excludeGroup.contains(
                                                                              user));
                                                            }
                                                            return false;
                                                          });
                                                });
                                      });
                          // Didn't contain this group
                          Assertions.assertFalse(contain);
                        }
                      });
            });
  }

  /**
   * Didn't call this function in the Lambda function body, It will return a random function name
   */
  public static String currentFunName() {
    return Thread.currentThread().getStackTrace()[2].getMethodName();
  }
}
