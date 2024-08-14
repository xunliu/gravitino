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
package org.apache.gravitino.authorization.ranger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.authorization.ranger.defines.VXGroup;
import org.apache.gravitino.authorization.ranger.defines.VXGroupList;
import org.apache.gravitino.authorization.ranger.defines.VXUser;
import org.apache.gravitino.authorization.ranger.defines.VXUserList;
import org.apache.gravitino.connector.AuthorizationPropertiesMeta;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RangerHiveAuthorizationPlugin is a plugin for Apache Ranger to manage the Hive authorization of
 * the Apache Gravitino.
 */
public class RangerHiveAuthorizationPlugin extends RangerAuthorizationPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(RangerHiveAuthorizationPlugin.class);

  public RangerHiveAuthorizationPlugin(String catalogProvider, Map<String, String> config) {
    super();
    this.catalogProvider = catalogProvider;
    String rangerUrl = config.get(AuthorizationPropertiesMeta.RANGER_ADMIN_URL);
    String authType = config.get(AuthorizationPropertiesMeta.RANGER_AUTH_TYPE);
    String username = config.get(AuthorizationPropertiesMeta.RANGER_USERNAME);
    // Apache Ranger Password should be minimum 8 characters with min one alphabet and one numeric.
    String password = config.get(AuthorizationPropertiesMeta.RANGER_PASSWORD);
    rangerServiceName = config.get(AuthorizationPropertiesMeta.RANGER_SERVICE_NAME);
    check(rangerUrl != null, "Ranger admin URL is required");
    check(authType != null, "Ranger auth type is required");
    check(username != null, "Ranger username is required");
    check(password != null, "Ranger password is required");
    check(rangerServiceName != null, "Ranger service name is required");

    rangerClient = new RangerClientExt(rangerUrl, authType, username, password);
  }

  /**
   * Ranger hive's privilege have `select`, `update`, `create`, `drop`, `alter`, `index`, `lock`,
   * `read`, `write`, `repladmin`, `serviceadmin`, `refresh` and `all`.
   *
   * <p>Reference: ranger/agents-common/src/main/resources/service-defs/ranger-servicedef-hive.json
   */
  @Override
  protected void initMapPrivileges() {
    mapPrivileges =
        ImmutableMap.<Privilege.Name, Set<String>>builder()
            .put(
                Privilege.Name.CREATE_SCHEMA,
                ImmutableSet.of(RangerDefines.ACCESS_TYPE_HIVE_SELECT))
            .put(
                Privilege.Name.CREATE_TABLE, ImmutableSet.of(RangerDefines.ACCESS_TYPE_HIVE_CREATE))
            .put(
                Privilege.Name.MODIFY_TABLE,
                ImmutableSet.of(
                    RangerDefines.ACCESS_TYPE_HIVE_UPDATE,
                    RangerDefines.ACCESS_TYPE_HIVE_DROP,
                    RangerDefines.ACCESS_TYPE_HIVE_ALTER,
                    RangerDefines.ACCESS_TYPE_HIVE_WRITE))
            .put(
                Privilege.Name.SELECT_TABLE,
                ImmutableSet.of(
                    RangerDefines.ACCESS_TYPE_HIVE_READ, RangerDefines.ACCESS_TYPE_HIVE_SELECT))
            .build();
  }

  /**
   * Because Ranger does not have Role concept, Each metadata object will have a unique Ranger
   * policy. we can use one or more Ranger policy to simulate the role. <br>
   * 1. Create a policy for each metadata object. <br>
   * 2. Save role name in the Policy properties. <br>
   * 3. Set `MANAGED_BY_GRAVITINO` label in the policy. <br>
   * 4. For easy manage, each privilege will create a RangerPolicyItemAccess in the policy. <br>
   * 5. The policy will only have one user, the user is the {OWNER} of the policy. <br>
   * 6. The policy will not have group. <br>
   */
  @Override
  public Boolean onRoleCreated(Role role) throws RuntimeException {
    return onRoleUpdated(
        role,
        role.securableObjects().stream()
            .map(securableObject -> RoleChange.addSecurableObject(securableObject))
            .toArray(RoleChange[]::new));
  }

  @Override
  public Boolean onRoleAcquired(Role role) throws RuntimeException {
    Boolean findAll;
    try {
      findAll =
          role.securableObjects().stream()
                  .filter(
                      securableObject -> {
                        RangerPolicy policy = findManagedPolicy(securableObject);
                        return policy != null;
                      })
                  .count()
              == role.securableObjects().size();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return findAll;
  }

  /**
   * Because one Ranger policy maybe contain multiple securable objects, so we didn't directly
   * remove the policy. <br>
   * Reference: https://github.com/apache/gravitino/issues/4509
   */
  @Override
  public Boolean onRoleDeleted(Role role) throws RuntimeException {
    return onRoleUpdated(
        role,
        role.securableObjects().stream()
            .map(securableObject -> RoleChange.removeSecurableObject(securableObject))
            .toArray(RoleChange[]::new));
  }

  @Override
  public Boolean onRoleUpdated(Role role, RoleChange... changes) throws RuntimeException {
    for (RoleChange change : changes) {
      boolean execResult;
      if (change instanceof RoleChange.AddSecurableObject) {
        execResult = doAddSecurableObject((RoleChange.AddSecurableObject) change);
      } else if (change instanceof RoleChange.RemoveSecurableObject) {
        execResult =
            doRemoveSecurableObject(role.name(), (RoleChange.RemoveSecurableObject) change);
      } else if (change instanceof RoleChange.UpdateSecurableObject) {
        execResult =
            doUpdateSecurableObject(role.name(), (RoleChange.UpdateSecurableObject) change);
      } else {
        throw new IllegalArgumentException(
            "Unsupported role change type: "
                + (change == null ? "null" : change.getClass().getSimpleName()));
      }
      if (!execResult) {
        return Boolean.FALSE;
      }
    }

    return Boolean.TRUE;
  }

  @VisibleForTesting
  public List<Privilege> getAllPrivileges() {
    return mapPrivileges.keySet().stream().map(Privileges::allow).collect(Collectors.toList());
  }

  @Override
  public Boolean onOwnerSet(MetadataObject metadataObject, Owner preOwner, Owner newOwner)
      throws RuntimeException {
    List<Privilege> allPrivileges = getAllPrivileges();

    SecurableObject securableObjects =
        SecurableObjects.parse(metadataObject.fullName(), metadataObject.type(), allPrivileges);

    AuditInfo auditInfo =
        AuditInfo.builder()
            .withCreator(PrincipalUtils.getCurrentUserName())
            .withCreateTime(Instant.now())
            .build();

    RoleEntity role =
        RoleEntity.builder()
            .withId(0L)
            .withName(OWNER_ROLE_NAME)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(securableObjects))
            .build();

    Boolean execResult = onRoleCreated(role);
    if (!execResult) {
      return Boolean.FALSE;
    }
    if (preOwner != null) {
      if (preOwner.type() == Owner.Type.USER) {
        UserEntity userEntity =
            UserEntity.builder()
                .withId(0L)
                .withName(preOwner.name())
                .withRoleNames(Collections.emptyList())
                .withRoleIds(Collections.emptyList())
                .withAuditInfo(auditInfo)
                .build();
        execResult = onRevokedRolesFromUser(Lists.newArrayList(role), userEntity);
        if (!execResult) {
          return Boolean.FALSE;
        }
      } else {
        GroupEntity groupEntity =
            GroupEntity.builder()
                .withId(0L)
                .withName(preOwner.name())
                .withRoleNames(Collections.emptyList())
                .withRoleIds(Collections.emptyList())
                .withAuditInfo(auditInfo)
                .build();
        execResult = onRevokedRolesFromGroup(Lists.newArrayList(role), groupEntity);
        if (!execResult) {
          return Boolean.FALSE;
        }
      }
    }
    if (newOwner != null) {
      if (newOwner.type() == Owner.Type.USER) {
        UserEntity userEntity =
            UserEntity.builder()
                .withId(0L)
                .withName(newOwner.name())
                .withRoleNames(Collections.emptyList())
                .withRoleIds(Collections.emptyList())
                .withAuditInfo(auditInfo)
                .build();
        execResult = onGrantedRolesToUser(Lists.newArrayList(role), userEntity);
        if (!execResult) {
          return Boolean.FALSE;
        }
      } else {
        GroupEntity groupEntity =
            GroupEntity.builder()
                .withId(0L)
                .withName(newOwner.name())
                .withRoleNames(Collections.emptyList())
                .withRoleIds(Collections.emptyList())
                .withAuditInfo(auditInfo)
                .build();
        execResult = onGrantedRolesToGroup(Lists.newArrayList(role), groupEntity);
        if (!execResult) {
          return Boolean.FALSE;
        }
      }
    }
    return Boolean.TRUE;
  }

  /**
   * Because one Ranger policy maybe contain multiple Gravitino securable objects, <br>
   * So we need to find the corresponding policy item mapping to set the user.
   */
  @Override
  public Boolean onGrantedRolesToUser(List<Role> roles, User user) throws RuntimeException {
    AtomicReference<Boolean> execResult = new AtomicReference<>(Boolean.TRUE);
    for (Role role : roles) {
      role.securableObjects()
          .forEach(
              securableObject -> {
                RangerPolicy policy = findManagedPolicy(securableObject);
                if (policy == null) {
                  LOG.warn("The policy is not exist for the securable object({})", securableObject);
                  execResult.set(Boolean.FALSE);
                  return;
                }

                securableObject
                    .privileges()
                    .forEach(
                        privilege -> {
                          // Convert Gravitino privilege to Ranger privilege
                          mapPrivileges
                              .getOrDefault(privilege.name(), Collections.emptySet())
                              .forEach(
                                  // Use the Ranger privilege name to search
                                  rangerPrivilegeName -> {
                                    policy
                                        /**
                                         * TODO: Maybe we need to add the user to the
                                         * Deny/DataMask/RowFilter policy item in the future.
                                         */
                                        .getPolicyItems()
                                        .forEach(
                                            policyItem -> {
                                              if (policyItem.getAccesses().stream()
                                                  .anyMatch(
                                                      policyItemAccess ->
                                                          policyItemAccess
                                                              .getType()
                                                              .equals(rangerPrivilegeName))) {
                                                // If the user is not exist in the policy item, then
                                                // add it.
                                                if (!policyItem.getUsers().contains(user.name())) {
                                                  policyItem.getUsers().add(user.name());
                                                }
                                              }
                                            });
                                    try {
                                      rangerClient.updatePolicy(policy.getId(), policy);
                                    } catch (RangerServiceException e) {
                                      throw new RuntimeException(e);
                                    }
                                  });
                        });
              });
      if (!execResult.get()) {
        return Boolean.FALSE;
      }
    }

    return Boolean.TRUE;
  }

  /**
   * Because one Ranger policy maybe contain multiple Gravitino securable objects, <br>
   * So we need to find the corresponding policy item mapping to remove the user.
   */
  @Override
  public Boolean onRevokedRolesFromUser(List<Role> roles, User user) throws RuntimeException {
    AtomicReference<Boolean> result = new AtomicReference<>(Boolean.TRUE);
    for (Role role : roles) {
      role.securableObjects()
          .forEach(
              securableObject -> {
                RangerPolicy policy = findManagedPolicy(securableObject);
                if (policy == null) {
                  LOG.warn("The policy is not exist for the securable object({})", securableObject);
                  result.set(Boolean.FALSE);
                  return;
                }

                securableObject
                    .privileges()
                    .forEach(
                        privilege -> {
                          // Convert Gravitino privilege to Ranger privilege
                          mapPrivileges
                              .getOrDefault(privilege.name(), Collections.emptySet())
                              .forEach(
                                  // Use the Ranger privilege name to search
                                  rangerPrivilegeName -> {
                                    policy
                                        // TODO: Maybe we need to add the user to the
                                        // Deny/DataMask/RowFilter policy item in the future.
                                        .getPolicyItems()
                                        .forEach(
                                            policyItem -> {
                                              if (policyItem.getAccesses().stream()
                                                  .anyMatch(
                                                      policyItemAccess ->
                                                          policyItemAccess
                                                              .getType()
                                                              .equals(rangerPrivilegeName))) {
                                                // If the user is exist in the policy item, then
                                                // remove it.
                                                policyItem.getUsers().removeIf(user.name()::equals);
                                              }
                                            });
                                    try {
                                      rangerClient.updatePolicy(policy.getId(), policy);
                                    } catch (RangerServiceException e) {
                                      throw new RuntimeException(e);
                                    }
                                  });
                        });
              });
      if (!result.get()) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onGrantedRolesToGroup(List<Role> roles, Group group) throws RuntimeException {
    AtomicReference<Boolean> result = new AtomicReference<>(Boolean.TRUE);
    for (Role role : roles) {
      role.securableObjects()
          .forEach(
              securableObject -> {
                RangerPolicy policy = findManagedPolicy(securableObject);
                if (policy == null) {
                  LOG.warn("The policy is not exist for the securable object({})", securableObject);
                  result.set(Boolean.FALSE);
                  return;
                }

                securableObject
                    .privileges()
                    .forEach(
                        privilege -> {
                          // Convert Gravitino privilege to Ranger privilege
                          mapPrivileges
                              .getOrDefault(privilege.name(), Collections.emptySet())
                              .forEach(
                                  // Use the Ranger privilege name to search
                                  rangerPrivilegeName -> {
                                    policy
                                        // TODO: Maybe we need to add the user to the
                                        // Deny/DataMask/RowFilter policy item in the future.
                                        .getPolicyItems()
                                        .forEach(
                                            policyItem -> {
                                              if (policyItem.getAccesses().stream()
                                                  .anyMatch(
                                                      policyItemAccess ->
                                                          policyItemAccess
                                                              .getType()
                                                              .equals(rangerPrivilegeName))) {
                                                // If the user is not exist in the policy item, then
                                                // add it.
                                                if (!policyItem
                                                    .getGroups()
                                                    .contains(group.name())) {
                                                  policyItem.getGroups().add(group.name());
                                                }
                                              }
                                            });
                                    try {
                                      rangerClient.updatePolicy(policy.getId(), policy);
                                    } catch (RangerServiceException e) {
                                      throw new RuntimeException(e);
                                    }
                                  });
                        });
              });
      if (!result.get()) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onRevokedRolesFromGroup(List<Role> roles, Group group) throws RuntimeException {
    AtomicReference<Boolean> result = new AtomicReference<>(Boolean.TRUE);
    for (Role role : roles) {
      role.securableObjects()
          .forEach(
              securableObject -> {
                RangerPolicy policy = findManagedPolicy(securableObject);
                if (policy == null) {
                  LOG.warn("The policy is not exist for the securable object({})", securableObject);
                  result.set(Boolean.FALSE);
                  return;
                }

                securableObject
                    .privileges()
                    .forEach(
                        privilege -> {
                          // Convert Gravitino privilege to Ranger privilege
                          mapPrivileges
                              .getOrDefault(privilege.name(), Collections.emptySet())
                              .forEach(
                                  // Use the Ranger privilege name to search
                                  rangerPrivilegeName -> {
                                    policy
                                        // TODO: Maybe we need to add the user to the
                                        // Deny/DataMask/RowFilter policy item in the future.
                                        .getPolicyItems()
                                        .forEach(
                                            policyItem -> {
                                              if (policyItem.getAccesses().stream()
                                                  .anyMatch(
                                                      policyItemAccess ->
                                                          policyItemAccess
                                                              .getType()
                                                              .equals(rangerPrivilegeName))) {
                                                // If the user is exist in the policy item, then
                                                // remove it.
                                                policyItem
                                                    .getGroups()
                                                    .removeIf(group.name()::equals);
                                              }
                                            });
                                    try {
                                      rangerClient.updatePolicy(policy.getId(), policy);
                                    } catch (RangerServiceException e) {
                                      throw new RuntimeException(e);
                                    }
                                  });
                        });
              });
      if (!result.get()) {
        return Boolean.FALSE;
      }
    }

    return Boolean.TRUE;
  }

  @Override
  public Boolean onUserAdded(User user) throws RuntimeException {
    VXUserList list = rangerClient.searchUser(ImmutableMap.of("name", user.name()));
    if (list.getListSize() > 0) {
      LOG.warn("The user({}) is already exist in the Ranger!", user.name());
      return Boolean.FALSE;
    }

    VXUser rangerUser = VXUser.builder().withName(user.name()).withDescription(user.name()).build();
    return rangerClient.createUser(rangerUser);
  }

  @Override
  public Boolean onUserRemoved(User user) throws RuntimeException {
    VXUserList list = rangerClient.searchUser(ImmutableMap.of("name", user.name()));
    if (list.getListSize() == 0) {
      LOG.warn("The user({}) is not exist in the Ranger!", user);
      return Boolean.FALSE;
    }
    rangerClient.deleteUser(list.getList().get(0).getId());
    return Boolean.TRUE;
  }

  @Override
  public Boolean onUserAcquired(User user) throws RuntimeException {
    VXUserList list = rangerClient.searchUser(ImmutableMap.of("name", user.name()));
    if (list.getListSize() == 0) {
      LOG.warn("The user({}) is not exist in the Ranger!", user);
      return Boolean.FALSE;
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onGroupAdded(Group group) throws RuntimeException {
    return rangerClient.createGroup(VXGroup.builder().withName(group.name()).build());
  }

  @Override
  public Boolean onGroupRemoved(Group group) throws RuntimeException {
    VXGroupList list = rangerClient.searchGroup(ImmutableMap.of("name", group.name()));
    if (list.getListSize() == 0) {
      LOG.warn("The group({}) is not exist in the Ranger!", group);
      return Boolean.FALSE;
    }
    return rangerClient.deleteGroup(list.getList().get(0).getId());
  }

  @Override
  public Boolean onGroupAcquired(Group group) {
    VXGroupList vxGroupList = rangerClient.searchGroup(ImmutableMap.of("name", group.name()));
    if (vxGroupList.getListSize() == 0) {
      LOG.warn("The group({}) is not exist in the Ranger!", group);
      return Boolean.FALSE;
    }
    return Boolean.TRUE;
  }

  /**
   * Add the securable object's privilege to the policy. <br>
   * 1. Find the policy base the securable object. <br>
   * 2. If the policy is exist and have same privilege, because support idempotent operation, so
   * return true. <br>
   * 3. If the policy is exist but have different privilege, also return true. Because one Ranger
   * policy maybe contain multiple Gravitino securable object <br>
   * 4. If the policy is not exist, then create a new policy. <br>
   */
  private boolean doAddSecurableObject(RoleChange.AddSecurableObject change) {
    RangerPolicy policy = findManagedPolicy(change.getSecurableObject());

    if (policy != null) {
      Set<String> policyPrivileges =
          policy.getPolicyItems().stream()
              .flatMap(policyItem -> policyItem.getAccesses().stream())
              .map(RangerPolicy.RangerPolicyItemAccess::getType)
              .collect(Collectors.toSet());
      Set<String> newPrivileges =
          change.getSecurableObject().privileges().stream()
              .filter(Objects::nonNull)
              .flatMap(privilege -> translatePrivilege(privilege.name()).stream())
              .filter(Objects::nonNull)
              .collect(Collectors.toSet());
      if (policyPrivileges.containsAll(newPrivileges)) {
        LOG.info(
            "The privilege({}) already added to Ranger policy({})!",
            policy.getName(),
            change.getSecurableObject().fullName());
        // If exist policy and have same privilege then directly return true, because support
        // idempotent operation.
        return true;
      }
    } else {
      policy = new RangerPolicy();
      policy.setService(rangerServiceName);
      policy.setName(change.getSecurableObject().fullName());
      policy.setPolicyLabels(Lists.newArrayList(MANAGED_BY_GRAVITINO));

      List<String> metadataObjectNamespaces =
          Lists.newArrayList(
              SecurableObjects.DOT_SPLITTER.splitToList(change.getSecurableObject().fullName()));
      if (metadataObjectNamespaces.size() > 4) {
        // The max level of the securable object is `catalog.db.table.column`
        throw new RuntimeException("The securable object than 4");
      }
      metadataObjectNamespaces.remove(0); // remove `catalog`

      for (int i = 0; i < metadataObjectNamespaces.size(); i++) {
        RangerPolicy.RangerPolicyResource policyResource =
            new RangerPolicy.RangerPolicyResource(metadataObjectNamespaces.get(i));
        policy
            .getResources()
            .put(
                i == 0
                    ? RangerDefines.RESOURCE_DATABASE
                    : i == 1 ? RangerDefines.RESOURCE_TABLE : RangerDefines.RESOURCE_COLUMN,
                policyResource);
      }
    }

    addPolicyItemAccess(policy, change.getSecurableObject());
    try {
      if (policy.getId() == null) {
        rangerClient.createPolicy(policy);
      } else {
        rangerClient.updatePolicy(policy.getId(), policy);
      }
    } catch (RangerServiceException e) {
      throw new RuntimeException(e);
    }

    return true;
  }

  /**
   * Remove the securable object's privilege from the policy. <br>
   * Because Ranger use unique metadata to location Ranger policy and manage the all privileges, So
   * one Ranger policy maybe contain multiple Gravitino privilege objects. <br>
   * Remove Ranger policy item condition is: <br>
   * 1. This Ranger policy item's accesses equal the Gravition securable object's privilege. <br>
   * 2. This Ranger policy item's users and groups is empty. <br>
   * If policy didn't have any policy item, then delete this policy: <br>
   */
  private boolean doRemoveSecurableObject(
      String roleName, RoleChange.RemoveSecurableObject change) {
    RangerPolicy policy = findManagedPolicy(change.getSecurableObject());

    final boolean[] hasChange = {false};
    if (policy != null) {
      policy
          .getPolicyItems()
          .forEach(
              policyItem -> {
                policyItem
                    .getAccesses()
                    .removeIf(
                        // Remove the policy item's accesses equal the Gravition securable object's
                        access -> {
                          // Use Gravitino privilege to search the Ranger policy item's access
                          boolean findAccess =
                              change.getSecurableObject().privileges().stream()
                                  .filter(Objects::nonNull)
                                  .flatMap(
                                      privilege -> translatePrivilege(privilege.name()).stream())
                                  .filter(Objects::nonNull)
                                  .anyMatch(privilege -> privilege.equals(access.getType()));
                          boolean notHasUserAndGroup =
                              (policyItem.getUsers().isEmpty()
                                      || policyItem.getUsers().size() == 1
                                          && policyItem
                                              .getUsers()
                                              .contains(RangerDefines.OWNER_USER))
                                  && policyItem.getGroups().isEmpty();

                          if (findAccess && notHasUserAndGroup) {
                            hasChange[0] = true;
                          }
                          return findAccess && notHasUserAndGroup;
                        });
              });

      // If the policy item's accesses is empty, then remove it.
      policy
          .getPolicyItems()
          .removeIf(
              policyItem ->
                  // Didn't need check users and groups
                  policyItem.getAccesses().isEmpty());

      if (hasChange[0]) {
        try {
          if (policy.getPolicyItems().size() == 0) {
            rangerClient.deletePolicy(policy.getId());
          } else {
            rangerClient.updatePolicy(policy.getId(), policy);
          }
        } catch (RangerServiceException e) {
          LOG.error("Failed to remove the policy item from the Ranger policy {}!", policy);
          throw new RuntimeException(e);
        }
      }
    } else {
      LOG.warn(
          "Cannot find the Ranger policy({}) for the Gravitino securable object({})!",
          roleName,
          change.getSecurableObject().fullName());
      // Don't throw exception or return false, because need support immutable operation.
    }
    return true;
  }

  /**
   * 1. Find the policy base securable object. <br>
   * 2. If the policy is exist, then user new securable object's privilege to update. <br>
   * 3. If the policy is not exist return false. <br>
   */
  private boolean doUpdateSecurableObject(
      String roleName, RoleChange.UpdateSecurableObject change) {
    RangerPolicy policy = findManagedPolicy(change.getSecurableObject());

    if (policy != null) {
      removePolicyItemAccess(policy, change.getSecurableObject());
      addPolicyItemAccess(policy, change.getNewSecurableObject());
      try {
        if (policy.getId() == null) {
          rangerClient.createPolicy(policy);
        } else {
          rangerClient.updatePolicy(policy.getId(), policy);
        }
      } catch (RangerServiceException e) {
        throw new RuntimeException(e);
      }
    } else {
      LOG.warn(
          "Cannot find the policy({}) for the securable object({})!",
          roleName,
          change.getSecurableObject().fullName());
      return false;
    }
    return true;
  }

  /**
   * Add policy item access items base the securable object's privileges. <br>
   * We didn't clean the policy items, because one Ranger policy maybe contain multiple Gravitino
   * securable objects. <br>
   */
  private void addPolicyItemAccess(RangerPolicy policy, SecurableObject securableObject) {
    // First check the privilege if support in the Ranger Hive
    checkSecurableObject(securableObject);

    // Add the policy items by the securable object's privileges
    securableObject
        .privileges()
        .forEach(
            gravitinoPrivilege -> {
              translatePrivilege(gravitinoPrivilege.name())
                  .forEach(
                      rangerPrivilege -> {
                        boolean alreadyExist =
                            policy.getPolicyItems().stream()
                                .anyMatch(
                                    policyItem ->
                                        policyItem.getAccesses().stream()
                                            .anyMatch(
                                                access ->
                                                    access.getType().equals(rangerPrivilege)));
                        if (alreadyExist) {
                          return;
                        }

                        RangerPolicy.RangerPolicyItem policyItem =
                            new RangerPolicy.RangerPolicyItem();
                        RangerPolicy.RangerPolicyItemAccess access =
                            new RangerPolicy.RangerPolicyItemAccess();
                        access.setType(rangerPrivilege);
                        policyItem.getAccesses().add(access);
                        policyItem.setUsers(Lists.newArrayList(RangerDefines.OWNER_USER));
                        if (Privilege.Condition.ALLOW == gravitinoPrivilege.condition()) {
                          policy.getPolicyItems().add(policyItem);
                        } else {
                          policy.getDenyPolicyItems().add(policyItem);
                        }
                      });
            });
  }

  /**
   * Remove policy item access items base the securable object's privileges. <br>
   * We didn't clean the policy items, because one Ranger policy maybe contain multiple Gravitino
   * privilege objects. <br>
   */
  private void removePolicyItemAccess(RangerPolicy policy, SecurableObject securableObject) {
    // First check the privilege if support in the Ranger Hive
    checkSecurableObject(securableObject);

    // Delete the policy items by the securable object's privileges
    policy
        .getPolicyItems()
        .removeIf(
            policyItem -> {
              return securableObject.privileges().stream()
                  .filter(Objects::nonNull)
                  .flatMap(privilege -> translatePrivilege(privilege.name()).stream())
                  .filter(Objects::nonNull)
                  .anyMatch(
                      privilege -> {
                        return policyItem.getAccesses().stream()
                            .anyMatch(access -> access.getType().equals(privilege));
                      });
            });
  }

  private boolean checkSecurableObject(SecurableObject securableObject) {
    securableObject
        .privileges()
        .forEach(
            privilege -> {
              check(
                  checkPrivilege(privilege.name()),
                  "This privilege %s is not support in the Ranger hive authorization",
                  privilege.name());
            });
    return true;
  }

  @Override
  public void close() throws IOException {}
}
