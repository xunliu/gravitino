/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.ranger;

import com.datastrato.gravitino.authorization.Group;
import com.datastrato.gravitino.authorization.Privilege;
import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.authorization.RoleChange;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.authorization.User;
import com.datastrato.gravitino.connector.AuthorizationPropertiesMeta;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerHiveAuthorizationHook extends RangerAuthorizationHook {
  private static final Logger LOG = LoggerFactory.getLogger(RangerHiveAuthorizationHook.class);

  public RangerHiveAuthorizationHook(String catalogProvider, Map<String, String> config) {
    super();
    this.catalogProvider = catalogProvider;
    String rangerUrl =
        config.get(AuthorizationPropertiesMeta.RANGER_ADMIN_URL); // "http://localhost:6080";
    String authType = config.get(AuthorizationPropertiesMeta.RANGER_AUTH_TYPE); // = "simple";
    String username = config.get(AuthorizationPropertiesMeta.RANGER_USERNAME); // = "admin";
    // Apache Ranger Password should be minimum 8 characters with min one alphabet and one numeric.
    String password = config.get(AuthorizationPropertiesMeta.RANGER_PASSWORD); // "rangerR0cks!";
    rangerServiceName = config.get(AuthorizationPropertiesMeta.RANGER_SERVICE_NAME); // = "hivedev";
    check(rangerUrl != null, "Ranger admin URL is required");
    check(authType != null, "Ranger auth type is required");
    check(username != null, "Ranger username is required");
    check(password != null, "Ranger password is required");
    check(rangerServiceName != null, "Ranger service name is required");

    rangerClient = new RangerClient(rangerUrl, authType, username, password, null);
  }

  @VisibleForTesting
  public String formatPolicyName(String roleName, String securableObjectFullName) {
    return roleName + "-" + securableObjectFullName;
  }

  /**
   * Because Ranger does not have Role concept, Each metadata object will have a unique Ranger
   * policy. we can use one or more Ranger policy to simulate the role. <br>
   * 1. Create a policy for each metadata object. <br>
   * 2. Save role name in the Policy properties. <br>
   * 3. Set `MANAGED_BY_GRAVITINO` label in the policy to distinguish between created by other ways.
   * <br>
   * 4. For easy manage, each privilege will create a RangerPolicyItemAccess in the policy. <br>
   * 5. The policy will only have one user, the user is the {OWNER} of the policy. <br>
   * 6. The policy will not have group. <br>
   */
  @Override
  public Boolean onCreateRole(Role role) throws RuntimeException {
    try {
      role.securableObjects()
          .forEach(
              securableObject -> {
                RangerPolicy policy = findManagedPolicy(securableObject, false);
                if (policy == null) {
                  policy = new RangerPolicy();
                  policy.setService(rangerServiceName);
                  policy.setName(formatPolicyName(role.name(), securableObject.fullName()));
                  policy.setPolicyLabels(Lists.newArrayList(MANAGED_BY_GRAVITINO));

                  List<String> objects =
                      SecurableObjects.DOT_SPLITTER.splitToList(securableObject.fullName());
                  if (objects.size() > 5) {
                    // The max level of the securable object is `metalake.catalog.db.table.column`
                    throw new RuntimeException("The securable object than 5");
                  }

                  for (int i = 1; i < objects.size(); i++) {
                    RangerPolicy.RangerPolicyResource policyResource =
                        new RangerPolicy.RangerPolicyResource(objects.get(i));
                    policy
                        .getResources()
                        .put(
                            i == 1
                                ? RangerRef.RESOURCE_DATABASE
                                : i == 2 ? RangerRef.RESOURCE_TABLE : RangerRef.RESOURCE_COLUMN,
                            policyResource);
                  }
                } else {
                  // Update the policy name
                  policy.setName(formatPolicyName(role.name(), securableObject.fullName()));
                  // Clear exist policy items for support idempotent operation.
                  policy.getPolicyItems().clear();
                }

                updatePolicyItemAccess(policy, securableObject);
                try {
                  if (policy.getId() == null) {
                    rangerClient.createPolicy(policy);
                  } else {
                    rangerClient.updatePolicy(policy.getId(), policy);
                  }
                } catch (RangerServiceException e) {
                  throw new RuntimeException(e);
                }
              });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return Boolean.TRUE;
  }

  private RangerPolicy createPolicyBySecurableObject(
      String roleName, SecurableObject securableObject) {
    RangerPolicy policy = new RangerPolicy();
    policy.setService(rangerServiceName);
    policy.setName(formatPolicyName(roleName, securableObject.fullName()));
    policy.setPolicyLabels(Lists.newArrayList(MANAGED_BY_GRAVITINO));

    List<String> objects = SecurableObjects.DOT_SPLITTER.splitToList(securableObject.fullName());
    if (objects.size() > 5) {
      // The max level of the securable object is `metalake.catalog.db.table.column`
      throw new RuntimeException("The securable object than 5");
    }

    for (int i = 1; i < objects.size(); i++) {
      RangerPolicy.RangerPolicyResource policyResource =
          new RangerPolicy.RangerPolicyResource(objects.get(i));
      policy
          .getResources()
          .put(
              i == 1
                  ? RangerRef.RESOURCE_DATABASE
                  : i == 2 ? RangerRef.RESOURCE_TABLE : RangerRef.RESOURCE_COLUMN,
              policyResource);
    }
    return policy;
  }

  @Override
  public Boolean onGetRole(Role role) throws RuntimeException {
    Boolean findAll = Boolean.FALSE;
    try {
      findAll = role.securableObjects().stream().filter(securableObject -> {
        RangerPolicy policy = findManagedPolicy(securableObject, false);
        return policy != null;
      }).count() == role.securableObjects().size();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return findAll;
  }

  @Override
  public Boolean onDeleteRole(Role role) throws RuntimeException {
    try {
      role.securableObjects()
          .forEach(
              securableObject -> {
                RangerPolicy policy = findManagedPolicy(securableObject, false);
                if (policy != null) {
                  try {
                    rangerClient.deletePolicy(policy.getId());
                  } catch (RangerServiceException e) {
                    throw new RuntimeException(e);
                  }
                }
              });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return Boolean.TRUE;
  }

  @Override
  public Boolean onUpdateRole(Role role, RoleChange... changes) throws RuntimeException {
    for (RoleChange change : changes) {
      if (change instanceof RoleChange.AddSecurableObject) {
        doAddSecurableObject(role.name(), (RoleChange.AddSecurableObject) change);
      } else if (change instanceof RoleChange.RemoveSecurableObject) {
        doRemoveSecurableObject(role.name(), (RoleChange.RemoveSecurableObject) change);
      } else if (change instanceof RoleChange.UpdateSecurableObject) {
        doUpdateSecurableObject(role.name(), (RoleChange.UpdateSecurableObject) change);
      } else {
        throw new IllegalArgumentException(
            "Unsupported role change type: "
                + (change == null ? "null" : change.getClass().getSimpleName()));
      }
    }

    return true;
  }

  @Override
  public Boolean onAddUser(User user) throws RuntimeException {
    rangerClient.
    return null;
  }

  @Override
  public Boolean onRemoveUser(String user) throws RuntimeException {
    return null;
  }

  @Override
  public User onGetUser(String user) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onAddGroup(String group) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onRemoveGroup(String group) throws RuntimeException {
    return null;
  }

  @Override
  public Group onGetGroup(String group) {
    return null;
  }

  @Override
  public Boolean onGrantRolesToUser(List<Role> roles, User user) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onRevokeRolesFromUser(List<Role> roles, User user) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onGrantRolesToGroup(List<Role> roles, Group group) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onRevokeRolesFromGroup(List<Role> roles, Group group) throws RuntimeException {
    return null;
  }

  @Override
  protected void initPrivileges() {
    mapPrivileges =
        ImmutableMap.<Privilege.Name, String>builder()
            .put(Privilege.Name.TABULAR_SELECT, "select")
            .put(Privilege.Name.TABULAR_UPDATE, "update")
            .put(Privilege.Name.TABULAR_CREATE, "create")
            .put(Privilege.Name.TABULAR_DROP, "drop")
            .put(Privilege.Name.TABULAR_ALTER, "alter")
            .put(Privilege.Name.TABULAR_INDEX, "index")
            .put(Privilege.Name.TABULAR_LOCK, "lock")
            .put(Privilege.Name.TABULAR_READ, "read")
            .put(Privilege.Name.TABULAR_WRITE, "write")
            .put(Privilege.Name.TABULAR_REPL_ADMIN, "repladmin")
            .put(Privilege.Name.TABULAR_SERVICE_ADMIN, "serviceadmin")
            .put(Privilege.Name.TABULAR_ALL, "all")
            .build();
  }

  //  @Override
  //  public void close() throws IOException {}

  /**
   * Because Ranger does not support roles, so we will create a policy with the role name as the
   * policy name. 1. Create a policy with the role name as the policy name. 2. Because every Ranger
   * policy must contain a unique resource, we will use a random UUID as the resource value. You can
   * set real resource values in next steps. - database: random UUID
   */
  //  @Override
  public Role createRole1(String name) throws UnsupportedOperationException {
    try {
      RangerPolicy policy = new RangerPolicy();
      policy.setService(rangerServiceName);
      policy.setName(name);

      RangerPolicy.RangerPolicyResource policyResource1 = new RangerPolicy.RangerPolicyResource();
      policyResource1.setValues(Lists.newArrayList("NEW_ROLE-" + UUID.randomUUID().toString()));
      //      policy.setResources(ImmutableMap.of("database", policyResource1));

      //      RangerPolicy.RangerPolicyResource policyResource2 = new
      // RangerPolicy.RangerPolicyResource();
      //      policyResource2.setValues(Lists.newArrayList("tab1*"));
      //      policy.setResources(ImmutableMap.of("table", policyResource2));

      RangerPolicy.RangerPolicyResource policyResource3 = new RangerPolicy.RangerPolicyResource();
      policyResource3.setValues(Lists.newArrayList("*"));
      policy.setResources(ImmutableMap.of("database", policyResource1));

      //      RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
      ////      List<RangerPolicy.RangerPolicyItemAccess> accesses = getList(new
      // RangerPolicy.RangerPolicyItemAccess());
      //
      //      RangerPolicy.RangerPolicyItemAccess access = new
      // RangerPolicy.RangerPolicyItemAccess();
      ////      access.setType("select");
      //      String serviceType = "hive";
      //      RangerServiceDef serviceDef =
      // EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(serviceType);
      //      serviceDef.getAccessTypes().forEach(accessType -> {
      //        if (accessType.getName().equals("select")) {
      ////          accessType.getImpliedGrants().forEach(access::setType);
      //          access.setType(accessType.getName());
      //        }
      //      });
      //
      ////      List<String> users = getList("user");
      ////      List<String> groups = getList("group");
      ////      List<RangerPolicy.RangerPolicyItemCondition> conditions = getList(new
      // RangerPolicy.RangerPolicyItemCondition());
      //
      ////      policyItem.getAccesses().add(new RangerPolicy.RangerPolicyItemAccess());
      //      policyItem.setAccesses(Lists.newArrayList(access));
      //
      //      policyItem.setUsers(Lists.newArrayList("user"));
      //      policyItem.setGroups(Lists.newArrayList("group"));
      //      policyItem.setConditions(Lists.newArrayList(new
      // RangerPolicy.RangerPolicyItemCondition(RangerRef.IMPLICIT_CONDITION_EXPRESSION_NAME,
      // Collections.singletonList("TAG.attr1 == 'value1'"))));
      //
      //      policy.setPolicyItems(Lists.newArrayList(policyItem));

      rangerClient.createPolicy(policy);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    final AuditInfo auditInfo =
        AuditInfo.builder()
            .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
            .withCreateTime(Instant.now())
            .build();
    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName(name)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList())
            .withProperties(ImmutableMap.of())
            .build();
    return role;
  }

  public Role createRole2(String name) throws UnsupportedOperationException {
    try {
      //      RangerService rangerService =
      // rangerClient.getService(serviceName);//.getService(serviceName);
      //      LOG.info(rangerService.getName());

      //      RangerPolicy policy2 = rangerClient.getPolicy(15);
      //      LOG.info(policy2.getName());

      RangerPolicy policy = new RangerPolicy();
      policy.setService(rangerServiceName);
      policy.setName(name);

      RangerPolicy.RangerPolicyResource policyResource1 = new RangerPolicy.RangerPolicyResource();
      policyResource1.setValues(Lists.newArrayList("default"));
      //      policy.setResources(ImmutableMap.of("database", policyResource1));

      RangerPolicy.RangerPolicyResource policyResource2 = new RangerPolicy.RangerPolicyResource();
      policyResource2.setValues(Lists.newArrayList("tab1*"));
      //      policy.setResources(ImmutableMap.of("table", policyResource2));

      RangerPolicy.RangerPolicyResource policyResource3 = new RangerPolicy.RangerPolicyResource();
      policyResource3.setValues(Lists.newArrayList("*"));
      policy.setResources(
          ImmutableMap.of(
              "database", policyResource1, "table", policyResource2, "column", policyResource3));

      RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
      //      List<RangerPolicy.RangerPolicyItemAccess> accesses = getList(new
      // RangerPolicy.RangerPolicyItemAccess());

      RangerPolicy.RangerPolicyItemAccess access = new RangerPolicy.RangerPolicyItemAccess();
      //      access.setType("select");
      String serviceType = "hive";
      RangerServiceDef serviceDef =
          EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(serviceType);
      serviceDef
          .getAccessTypes()
          .forEach(
              accessType -> {
                if (accessType.getName().equals("select")) {
                  //          accessType.getImpliedGrants().forEach(access::setType);
                  access.setType(accessType.getName());
                }
              });

      //      List<String> users = getList("user");
      //      List<String> groups = getList("group");
      //      List<RangerPolicy.RangerPolicyItemCondition> conditions = getList(new
      // RangerPolicy.RangerPolicyItemCondition());

      //      policyItem.getAccesses().add(new RangerPolicy.RangerPolicyItemAccess());
      policyItem.setAccesses(Lists.newArrayList(access));

      policyItem.setUsers(Lists.newArrayList("user"));
      policyItem.setGroups(Lists.newArrayList("group"));
      policyItem.setConditions(
          Lists.newArrayList(
              new RangerPolicy.RangerPolicyItemCondition(
                  RangerRef.IMPLICIT_CONDITION_EXPRESSION_NAME,
                  Collections.singletonList("TAG.attr1 == 'value1'"))));

      policy.setPolicyItems(Lists.newArrayList(policyItem));

      rangerClient.createPolicy(policy);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    final AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    final Map<String, String> map = ImmutableMap.of("k1", "v1", "k2", "v2");
    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName(name)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList())
            .withProperties(map)
            .build();
    return role;
  }

  /**
   * If the role's object privilege is exist, then return exception. If the role's object privilege
   * is not exist, then return true.
   */
  public Boolean dropRole1(Role role) throws UnsupportedOperationException {
    // Check whether the role have object privilege
    Iterator<SecurableObject> iter = role.securableObjects().iterator();
    while (iter.hasNext()) {
      SecurableObject securableObject = iter.next();

      RangerPolicy policy = findManagedPolicy(securableObject, false);
      if (policy != null) {
        LOG.warn(
            "The role({}) have object privilege({}) is exist!",
            role.name(),
            securableObject.fullName());
        return false;
      }
    }

    return true;
  }

  public Role updateRole1(String roleName, RoleChange... changes)
      throws UnsupportedOperationException {
    for (RoleChange change : changes) {
      if (change instanceof RoleChange.AddSecurableObject) {
        doAddSecurableObject(roleName, (RoleChange.AddSecurableObject) change);
      } else {
        throw new IllegalArgumentException(
            "Unsupported role change type: "
                + (change == null ? "null" : change.getClass().getSimpleName()));
      }
    }
    return null;
  }

  private boolean doAddSecurableObject(String roleName, RoleChange.AddSecurableObject change) {
    RangerPolicy policy = findManagedPolicy(change.getSecurableObject(), false);

    if (policy != null) {
      // If exist policy and have same privilege, then directly return true.
      Set<String> policyPrivileges =
          policy.getPolicyItems().stream()
              .flatMap(policyItem -> policyItem.getAccesses().stream())
              .map(RangerPolicy.RangerPolicyItemAccess::getType)
              .collect(Collectors.toSet());
      Set<String> newPrivileges =
          change.getSecurableObject().privileges().stream()
              .map(privilege -> translatePrivilege(privilege.name()))
              .collect(Collectors.toSet());
      if (policyPrivileges.containsAll(newPrivileges)) {
        LOG.info(
            "The privilege({}) already added to Ranger policy({})!",
            policy.getName(),
            change.getSecurableObject().fullName());
        return true;
      }
      // Update the policy name
      policy.setName(formatPolicyName(roleName, change.getSecurableObject().fullName()));
      // Clear exist policy items for support idempotent operation.
      policy.getPolicyItems().clear();
    } else {
      policy = new RangerPolicy();
      policy.setService(rangerServiceName);
      policy.setName(formatPolicyName(roleName, change.getSecurableObject().fullName()));
      policy.setPolicyLabels(Lists.newArrayList(MANAGED_BY_GRAVITINO));

      List<String> entities =
          SecurableObjects.DOT_SPLITTER.splitToList(change.getSecurableObject().fullName());
      if (entities.size() > 5) {
        // The max level of the securable object is `metalake.catalog.db.table.column`
        throw new RuntimeException("The securable object than 5");
      }

      for (int i = 0; i < entities.size(); i++) {
        RangerPolicy.RangerPolicyResource policyResource =
            new RangerPolicy.RangerPolicyResource(entities.get(i));
        policy
            .getResources()
            .put(
                i == 1
                    ? RangerRef.RESOURCE_DATABASE
                    : i == 2 ? RangerRef.RESOURCE_TABLE : RangerRef.RESOURCE_COLUMN,
                policyResource);
      }
    }

    updatePolicyItemAccess(policy, change.getSecurableObject());
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

  private boolean doRemoveSecurableObject(
      String roleName, RoleChange.RemoveSecurableObject change) {
    RangerPolicy policy = findManagedPolicy(change.getSecurableObject(), false);

    if (policy != null) {
      // If exist policy and have same privileges.
      Set<String> policyPrivileges =
          policy.getPolicyItems().stream()
              .flatMap(policyItem -> policyItem.getAccesses().stream())
              .map(RangerPolicy.RangerPolicyItemAccess::getType)
              .collect(Collectors.toSet());
      Set<String> newPrivileges =
          change.getSecurableObject().privileges().stream()
              .map(privilege -> translatePrivilege(privilege.name()))
              .collect(Collectors.toSet());
      if (policyPrivileges.containsAll(newPrivileges)) {
        try {
          rangerClient.deletePolicy(policy.getId());
        } catch (RangerServiceException e) {
          throw new RuntimeException(e);
        }
      } else {
        LOG.warn(
            "The policy({}) have different privilege({})!",
            policy.getName(),
            change.getSecurableObject().fullName());
      }
    } else {
      LOG.warn(
          "Cannot find the policy({}) for the securable object({})!",
          roleName,
          change.getSecurableObject().fullName());
    }
    return true;
  }

  /**
   * 1. Find the policy by the securable object. <br>
   * 2. If the policy is exist, then user new securable object's privilege to update this policy.
   * <br>
   * 3. If the policy is not exist return false. <br>
   */
  private boolean doUpdateSecurableObject(
      String roleName, RoleChange.UpdateSecurableObject change) {
    // Must same entity
    RangerPolicy policy = findManagedPolicy(change.getSecurableObject(), false);

    if (policy != null) {
      updatePolicyItemAccess(policy, change.getNewSecurableObject());
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
   * Use the securable object's privileges to update policy item access items.
   * */
  private void updatePolicyItemAccess(RangerPolicy policy, SecurableObject securableObject) {
    // First check the privilege is support in the Ranger
    securableObject
            .privileges()
            .forEach(
                    privilege -> {
                      check(
                              checkPrivilege(privilege.name()),
                              "This privilege %s is not support in the Ranger hive authorization",
                              privilege.name());
                    });

    // Clean the policy items
    policy.getPolicyItems().clear();
    policy.getDenyPolicyItems().clear();
    policy.getDataMaskPolicyItems().clear();
    policy.getRowFilterPolicyItems().clear();

    // Update the policy items
    securableObject
        .privileges()
        .forEach(
            privilege -> {
              RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
              RangerPolicy.RangerPolicyItemAccess access =
                  new RangerPolicy.RangerPolicyItemAccess();
              access.setType(translatePrivilege(privilege.name()));
              policyItem.getAccesses().add(access);
              policyItem.setUsers(Lists.newArrayList(POLICY_ITEM_OWNER_USER));
              if (Privilege.Condition.ALLOW == privilege.condition()) {
                policy.getPolicyItems().add(policyItem);
              } else {
                policy.getDenyPolicyItems().add(policyItem);
              }
            });
  }
}
