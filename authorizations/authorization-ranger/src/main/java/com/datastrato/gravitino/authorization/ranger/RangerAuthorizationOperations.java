/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.ranger;

import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.authorization.AuthorizationOperations;
import com.datastrato.gravitino.authorization.AuthorizationRole;
import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.authorization.RoleChange;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.utils.NamespaceUtil;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class RangerAuthorizationOperations implements AuthorizationOperations, AuthorizationRole {
  private static final Logger LOG = LoggerFactory.getLogger(RangerAuthorizationOperations.class);

  private static RangerClient rangerClient;
  private String rangerUrl = "http://localhost:6080";
  private static final String username = "admin";
  // Apache Ranger Password should be minimum 8 characters with min one alphabet and one numeric.
  private static final String password = "rangerR0cks!";
  /* for kerberos authentication:
  authType = "kerberos"
  username = principal
  password = path of the keytab file */
  private static final String authType = "simple";

  String serviceName = "hivedev";
  String hiveType = "hive";

  public RangerAuthorizationOperations() {}

  @Override
  public void initialize(Map<String, String> config) throws RuntimeException {
//    String usernameKey = "username";
//    String usernameVal = "admin";
//    String jdbcKey = "jdbc.driverClassName";
//    String jdbcVal = "io.trino.jdbc.TrinoDriver";
//    String jdbcUrlKey = "jdbc.url";
//    String jdbcUrlVal = "http://localhost:8080";

    rangerClient = new RangerClient(rangerUrl, authType, username, password, null);
    LOG.info("Ranger client initialized");
  }

  @Override
  public void close() throws IOException {}

  /**
   * Because Ranger does not support roles, so we will create a policy with the role name as the
   * policy name.
   * 1. Create a policy with the role name as the policy name.
   * 2. Because every Ranger policy must contain a unique resource, we will use a random UUID as
   *    the resource value. You can set real resource values in next steps.
   *    - database: random UUID
   * */
  @Override
  public Role createRole(String name) throws UnsupportedOperationException {
    try {
      RangerPolicy policy = new RangerPolicy();
      policy.setService(serviceName);
      policy.setName(name);

      RangerPolicy.RangerPolicyResource policyResource1 = new RangerPolicy.RangerPolicyResource();
      policyResource1.setValues(Lists.newArrayList("NEW_ROLE-" +  UUID.randomUUID().toString()));
//      policy.setResources(ImmutableMap.of("database", policyResource1));

//      RangerPolicy.RangerPolicyResource policyResource2 = new RangerPolicy.RangerPolicyResource();
//      policyResource2.setValues(Lists.newArrayList("tab1*"));
//      policy.setResources(ImmutableMap.of("table", policyResource2));

      RangerPolicy.RangerPolicyResource policyResource3 = new RangerPolicy.RangerPolicyResource();
      policyResource3.setValues(Lists.newArrayList("*"));
      policy.setResources(ImmutableMap.of("database", policyResource1));


//      RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
////      List<RangerPolicy.RangerPolicyItemAccess> accesses = getList(new RangerPolicy.RangerPolicyItemAccess());
//
//      RangerPolicy.RangerPolicyItemAccess access = new RangerPolicy.RangerPolicyItemAccess();
////      access.setType("select");
//      String serviceType = "hive";
//      RangerServiceDef serviceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(serviceType);
//      serviceDef.getAccessTypes().forEach(accessType -> {
//        if (accessType.getName().equals("select")) {
////          accessType.getImpliedGrants().forEach(access::setType);
//          access.setType(accessType.getName());
//        }
//      });
//
////      List<String> users = getList("user");
////      List<String> groups = getList("group");
////      List<RangerPolicy.RangerPolicyItemCondition> conditions = getList(new RangerPolicy.RangerPolicyItemCondition());
//
////      policyItem.getAccesses().add(new RangerPolicy.RangerPolicyItemAccess());
//      policyItem.setAccesses(Lists.newArrayList(access));
//
//      policyItem.setUsers(Lists.newArrayList("user"));
//      policyItem.setGroups(Lists.newArrayList("group"));
//      policyItem.setConditions(Lists.newArrayList(new RangerPolicy.RangerPolicyItemCondition(RangerRef.IMPLICIT_CONDITION_EXPRESSION_NAME, Collections.singletonList("TAG.attr1 == 'value1'"))));
//
//      policy.setPolicyItems(Lists.newArrayList(policyItem));

      rangerClient.createPolicy(policy);
    } catch (Exception e) {
        throw new RuntimeException(e);
    }

    final AuditInfo auditInfo =
          AuditInfo.builder().withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                  .withCreateTime(Instant.now()).build();
    RoleEntity role =
            RoleEntity.builder()
                    .withId(1L)
                    .withName(name)
                    .withAuditInfo(auditInfo)
                    .withSecurableObjects(Lists.newArrayList())
                    .withProperties(ImmutableMap.of())
                    .withPolicies(Lists.newArrayList())
                    .build();
    return role;
  }

  public Role createRole2(String name) throws UnsupportedOperationException {
    try {
//      RangerService rangerService = rangerClient.getService(serviceName);//.getService(serviceName);
//      LOG.info(rangerService.getName());

//      RangerPolicy policy2 = rangerClient.getPolicy(15);
//      LOG.info(policy2.getName());

      RangerPolicy policy = new RangerPolicy();
      policy.setService(serviceName);
      policy.setName(name);

      RangerPolicy.RangerPolicyResource policyResource1 = new RangerPolicy.RangerPolicyResource();
      policyResource1.setValues(Lists.newArrayList("default"));
//      policy.setResources(ImmutableMap.of("database", policyResource1));

      RangerPolicy.RangerPolicyResource policyResource2 = new RangerPolicy.RangerPolicyResource();
      policyResource2.setValues(Lists.newArrayList("tab1*"));
//      policy.setResources(ImmutableMap.of("table", policyResource2));

      RangerPolicy.RangerPolicyResource policyResource3 = new RangerPolicy.RangerPolicyResource();
      policyResource3.setValues(Lists.newArrayList("*"));
      policy.setResources(ImmutableMap.of("database", policyResource1, "table", policyResource2, "column", policyResource3));


      RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
//      List<RangerPolicy.RangerPolicyItemAccess> accesses = getList(new RangerPolicy.RangerPolicyItemAccess());

      RangerPolicy.RangerPolicyItemAccess access = new RangerPolicy.RangerPolicyItemAccess();
//      access.setType("select");
      String serviceType = "hive";
      RangerServiceDef serviceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(serviceType);
      serviceDef.getAccessTypes().forEach(accessType -> {
        if (accessType.getName().equals("select")) {
//          accessType.getImpliedGrants().forEach(access::setType);
          access.setType(accessType.getName());
        }
      });

//      List<String> users = getList("user");
//      List<String> groups = getList("group");
//      List<RangerPolicy.RangerPolicyItemCondition> conditions = getList(new RangerPolicy.RangerPolicyItemCondition());

//      policyItem.getAccesses().add(new RangerPolicy.RangerPolicyItemAccess());
      policyItem.setAccesses(Lists.newArrayList(access));

      policyItem.setUsers(Lists.newArrayList("user"));
      policyItem.setGroups(Lists.newArrayList("group"));
      policyItem.setConditions(Lists.newArrayList(new RangerPolicy.RangerPolicyItemCondition(RangerRef.IMPLICIT_CONDITION_EXPRESSION_NAME, Collections.singletonList("TAG.attr1 == 'value1'"))));

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
                    .withPolicies(Lists.newArrayList())
                    .build();
    return role;
  }

  private <T> List<T> getList(T value) {
    List<T> ret = new ArrayList<>();

    int count = getRandomNumber(10);
    for(int i = 0; i < count; i ++) {
      ret.add(value);
    }

    return ret;
  }

  private int getRandomNumber(int maxValue) {
    return (int)(Math.random() * maxValue);
  }

  @Override
  public boolean dropRole(String name) throws UnsupportedOperationException {
    return false;
  }

  @Override
  public Role updateRole(String roleName, RoleChange... changes) throws UnsupportedOperationException {
    for (RoleChange change : changes) {
      if (change instanceof RoleChange.RenameRole) {
        doRenameRole((RoleChange.RenameRole) change);
      } else if (change instanceof RoleChange.AddSecurableObject) {
        doAddSecurableObject(roleName, (RoleChange.AddSecurableObject) change);
      } else {
        throw new IllegalArgumentException(
                "Unsupported role change type: "
                        + (change == null ? "null" : change.getClass().getSimpleName()));
      }
    }
    return null;
  }

  public void createHiveDev(String name) throws RangerServiceException {
    String usernameKey = "username";
    String usernameVal = "admin";
    String jdbcKey = "jdbc.driverClassName";
    String jdbcVal = "io.trino.jdbc.TrinoDriver";
    String jdbcUrlKey = "jdbc.url";
    String jdbcUrlVal = "http://localhost:8080";

    final String hiveType = "hive";
    final String serviceName = "hivedev";

    RangerService service = new RangerService();
    service.setType(hiveType);
    service.setName(serviceName);
    service.setConfigs(
            ImmutableMap.<String, String>builder()
                    .put(usernameKey, usernameVal)
                    .put(jdbcKey, jdbcVal)
                    .put(jdbcUrlKey, jdbcUrlVal)
                    .build());

//    RangerService createdService = rangerClient.createService(service);
  }

  private String doRenameRole(RoleChange.RenameRole change) {
    return change.getNewName();
  }

  private String doAddSecurableObject(String roleName, RoleChange.AddSecurableObject change) {
      try {
        RangerPolicy policy = rangerClient.getPolicy(serviceName, roleName);
        Map<String, RangerPolicy.RangerPolicyResource> resourceMap = new ImmutableMap.Builder<String, RangerPolicy.RangerPolicyResource>().build();

        SecurableObjects.DOT.splitToList(change.getSecurableObject().fullName()).forEach(name -> {
          RangerPolicy.RangerPolicyResource policyResource = new RangerPolicy.RangerPolicyResource();
          policyResource.setValues(Lists.newArrayList(name));
          resourceMap.put(name, policyResource);
        });

      } catch (RangerServiceException e) {
          throw new RuntimeException(e);
      }

      LOG.info(roleName);
    return change.getSecurableObject().name();
  }
}
