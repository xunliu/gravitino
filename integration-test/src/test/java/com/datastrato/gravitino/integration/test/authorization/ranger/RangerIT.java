/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.authorization.ranger;

import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.RangerContainer;
import com.datastrato.gravitino.integration.test.container.TrinoContainer;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerIT {
  private static final Logger LOG = LoggerFactory.getLogger(RangerIT.class);
  protected static final String RANGER_TRINO_REPO_NAME = "trinoDev";
  private static final String RANGER_TRINO_TYPE = "trino";
  protected static final String RANGER_HIVE_REPO_NAME = "hiveDev";
  private static final String RANGER_HIVE_TYPE = "hive";
  private static RangerClient rangerClient;

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  @BeforeAll
  public static void setup() {
    containerSuite.startRangerContainer();

    rangerClient = containerSuite.getRangerContainer().rangerClient;
  }

  @AfterAll
  public static void cleanup() throws RangerServiceException {
    if (rangerClient != null) {
      if (rangerClient.getService(RANGER_TRINO_REPO_NAME)!=null) {
        rangerClient.deleteService(RANGER_TRINO_REPO_NAME);
      }
      if (rangerClient.getService(RANGER_HIVE_REPO_NAME)!=null) {
        rangerClient.deleteService(RANGER_HIVE_REPO_NAME);
      }
    }
  }

  public void createRangerTrinoRepository(String tirnoIp) {
    String usernameKey = "username";
    String usernameVal = "admin";
    String jdbcKey = "jdbc.driverClassName";
    String jdbcVal = "io.trino.jdbc.TrinoDriver";
    String jdbcUrlKey = "jdbc.url";
    String jdbcUrlVal = String.format("http:hive2://%s:%d", tirnoIp, TrinoContainer.TRINO_PORT);

    RangerService service = new RangerService();
    service.setType(RANGER_TRINO_TYPE);
    service.setName(RANGER_TRINO_REPO_NAME);
    service.setConfigs(
        ImmutableMap.<String, String>builder()
            .put(usernameKey, usernameVal)
            .put(jdbcKey, jdbcVal)
            .put(jdbcUrlKey, jdbcUrlVal)
            .build());

    try {
      RangerService createdService = rangerClient.createService(service);
      Assertions.assertNotNull(createdService);

      Map<String, String> filter = Collections.emptyMap();
      List<RangerService> services = rangerClient.findServices(filter);
      Assertions.assertEquals(services.get(0).getName(), RANGER_TRINO_REPO_NAME);
      Assertions.assertEquals(services.get(0).getType(), RANGER_TRINO_TYPE);
      Assertions.assertEquals(services.get(0).getConfigs().get(usernameKey), usernameVal);
      Assertions.assertEquals(services.get(0).getConfigs().get(jdbcKey), jdbcVal);
      Assertions.assertEquals(services.get(0).getConfigs().get(jdbcUrlKey), jdbcUrlVal);
    } catch (RangerServiceException e) {
      throw new RuntimeException(e);
    }
  }

  public static void createRangerHiveRepository(String hiveIp) {
    try {
      if(null != rangerClient.getService(RANGER_HIVE_REPO_NAME)) {
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
    String jdbcUrlVal = String.format("jdbc:hive2://%s:%d", hiveIp, RangerContainer.RANGER_SERVER_PORT);

    RangerService service = new RangerService();
    service.setType(RANGER_HIVE_TYPE);
    service.setName(RANGER_HIVE_REPO_NAME);
    service.setConfigs(
            ImmutableMap.<String, String>builder()
                    .put(usernameKey, usernameVal)
                    .put(passwordKey, passwordVal)
                    .put(jdbcKey, jdbcVal)
                    .put(jdbcUrlKey, jdbcUrlVal)
                    .build());

      try {
        RangerService createdService = rangerClient.createService(service);
        Assertions.assertNotNull(createdService);

        Map<String, String> filter = Collections.emptyMap();
        List<RangerService> services = rangerClient.findServices(filter);
        Assertions.assertEquals(services.get(0).getName(), RANGER_HIVE_REPO_NAME);
        Assertions.assertEquals(services.get(0).getType(), RANGER_HIVE_TYPE);
        Assertions.assertEquals(services.get(0).getConfigs().get(usernameKey), usernameVal);
        Assertions.assertEquals(services.get(0).getConfigs().get(jdbcKey), jdbcVal);
        Assertions.assertEquals(services.get(0).getConfigs().get(jdbcUrlKey), jdbcUrlVal);
      } catch (RangerServiceException e) {
          throw new RuntimeException(e);
      }
  }

  protected void createRangerHivePolicy(String policyName,
                                        Map<String, RangerPolicy.RangerPolicyResource> policyResourceMap,
                                        List<RangerPolicy.RangerPolicyItem> policyItems) {
    RangerPolicy policy = new RangerPolicy();
    policy.setService(RANGER_HIVE_REPO_NAME);
    policy.setName(policyName);
    policy.setResources(policyResourceMap);
    policy.setPolicyItems(policyItems);

    try {
      rangerClient.createPolicy(policy);
    } catch (RangerServiceException e) {
      throw new RuntimeException(e);
    }
  }

  protected boolean createRangerHiveACL2(SecurableObject securableObject, String user, String grouup) {
    RangerPolicy policy = new RangerPolicy();
    policy.setService(RANGER_HIVE_REPO_NAME);
    policy.setName(securableObject.fullName());

    final Splitter DOT_SPLITTER = Splitter.on('.');
    List<String> objects =
            DOT_SPLITTER.splitToList(securableObject.fullName());

    for (int i = 1; i < objects.size(); i++) {
      // Skip `catalog` in the securable object
      RangerPolicy.RangerPolicyResource policyResource =
              new RangerPolicy.RangerPolicyResource(objects.get(i));
      policy.getResources().put(i == 1 ?
                      RangerRef.RESOURCE_DATABASE : i == 2 ?
                      RangerRef.RESOURCE_TABLE : RangerRef.RESOURCE_COLUMN,
              policyResource);
    }

    securableObject
            .privileges()
            .forEach(
                    privilege -> {
                      RangerPolicy.RangerPolicyItem policyItem =
                              new RangerPolicy.RangerPolicyItem();
                      RangerPolicy.RangerPolicyItemAccess access =
                              new RangerPolicy.RangerPolicyItemAccess();
                      access.setType("all");
                      policyItem.getAccesses().add(access);
                      policyItem.setUsers(Lists.newArrayList(user));
                    });

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
}
