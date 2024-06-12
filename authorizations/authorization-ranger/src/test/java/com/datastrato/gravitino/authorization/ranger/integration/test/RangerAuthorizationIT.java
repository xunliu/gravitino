/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.ranger.integration.test;

import com.datastrato.gravitino.authorization.ranger.RangerAuthorizationOperations;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.google.common.collect.ImmutableMap;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

//@Tag("gravitino-docker-it")
//@TestInstance(Lifecycle.PER_CLASS)
public class RangerAuthorizationIT {// extends AbstractIT {
  private static final Logger LOG = LoggerFactory.getLogger(RangerAuthorizationIT.class);

  private static final String serviceName = "hivedev";
  private static final String hiveType = "hive";
  private static RangerClient rangerClient;
//  private static final String provider = "ranger";

//  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  @BeforeAll
  public static void setup() throws RangerServiceException {
//    containerSuite.startRangerContainer();

//    rangerClient = containerSuite.getRangerContainer().rangerClient;
    initRangerClient();
  }

  @AfterAll
  public static void cleanup() throws RangerServiceException {
    if (rangerClient != null) {
      rangerClient.deleteService(serviceName);
    }
  }

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

    LOG.info("");

//    createHiveService();
  }

  @Test
  public void createRole() {
    RangerAuthorizationOperations rangerAuthorizationOperations = new RangerAuthorizationOperations();
    rangerAuthorizationOperations.initialize(ImmutableMap.of("provider", "ranger"));
    rangerAuthorizationOperations.createRole("testRole");
  }

  public static void createHiveService() throws RangerServiceException {
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
    service.setName(serviceName);
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
    Assertions.assertEquals(services.get(0).getName(), serviceName);
    Assertions.assertEquals(services.get(0).getType(), hiveType);
    Assertions.assertEquals(services.get(0).getConfigs().get(usernameKey), usernameVal);
    Assertions.assertEquals(services.get(0).getConfigs().get(jdbcKey), jdbcVal);
    Assertions.assertEquals(services.get(0).getConfigs().get(jdbcUrlKey), jdbcUrlVal);
  }
}
