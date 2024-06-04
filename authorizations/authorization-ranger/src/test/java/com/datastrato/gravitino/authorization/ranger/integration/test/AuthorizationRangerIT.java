/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.ranger.integration.test;

import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("gravitino-docker-it")
@TestInstance(Lifecycle.PER_CLASS)
public class AuthorizationRangerIT extends AbstractIT {
  private static final String serviceName = "trino-test";
  private static final String trinoType = "trino";
  private static RangerClient rangerClient;
  private static final String provider = "ranger";

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  @BeforeAll
  public static void setup() throws RangerServiceException {
    containerSuite.startRangerContainer();

    rangerClient = containerSuite.getRangerContainer().rangerClient;
    createTrinoService();
  }

  @AfterAll
  public static void cleanup() throws RangerServiceException {
    if (rangerClient != null) {
      rangerClient.deleteService(serviceName);
    }
  }

  public static void createTrinoService() throws RangerServiceException {
    String usernameKey = "username";
    String usernameVal = "admin";
    String jdbcKey = "jdbc.driverClassName";
    String jdbcVal = "io.trino.jdbc.TrinoDriver";
    String jdbcUrlKey = "jdbc.url";
    String jdbcUrlVal = "http://localhost:8080";

    RangerService service = new RangerService();
    service.setType(trinoType);
    service.setName(serviceName);
    service.setConfigs(
            ImmutableMap.<String, String>builder()
                    .put(usernameKey, usernameVal)
                    .put(jdbcKey, jdbcVal)
                    .put(jdbcUrlKey, jdbcUrlVal)
                    .build());

    RangerService createdService = rangerClient.createService(service);
    Assertions.assertNotNull(createdService);

    Map<String, String> filter = Collections.emptyMap();
    List<RangerService> services = rangerClient.findServices(filter);
    Assertions.assertEquals(services.get(0).getName(), serviceName);
    Assertions.assertEquals(services.get(0).getType(), trinoType);
    Assertions.assertEquals(services.get(0).getConfigs().get(usernameKey), usernameVal);
    Assertions.assertEquals(services.get(0).getConfigs().get(jdbcKey), jdbcVal);
    Assertions.assertEquals(services.get(0).getConfigs().get(jdbcUrlKey), jdbcUrlVal);
  }
}
