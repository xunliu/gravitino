/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.authorization.ranger;

import com.datastrato.gravitino.MetadataObject;
import com.datastrato.gravitino.authorization.Privileges;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.container.RangerContainer;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.compress.utils.Lists;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import com.datastrato.gravitino.authorization.SecurableObject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Tag("gravitino-docker-it")
public class RangerHiveIT extends RangerIT {
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static Connection connection;
  private static String userName = "hive";
  @BeforeAll
  public static void setup() {
    RangerIT.setup();

    containerSuite.startHiveContainer(true,
            ImmutableMap.of(
                    RangerContainer.DOCKER_ENV_RANGER_SERVER_URL, String.format("http://%s:%d",
                            containerSuite.getRangerContainer().getContainerIpAddress(),
                            RangerContainer.RANGER_SERVER_PORT),
            RangerContainer.DOCKER_ENV_RANGER_HIVE_REPOSITORY_NAME, RangerIT.RANGER_HIVE_REPO_NAME));

    createRangerHiveRepository();

    // Create hive connection
    String url = String.format("jdbc:hive2://%s:%d/default", containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_SERVICE_PORT);
      try {
          Class.forName("org.apache.hive.jdbc.HiveDriver");
        connection = DriverManager.getConnection(url, userName, "");
      } catch (ClassNotFoundException | SQLException e) {
          throw new RuntimeException(e);
      }
  }

  @AfterAll
  public static void cleanup() {
  }

  @Test
  public void testAllowShowDatabase() throws Exception {
    String catalogName = "catalog";
    String dbName = "db1";
    String tabName = "tab1";


    Map<String, RangerPolicy.RangerPolicyResource> policyResourceMap = ImmutableMap.of(
            RangerRef.RESOURCE_DATABASE, new RangerPolicy.RangerPolicyResource(dbName),
            RangerRef.RESOURCE_TABLE, new RangerPolicy.RangerPolicyResource(tabName)
    );

    RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
    policyItem.setUsers(Arrays.asList(userName));
    policyItem.setAccesses(Arrays.asList(new RangerPolicy.RangerPolicyItemAccess("all")));

    createRangerHivePolicy("policy1", policyResourceMap, Collections.singletonList(policyItem));

    System.out.println("*** List the existing Databases....");
    Statement stmt = connection.createStatement();
    String sql = "show databases";
    System.out.println("Executing Query: " + sql);
    ResultSet rs = stmt.executeQuery(sql);
    while (rs.next()) {
      System.out.println(rs.getString(1));
    }
  }
}
