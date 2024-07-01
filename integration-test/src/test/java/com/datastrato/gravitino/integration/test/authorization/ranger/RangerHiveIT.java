/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.authorization.ranger;

import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.container.RangerContainer;
import com.google.common.collect.ImmutableMap;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

@Tag("gravitino-docker-it")
public class RangerHiveIT extends RangerIT {
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static Connection adminConnection;
  private static Connection anonymousConnection;
  private static final String adminUser = "datastrato";
  private static final String anonymouslUser = "anonymous";
  @BeforeAll
  public static void setup() {
    RangerIT.setup();

    containerSuite.startHiveContainer(true,
            ImmutableMap.of(
                    RangerContainer.DOCKER_ENV_RANGER_SERVER_URL, String.format("http://%s:%d",
                            containerSuite.getRangerContainer().getContainerIpAddress(),
                            RangerContainer.RANGER_SERVER_PORT),
            RangerContainer.DOCKER_ENV_RANGER_HIVE_REPOSITORY_NAME, RangerIT.RANGER_HIVE_REPO_NAME,
                    RangerContainer.DOCKER_ENV_RANGER_HDFS_REPOSITORY_NAME, RangerIT.RANGER_HDFS_REPO_NAME));

    createRangerHdfsRepository(containerSuite.getHiveContainer().getContainerIpAddress());
    createRangerHiveRepository(containerSuite.getHiveContainer().getContainerIpAddress());
    allowAnonymousVisitHDFS();

      // Create hive connection
    String url = String.format("jdbc:hive2://%s:%d/default", containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_SERVICE_PORT);
    try {
      Class.forName("org.apache.hive.jdbc.HiveDriver");
      adminConnection = DriverManager.getConnection(url, adminUser, "");
      anonymousConnection = DriverManager.getConnection(url, anonymouslUser, "");
    } catch (ClassNotFoundException | SQLException e) {
        throw new RuntimeException(e);
    }
  }

  @AfterAll
  public static void cleanup() {
  }

  static void allowAnonymousVisitHDFS() {
    Map<String, RangerPolicy.RangerPolicyResource> policyResourceMap = ImmutableMap.of(
            RangerRef.RESOURCE_PATH, new RangerPolicy.RangerPolicyResource("/*")
    );
    RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
    policyItem.setUsers(Arrays.asList(RangerRef.CURRENT_USER));
    policyItem.setAccesses(Arrays.asList(
            new RangerPolicy.RangerPolicyItemAccess(RangerRef.ACCESS_TYPE_HDFS_READ),
            new RangerPolicy.RangerPolicyItemAccess(RangerRef.ACCESS_TYPE_HDFS_WRITE),
            new RangerPolicy.RangerPolicyItemAccess(RangerRef.ACCESS_TYPE_HDFS_EXECUTE)));
    updateOrCreateRangerPolicy(RangerRef.SERVICE_TYPE_HFDS, RANGER_HDFS_REPO_NAME, "policy1",
            policyResourceMap, Collections.singletonList(policyItem));
  }

  @Test
  public void testAllowShowDatabase() throws Exception {
    String catalogName = "catalog";
    String dbName = "db1";
    String tabName = "tab1";

    Map<String, RangerPolicy.RangerPolicyResource> policyResourceMap = ImmutableMap.of(
            RangerRef.RESOURCE_DATABASE, new RangerPolicy.RangerPolicyResource(dbName)/*,
            RangerRef.RESOURCE_TABLE, new RangerPolicy.RangerPolicyResource(tabName)*/
    );
    RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
    policyItem.setUsers(Arrays.asList(adminUser));
    policyItem.setAccesses(Arrays.asList(new RangerPolicy.RangerPolicyItemAccess("all")));
    createRangerHivePolicy("policy1", policyResourceMap, Collections.singletonList(policyItem));

    System.out.println("*** List the existing Databases....");
    Statement stmt = adminConnection.createStatement();
    stmt.execute(String.format("CREATE DATABASE %s", dbName));

    String sql = "show databases";
    System.out.println("Executing Query: " + sql);
    ResultSet rs = stmt.executeQuery(sql);
    while (rs.next()) {
      System.out.println(rs.getString(1));
    }

    Statement stmt2 = anonymousConnection.createStatement();
    String sql2 = "show databases";
    System.out.println("Executing Query: " + sql2);
    ResultSet rs2 = stmt2.executeQuery(sql);
    while (rs2.next()) {
      System.out.println(rs2.getString(1));
    }


  }
}
