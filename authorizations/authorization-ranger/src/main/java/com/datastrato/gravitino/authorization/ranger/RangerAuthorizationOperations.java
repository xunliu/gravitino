/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.ranger;

import com.datastrato.gravitino.authorization.AuthorizationOperations;
import com.datastrato.gravitino.authorization.AuthorizationRole;
import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.authorization.RoleChange;
import com.datastrato.gravitino.authorization.RoleHelper;
import com.google.common.collect.ImmutableMap;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;

import java.io.IOException;
import java.util.Map;

public class RangerAuthorizationOperations implements AuthorizationOperations, AuthorizationRole {

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
    String usernameKey = "username";
    String usernameVal = "admin";
    String jdbcKey = "jdbc.driverClassName";
    String jdbcVal = "io.trino.jdbc.TrinoDriver";
    String jdbcUrlKey = "jdbc.url";
    String jdbcUrlVal = "http://localhost:8080";

    rangerClient = new RangerClient(rangerUrl, authType, username, password, null);
  }

  @Override
  public void close() throws IOException {}

  @Override
  public Role createRole(String name) throws UnsupportedOperationException {
//    RangerPolicy policy = new RangerPolicy(serviceName, name, );
//    rangerClient.createPolicy(policy)

    return RoleHelper.createRole(name);
  }

  @Override
  public boolean dropRole(String name) throws UnsupportedOperationException {
    return false;
  }

  @Override
  public Role updateRole(String name, RoleChange... changes) throws UnsupportedOperationException {
    for (RoleChange change : changes) {
      if (change instanceof RoleChange.RenameRole) {
        doRenameRole((RoleChange.RenameRole) change);
      } else if (change instanceof RoleChange.AddSecurableObject) {
        doAddSecurableObject((RoleChange.AddSecurableObject) change);
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

    RangerService createdService = rangerClient.createService(service);
  }

  private void doRenameRole(RoleChange.RenameRole change) {

  }

  private void doAddSecurableObject(RoleChange.AddSecurableObject change) {

  }
}
