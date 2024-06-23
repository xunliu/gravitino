package com.datastrato.gravitino.authorization.chain.authorization1;

import com.datastrato.gravitino.authorization.AuthorizationOperations;
import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.authorization.RoleChange;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class TestAuthorizationOperations1 implements AuthorizationOperations {
  public Map<String, String> mapPermsion = new HashMap<>();

  @Override
  public Role createRole(String name) throws UnsupportedOperationException {
    mapPermsion.put("createRole", "TestAuthorizationOperations1");
    return null;
  }

  @Override
  public Boolean dropRole(Role role) throws UnsupportedOperationException {
    mapPermsion.put("dropRole", "TestAuthorizationOperations1");
    return false;
  }

  @Override
  public Boolean toUser(String userName) throws UnsupportedOperationException {
    mapPermsion.put("toUser", "TestAuthorizationOperations1");
    return false;
  }

  @Override
  public Boolean toGroup(String userName) throws UnsupportedOperationException {
    mapPermsion.put("toGroup", "TestAuthorizationOperations1");
    return false;
  }

  @Override
  public Role updateRole(String name, RoleChange... changes) throws UnsupportedOperationException {
    mapPermsion.put("updateRole", "TestAuthorizationOperations1");
    return null;
  }
}
