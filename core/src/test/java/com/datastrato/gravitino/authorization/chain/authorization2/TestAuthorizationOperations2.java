package com.datastrato.gravitino.authorization.chain.authorization2;

import com.datastrato.gravitino.authorization.AuthorizationOperations;
import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.authorization.RoleChange;

import java.util.HashMap;
import java.util.Map;

public class TestAuthorizationOperations2 implements AuthorizationOperations {
  public Map<String, String> mapPermsion = new HashMap<>();

  @Override
  public Role createRole(String name) throws UnsupportedOperationException {
    mapPermsion.put("createRole", "TestAuthorizationOperations2");
    return null;
  }

  @Override
  public Boolean dropRole(Role role) throws UnsupportedOperationException {
    mapPermsion.put("dropRole", "TestAuthorizationOperations2");
    return false;
  }

  @Override
  public Boolean toUser(String userName) throws UnsupportedOperationException {
    mapPermsion.put("toUser", "TestAuthorizationOperations2");
    return false;
  }

  @Override
  public Boolean toGroup(String userName) throws UnsupportedOperationException {
    mapPermsion.put("toGroup", "TestAuthorizationOperations2");
    return false;
  }

  @Override
  public Role updateRole(String name, RoleChange... changes) throws UnsupportedOperationException {
    mapPermsion.put("updateRole", "TestAuthorizationOperations2");
    return null;
  }
}
