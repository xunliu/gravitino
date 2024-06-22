package com.datastrato.gravitino.authorization;

import java.io.IOException;
import java.util.Map;

public class TestAuthorizationOperations implements AuthorizationOperations {
  public TestAuthorizationOperations(Map<String, String> config) {}

  @Override
  public void initialize(Map<String, String> config) throws RuntimeException {}

  @Override
  public String translatePrivilege(Privilege.Name name) {
    return null;
  }

  @Override
  public void close() throws IOException {}

  public static String chainTest(String s) {
    return s + "abc";
  }
}
