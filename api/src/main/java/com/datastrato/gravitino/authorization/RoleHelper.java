/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Audit;

import java.util.List;
import java.util.Map;
public class RoleHelper {

  public static Role createRole(String name) {
    return new RoleImpl(name);
  }

  private static class RoleImpl implements Role {
    private final String name;
    private Map<String, String> properties;

    private List<SecurableObject> securableObjects;

    private List<Policy> policies;

    RoleImpl(String name) {
      this.name = name;
    }

    @Override
    public Audit auditInfo() {
      return null;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public List<SecurableObject> securableObjects() {
      return securableObjects;
    }

    @Override
    public List<Policy> policies() {
      return policies;
    }
  }
}
