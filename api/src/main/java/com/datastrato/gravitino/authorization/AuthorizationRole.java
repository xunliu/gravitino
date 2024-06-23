/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

public interface AuthorizationRole {
  Role createRole(String name) throws UnsupportedOperationException;

  Boolean dropRole(Role role) throws UnsupportedOperationException;

  Boolean toUser(String userName) throws UnsupportedOperationException;

  Boolean toGroup(String groupName) throws UnsupportedOperationException;

  Role updateRole(String name, RoleChange... changes) throws UnsupportedOperationException;
}
