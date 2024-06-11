/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.ranger;

import com.datastrato.gravitino.authorization.AuthorizationOperations;
import com.datastrato.gravitino.authorization.AuthorizationProvider;
import com.datastrato.gravitino.authorization.BaseAuthorization;
import java.util.Map;

/** Implementation of a Mysql catalog in Gravitino. */
public class RangerAuthorization extends BaseAuthorization<RangerAuthorization> {

  @Override
  public String shortName() {
    return "ranger";
  }

  @Override
  protected AuthorizationOperations newOps(Map<String, String> config) {
    return new RangerAuthorizationOperations();
  }

//    @Override
//    public RangerAuthorization clone() {
//        RangerAuthorization rangerAuthorization = (RangerAuthorization) super.clone();
////        try {
////            return (RangerAuthorization) super.clone();
////        } catch (CloneNotSupportedException e) {
////            throw new AssertionError();
////        }
//    }
}
