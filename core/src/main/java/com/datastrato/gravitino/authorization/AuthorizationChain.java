/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import java.util.function.Function;
import java.util.stream.Stream;

/* Manages the authorization instances and operations. */
public class AuthorizationChain {

  private AuthorizationOperations authorizationOperations;

  public AuthorizationChain(String type) {
//    switch (type) {
//      case "Type1":
//        operations = new AuthorizationOperations1();
//        break;
//      case "Type2":
//        operations = new AuthorizationOperations2();
//        break;
//      default:
//        throw new IllegalArgumentException("Unknown type: " + type);
//    }
  }

  // 用于接收不同类型的函数和可变参数
  @SafeVarargs
  public final boolean runChain(Function<AuthorizationOperations, Boolean>... functions) {
    for (Function<AuthorizationOperations, Boolean> function : functions) {
      boolean result = function.apply(authorizationOperations);
      if (!result) {
        return false;
      }
    }
    return true;
  }
}
