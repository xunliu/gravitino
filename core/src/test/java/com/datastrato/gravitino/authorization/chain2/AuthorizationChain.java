package com.datastrato.gravitino.authorization.chain2;

import java.util.function.Function;

public class AuthorizationChain {

  private AuthorizationOperations operations;

  public AuthorizationChain(String type) {
    switch (type) {
      case "Type1":
        operations = new AuthorizationOperations1();
        break;
      case "Type2":
        operations = new AuthorizationOperations2();
        break;
      default:
        throw new IllegalArgumentException("Unknown type: " + type);
    }
  }

  // 用于接收不同类型的函数和可变参数
  @SafeVarargs
  public final <R> void runChain(Function<AuthorizationOperations, R>... functions) {
    for (Function<AuthorizationOperations, R> function : functions) {
      function.apply(operations);
//      if (!result) {
//        return false; // 如果任何一个函数返回 false，则中断并返回 false
//      }
    }
//    return true;
  }
}