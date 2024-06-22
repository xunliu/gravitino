package com.datastrato.gravitino.authorization.chain;

import java.util.function.Function;
import java.util.stream.Stream;

public class AuthorizationChain {
  public Function<String, String> buildChain(
      String type, Function<AuthorizationOperations, Function<String, String>>... functions) {
    AuthorizationOperations operations;
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

    return Stream.of(functions)
        .map(func -> func.apply(operations))
        .reduce(Function::andThen)
        .orElseThrow(() -> new IllegalArgumentException("Empty Chain found"));
  }
}
