package com.datastrato.gravitino.authorization.chain;

import java.util.function.Function;

public class AuthorizationOperations2 implements AuthorizationOperations {
  @Override
  public Function<String, String> func1(String param) {
    return input -> input + " (from AO2) " + param;
  }

  @Override
  public Function<String, String> func2(String param) {
    return input -> "[" + param + "] " + input.toUpperCase();
  }

  @Override
  public Function<String, String> func3(String param) {
    return input -> input + "***" + param;
  }
}
