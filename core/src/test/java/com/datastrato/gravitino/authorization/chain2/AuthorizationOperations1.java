package com.datastrato.gravitino.authorization.chain2;

import java.util.function.Function;

public class AuthorizationOperations1 implements AuthorizationOperations {

  @Override
  public String func1(String param1) {
    System.out.println("Operation1 with String: " + param1);
    return String.valueOf(param1.length());
  }

  @Override
  public boolean func2(int param1, int param2) {
    throw new UnsupportedOperationException("func3 is not supported");
  }

  @Override
  public boolean func3(double param1, double param2, double param3, double param4) {
    throw new UnsupportedOperationException("func3 is not supported");
  }
}
