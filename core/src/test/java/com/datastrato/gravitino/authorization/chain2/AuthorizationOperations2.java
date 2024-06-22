package com.datastrato.gravitino.authorization.chain2;

import java.util.function.Function;

public class AuthorizationOperations2 implements AuthorizationOperations {

  @Override
  public boolean func1(String param1) {
    System.out.println("AO2 with String: " + param1);
    return param1.contains("a");
  }

  @Override
  public boolean func2(int param1, int param2) {
    System.out.println("AO2 with Integers: " + param1 + ", " + param2);
    return param1 * param2 > 1000;
  }

  @Override
  public boolean func3(double param1, double param2, double param3, double param4) {
    System.out.println("AO2 with Doubles: " + param1 + ", " + param2 + ", " + param3 + ", " + param4);
    return param1 * param2 * param3 * param4 < 1000.0;
  }
}
