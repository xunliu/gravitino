package com.datastrato.gravitino.authorization.chain2;

import java.util.function.Function;

public interface AuthorizationOperations {
  boolean func1(String param1);
  boolean func2(int param1, int param2);
  boolean func3(double param1, double param2, double param3, double param4);
}
