package com.datastrato.gravitino.authorization.chain;

import java.util.function.Function;

public class FunctionWithParam<T> {
  private final Function<T, Function<T, T>> function;
  private final T param;

  public FunctionWithParam(Function<T, Function<T, T>> function, T param) {
    this.function = function;
    this.param = param;
  }

  public Function<T, T> getFunction() {
    return function.apply(param);
  }
}
