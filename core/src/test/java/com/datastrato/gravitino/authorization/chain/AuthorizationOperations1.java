package com.datastrato.gravitino.authorization.chain;

import java.util.function.Function;

public class AuthorizationOperations1 implements AuthorizationOperations {
    @Override
    public Function<String, String> func1(String param) {
        return input -> input + param;
    }

    @Override
    public Function<String, String> func2(String param) {
        return input -> input.toUpperCase() + param;
    }

    @Override
    public Function<String, String> func3(String param) {
        return input -> input + param + param;
    }
}
