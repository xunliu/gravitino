package com.datastrato.gravitino.authorization.chain;

import java.util.function.Function;

public interface AuthorizationOperations {
    Function<String, String> func1(String param);
    Function<String, String> func2(String param);
    Function<String, String> func3(String param);
}