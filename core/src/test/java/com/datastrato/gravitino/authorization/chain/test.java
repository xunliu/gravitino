package com.datastrato.gravitino.authorization.chain;

import java.util.function.Function;

public class test {
  public static void main(String[] args) {
    // 决定使用哪个操作类
    String type = "Type2"; // 或者 "Type2"

    // 创建 AuthorizationChain 实例
    AuthorizationChain chain = new AuthorizationChain();

    // 设置函数和对应的参数，直接将函数调用结果传递给 buildChain
    Function<AuthorizationOperations, Function<String, String>> func1 = ops -> ops.func1("abc");
    Function<AuthorizationOperations, Function<String, String>> func2 = ops -> ops.func2("def");
    Function<AuthorizationOperations, Function<String, String>> func3 = ops -> ops.func3("ghi");

    // 根据不同类型构建带参数的链式调用
    Function<String, String> resultChain = chain.buildChain(type, func1, func2, func3);

    // 输入字符串并使用链式调用函数
    String result = resultChain.apply("input");

    // 打印结果
    System.out.println(result);
  }
}
