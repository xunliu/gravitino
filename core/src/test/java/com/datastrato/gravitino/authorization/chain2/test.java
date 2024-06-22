package com.datastrato.gravitino.authorization.chain2;

import java.util.function.Function;

public class test {
  public static void main(String[] args) {
    // 决定使用哪个操作类
    String type = "Type2"; // 或者 "Type2"

    // 创建 AuthorizationChain 实例
    AuthorizationChain chain = new AuthorizationChain(type);

    // 调用 runChain 方法并传递函数和参数组合
    boolean result = chain.runChain(
            ops -> ops.func1("abc"),
            ops -> ops.func2(50, 60),
            ops -> ops.func3(5.0, 10.0, 15.0, 25.0)
    );

    // 打印结果
    System.out.println("Final result: " + result);
  }
}
