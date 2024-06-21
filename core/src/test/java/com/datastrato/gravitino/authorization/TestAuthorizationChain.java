package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.NameIdentifier;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class TestAuthorizationChain {
//    void testCreateNameIdentifier() {
//        AuthorizationChain chain = new AuthorizationChain();
//        chain.newChain(hiveCatalogEntity).chainOps(ChangOps.createRole(), ChangOps.createGroup(1,2,3), ChangOps.createUser(), ...)
//             .newChain(HdfsCatalogEntity).chainOps(ChangOps.createRole(), ChangOps.createGroup(), ChangOps.createUser());
//    }

    private static final Logger LOG = LoggerFactory.getLogger(TestAuthorizationChain.class);

    @Test
    void test1() {
        AuthorizationChain<String> chain = new AuthorizationChain<>();
        Function<String, String> function = chain.buildChain(
                TestAuthorizationOperations::chainTest
        );

        LOG.info(function.apply("111"));

        Map<String, Function<String, String>> chainMap = ImmutableMap.of(
                "chain1", chain.buildChain(
                        TestAuthorizationOperations::chainTest
                ),
                "chain2", chain.buildChain(
                        TestAuthorizationOperations::chainTest,
                        TestAuthorizationOperations::chainTest
                )
        );
        String result = chainMap.getOrDefault(
                "1",
                Function.identity()
        ).apply("2");
        LOG.info(result);
    }

    @Test
    void test2() {
        // 创建 AuthorizationChain 实例
        AuthorizationChain<String> chain = new AuthorizationChain<>();

        // 创建多个 Function<String, String> 函数
        Function<String, String> func1 = TestAuthorizationOperations::chainTest;
        Function<String, String> func2 = TestAuthorizationOperations::chainTest;
        Function<String, String> func3 = TestAuthorizationOperations::chainTest;

        // 构建链式调用
        Function<String, String> resultChain = chain.buildChain(func1, func2, func3);

        // 输入字符串并使用链式调用函数
        String result = resultChain.apply("input");

        // 打印结果
        System.out.println(result); // 输出 "inputabcabcabc"
    }
}