package com.datastrato.gravitino.authorization.chain;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.authorization.AuthorizationManager;
import com.datastrato.gravitino.authorization.AuthorizationOperations;
import com.datastrato.gravitino.authorization.chain.authorization1.TestAuthorizationOperations1;
import com.datastrato.gravitino.authorization.chain.authorization2.TestAuthorizationOperations2;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;

public class TestAuthorizationChain {
  private static AuthorizationManager authorizationManager;
  private static Config config;

  private static CatalogEntity catalogTest1;
  private static CatalogEntity catalogTest2;

  @BeforeAll
  public static void setUp() throws Exception {
    AuditInfo auditInfo1 =
        AuditInfo.builder().withCreator("testIcebergUser").withCreateTime(Instant.now()).build();

    catalogTest1 =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog-test1")
            .withNamespace(Namespace.of("default"))
            .withType(Catalog.Type.RELATIONAL)
            .withProperties(ImmutableMap.of(AuthorizationManager.AUTHORIZATION_PROVIDER, "test1"))
            .withProvider("hive")
            .withAuditInfo(auditInfo1)
            .build();

    AuditInfo auditInfo2 =
            AuditInfo.builder().withCreator("testIcebergUser").withCreateTime(Instant.now()).build();
    catalogTest2 =
            CatalogEntity.builder()
                    .withId(2L)
                    .withName("catalog-test2")
                    .withNamespace(Namespace.of("default"))
                    .withType(Catalog.Type.RELATIONAL)
                    .withProperties(ImmutableMap.of(AuthorizationManager.AUTHORIZATION_PROVIDER, "test2"))
                    .withProvider("hive")
                    .withAuditInfo(auditInfo2)
                    .build();

    config = new Config(false) {};
    config.set(Configs.AUTHORIZATION_LOAD_ISOLATED, false);

    authorizationManager = new AuthorizationManager(config, null, null);
//    authorizationManager.createAuthorization(catalog1);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (authorizationManager != null) {
      authorizationManager.close();
    }
  }

  @Test
  public void testAuthorizationCatalog1() {
    boolean result = authorizationManager.runAuthorizationChain(catalogTest1,
            ops -> ops.createRole("abc"),
            ops -> ops.toUser("")
    );

    AuthorizationOperations authOps1 = authorizationManager.loadAuthorizationAndWrap(catalogTest1).getOps();
    Assertions.assertTrue(authOps1 instanceof TestAuthorizationOperations1);
    Assertions.assertEquals(((TestAuthorizationOperations1)authOps1).mapPermsion.get("createRole"), "TestAuthorizationOperations1");
    System.out.println("result: " + result);
  }

  @Test
  public void testAuthorizationCatalog2() {
    boolean result = authorizationManager.runAuthorizationChain(catalogTest2,
            ops -> ops.createRole("abc"),
            ops -> ops.toUser("")
    );

    AuthorizationOperations authOps2 = authorizationManager.loadAuthorizationAndWrap(catalogTest2).getOps();
    Assertions.assertTrue(authOps2 instanceof TestAuthorizationOperations2);
    Assertions.assertEquals(((TestAuthorizationOperations2)authOps2).mapPermsion.get("createRole"), "TestAuthorizationOperations2");
    System.out.println("result: " + result);
  }

  @Test
  public void testAuthorizationCatalog1and2() {
    boolean result = authorizationManager.runAuthorizationChain(catalogTest1,
            ops -> ops.createRole("abc"),
            ops -> ops.toUser("")
    );
    authorizationManager.runAuthorizationChain(catalogTest2,
            ops -> ops.createRole("abc"),
            ops -> ops.toUser("")
    );

    AuthorizationOperations authOps1 = authorizationManager.loadAuthorizationAndWrap(catalogTest1).getOps();
    Assertions.assertTrue(authOps1 instanceof TestAuthorizationOperations1);
    Assertions.assertEquals(((TestAuthorizationOperations1)authOps1).mapPermsion.get("createRole"), "TestAuthorizationOperations1");

    AuthorizationOperations authOps2 = authorizationManager.loadAuthorizationAndWrap(catalogTest2).getOps();
    Assertions.assertTrue(authOps2 instanceof TestAuthorizationOperations2);
    Assertions.assertEquals(((TestAuthorizationOperations2)authOps2).mapPermsion.get("createRole"), "TestAuthorizationOperations2");
    System.out.println("result: " + result);
  }

}
