package com.datastrato.gravitino.integration.test.trino;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;

public class JettyServer {

  public static void main(String[] args) throws Exception {
    // 创建 Jetty 服务器并设置端口
    Server server = new Server(1000);

    // 创建 ServletContextHandler
    ServletContextHandler contextHandler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    contextHandler.setContextPath("/");
    server.setHandler(contextHandler);

    // 配置 Jersey Servlet
    ServletHolder jerseyServlet = contextHandler.addServlet(ServletContainer.class, "/*");
    jerseyServlet.setInitOrder(0);
    jerseyServlet.setInitParameter("jersey.config.server.provider.classnames", ExampleResource.class.getCanonicalName());

    try {
      // 启动服务器
      server.start();
      server.join();
    } finally {
      server.destroy();
    }
  }
}
