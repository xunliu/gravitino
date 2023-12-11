package com.datastrato.gravitino.integration.test.trino;

import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;

@Path("/example")
public class ExampleResource {

  TrinoAgent trinoAgent = new TrinoAgent();

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createSomething(String requestBody) {
    // 处理请求体
    System.out.println("Received POST data: " + requestBody);

//    requestBody = "WITH employeeperformance AS (  SELECT    employee_id,    AVG(rating) AS average_rating  FROM \"metalake_demo.catalog-pg\".hr.employee_performance  GROUP BY    employee_id), employeesales AS (  SELECT    employee_id,    SUM(total_amount) AS total_sales  FROM \"metalake_demo.catalog-pg\".sales.sales  GROUP BY    employee_id) SELECT  e.employee_id,  average_rating, total_sales FROM employeeperformance AS e JOIN employeesales AS s ON e.employee_id = s.employee_id";

    ArrayList<ArrayList<String>> arrayLists = trinoAgent.executeQuerySQL(requestBody);

    // 创建ObjectMapper实例
    ObjectMapper mapper = new ObjectMapper();
    String json = "";
    try {
      // 将listOfLists转换为JSON字符串
      json = mapper.writeValueAsString(arrayLists);

      // 打印JSON字符串
      System.out.println(json);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // 返回响应
    return Response.status(Response.Status.CREATED).entity(json).build();
  }
}
