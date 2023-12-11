package com.datastrato.gravitino.integration.test.trino;

import com.datastrato.gravitino.integration.test.container.TrinoContainer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class TrinoAgent {
  public static final Logger LOG = LoggerFactory.getLogger(TrinoAgent.class);
  static Connection trinoJdbcConnection = null;

  public TrinoAgent() {
    initTrinoJdbcConnection();
    executeUpdateSQL("set session allow_pushdown_into_connectors=false");
  }

//  @Test
//  public void test() {
//
////    executeQuerySQL("WITH employeeperformance AS (  SELECT    employee_id,    AVG(rating) AS average_rating  FROM \"metalake_demo.catalog-pg\".hr.employee_performance  GROUP BY    employee_id), employeesales AS (  SELECT    employee_id,    SUM(total_amount) AS total_sales  FROM \"metalake_demo.catalog-pg\".sales.sales  GROUP BY    employee_id) SELECT  e.employee_id,  average_rating, total_sales FROM employeeperformance AS e JOIN employeesales AS s ON e.employee_id = s.employee_id");
//  }

  public boolean initTrinoJdbcConnection() {
    try {
      trinoJdbcConnection = DriverManager.getConnection("jdbc:trino://127.0.0.1:8080", "admin", "");
    } catch (SQLException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
    return true;
  }

  public ArrayList<ArrayList<String>> executeQuerySQL(String sql) {
    LOG.info("executeQuerySQL: {}", sql);
    ArrayList<ArrayList<String>> queryData = new ArrayList<>();
    try (Statement stmt = trinoJdbcConnection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      ResultSetMetaData metaData = rs.getMetaData();
      int columnCount = metaData.getColumnCount();

      ArrayList<String> columns = new ArrayList<>();
      for (int i = 0; i < columnCount; i++) {
        String columnLabel = metaData.getColumnLabel(i + 1);
        LOG.info("columnLabel: {}", columnLabel);
        columns.add(columnLabel);
      }
      queryData.add(columns);

      while (rs.next()) {
        ArrayList<String> record = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
          String columnValue = rs.getString(i);
          record.add(columnValue);
        }
        queryData.add(record);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return queryData;
  }

  public void executeUpdateSQL(String sql) {
    LOG.info("executeUpdateSQL: {}", sql);
    try (Statement stmt = trinoJdbcConnection.createStatement()) {
      stmt.executeUpdate(sql);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
