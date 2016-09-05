package com.vizexplorer.challenge.toolengineer;

import static java.lang.Long.toHexString;
import static java.sql.DriverManager.getConnection;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DBTest
{

  private static Connection connection;

  @BeforeClass
  public static void openConnection() throws SQLException
  {
    String databaseName = "challenge_" + toHexString(System.nanoTime());
    connection = getConnection("jdbc:hsqldb:mem:" + databaseName, "SA", "");

    try (Statement stmt = connection.createStatement();)
    {
      stmt.execute("CREATE TABLE persons ( name VARCHAR(255), age INT )");
    }
  }

  @AfterClass
  public static void closeConnection() throws SQLException
  {
    connection.close();
  }

  @Before
  public void populateDB() throws SQLException
  {
    for (int i = 0; i < 10; i++)
    {
      try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO persons (name, age) values (?, ?)");)
      {
        stmt.setString(1, "A guy " + i);
        stmt.setInt(2, 22 + i);
        stmt.execute();
      }
    }
  }

  @Test
  public void checkAverage() throws SQLException
  {
    double result;
    try (PreparedStatement stmt = connection.prepareStatement("SELECT AVG(age) FROM persons");
        ResultSet resultSet = stmt.executeQuery();)
    {
      if (resultSet.next())
        result = resultSet.getDouble(1);
      else
        throw new AssertionError("Invalid resultset");
    }

    assertThat(result, equalTo(26.0));
  }

}
