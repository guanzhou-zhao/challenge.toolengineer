package com.vizexplorer.challenge.toolengineer;

import static java.sql.DriverManager.getConnection;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.yandex.qatools.embed.postgresql.PostgresExecutable;
import ru.yandex.qatools.embed.postgresql.PostgresProcess;
import ru.yandex.qatools.embed.postgresql.PostgresStarter;
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig;

public class DBTest
{

  private static Connection connection;
  private static PostgresProcess process;

  @BeforeClass
  public static void openConnection() throws SQLException, IOException
  {
    String databaseName = "challenge_db";
    String userName = "SA";
    String password = "123";

    // start Postgres service
    final PostgresStarter<PostgresExecutable, PostgresProcess> runtime = PostgresStarter.getDefaultInstance();
    final PostgresConfig config = PostgresConfig.defaultWithDbName(databaseName, userName, password);
    PostgresExecutable exec = runtime.prepare(config);
    process = exec.start();

   // connecting to a running Postgres
    String url = String.format("jdbc:postgresql://%s:%s/%s",
            config.net().host(),
            config.net().port(),
            config.storage().dbName()
    );
    connection = getConnection(url, userName, password);

    try (Statement stmt = connection.createStatement();)
    {
      stmt.execute("CREATE TABLE persons ( name VARCHAR(255), age INT )");
    }
  }

  @AfterClass
  public static void closeConnection() throws SQLException
  {
    connection.close();
    process.stop();
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

    assertThat(result, equalTo(26.5));
  }

}
