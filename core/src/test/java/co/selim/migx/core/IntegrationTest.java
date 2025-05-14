package co.selim.migx.core;

import io.vertx.core.Vertx;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.List;

@Testcontainers
public abstract class IntegrationTest {

  @Container
  private final JdbcDatabaseContainer<?> flywayPgContainer = new PostgreSQLContainer<>("postgres:14.3");

  @Container
  private final JdbcDatabaseContainer<?> migxPgContainer = new PostgreSQLContainer<>("postgres:14.3");

  @Container
  private final JdbcDatabaseContainer<?> flywayMySqlContainer = new MySQLContainer<>("mysql:8.0.36");

  @Container
  private final JdbcDatabaseContainer<?> migxMySqlContainer = new MySQLContainer<>("mysql:8.0.36");

  protected enum Database {
    POSTGRES, MYSQL
  }

  protected enum MigrationTool {
    FLYWAY, MIGX
  }

  protected Flyway getFlyway(Database db, String... locations) {
    JdbcDatabaseContainer<?> container = switch (db) {
      case POSTGRES -> flywayPgContainer;
      case MYSQL -> flywayMySqlContainer;
    };
    String jdbcUrl = container.getJdbcUrl();
    String username = container.getUsername();
    String password = container.getPassword();

    return new FluentConfiguration()
      .dataSource(jdbcUrl, username, password)
      .locations(locations)
      .load();
  }

  protected List<SchemaHistoryEntry> getSchemaHistory(Database db, MigrationTool migrationTool) {
    return withConnection(db, migrationTool, connection -> {
      String query = "select * from flyway_schema_history order by installed_rank";
      try (ResultSet resultSet = connection.createStatement().executeQuery(query)) {
        return SchemaHistoryEntry.MAPPER.apply(resultSet);
      }
    });
  }

  private <T> T withConnection(Database db, MigrationTool migrationTool, ThrowingFunction<Connection, T> function) {
    JdbcDatabaseContainer<?> container = switch (migrationTool) {
      case MIGX -> switch (db) {
        case POSTGRES -> migxPgContainer;
        case MYSQL -> migxMySqlContainer;
      };
      case FLYWAY -> switch (db) {
        case POSTGRES -> flywayPgContainer;
        case MYSQL -> flywayMySqlContainer;
      };
    };
    try (Connection connection = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(), container.getPassword())) {
      return function.apply(connection);
    } catch (Throwable e) {
      Assertions.fail(e);
      throw new RuntimeException(e);
    }
  }

  protected Migx getMigx(Vertx vertx, Database db, List<String> locations) {
    JdbcDatabaseContainer<?> container = switch (db) {
      case POSTGRES -> migxPgContainer;
      case MYSQL -> migxMySqlContainer;
    };
    String jdbcUrl = container.getJdbcUrl();
    String username = container.getUsername();
    String password = container.getPassword();

    SqlConnectOptions connectOptions = switch (db) {
      case POSTGRES -> PgConnectOptions.fromUri(jdbcUrl.substring("jdbc:".length()))
        .setUser(username)
        .setPassword(password);
      case MYSQL -> MySQLConnectOptions.fromUri(jdbcUrl.substring("jdbc:".length()))
        .setUser(username)
        .setPassword(password);
    };
    PoolOptions poolOptions = new PoolOptions().setMaxSize(4);
    Pool client = Pool.pool(vertx, connectOptions, poolOptions);
    return Migx.create(vertx, client, locations);
  }
}
