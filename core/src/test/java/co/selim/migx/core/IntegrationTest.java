package co.selim.migx.core;

import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.function.Function;

@Testcontainers
public abstract class IntegrationTest {

  private static final String PG_IMAGE_NAME = "postgres:14.3";

  @Container
  protected final PostgreSQLContainer<?> flywayContainer = new PostgreSQLContainer<>(PG_IMAGE_NAME);

  @Container
  protected final PostgreSQLContainer<?> migxContainer = new PostgreSQLContainer<>(PG_IMAGE_NAME);

  protected enum Database {
    FLYWAY, MIGX
  }

  protected Flyway getFlyway() {
    String jdbcUrl = flywayContainer.getJdbcUrl();
    String username = flywayContainer.getUsername();
    String password = flywayContainer.getPassword();

    return new FluentConfiguration()
      .dataSource(jdbcUrl, username, password)
      .load();
  }

  protected <T> T withConnection(Database database, ThrowingFunction<Connection, T> function) {
    PostgreSQLContainer<?> container = switch (database) {
      case MIGX -> migxContainer;
      case FLYWAY -> flywayContainer;
    };
    try (Connection connection = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(), container.getPassword())) {
      return function.apply(connection);
    } catch (Throwable e) {
      Assertions.fail(e);
      throw new RuntimeException(e);
    }
  }

  protected Migx getMigx(Vertx vertx) {
    String jdbcUrl = migxContainer.getJdbcUrl();
    String username = migxContainer.getUsername();
    String password = migxContainer.getPassword();

    PgConnectOptions connectOptions = PgConnectOptions.fromUri(jdbcUrl.substring("jdbc:".length()))
      .setUser(username)
      .setPassword(password);
    PoolOptions poolOptions = new PoolOptions().setMaxSize(4);
    SqlClient client = PgPool.client(vertx, connectOptions, poolOptions);
    return Migx.create(vertx, client);
  }
}
