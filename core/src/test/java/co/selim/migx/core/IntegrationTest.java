package co.selim.migx.core;

import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.provider.Arguments;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

@Testcontainers
public abstract class IntegrationTest {

  private static JdbcDatabaseContainer<?> pgContainer() {
    return new PostgreSQLContainer<>("postgres:17.5-alpine");
  }

  private static JdbcDatabaseContainer<?> mySqlContainer() {
    return new MySQLContainer<>("mysql:8.0.36");
  }

  protected static Stream<Arguments> dbArguments() {
    return Stream.of(
      Arguments.of(pgContainer(), pgContainer()),
      Arguments.of(mySqlContainer(), mySqlContainer())
    );
  }

  protected Flyway getFlyway(JdbcDatabaseContainer<?> container, String... locations) {
    String jdbcUrl = container.getJdbcUrl();
    String username = container.getUsername();
    String password = container.getPassword();

    return new FluentConfiguration()
      .dataSource(jdbcUrl, username, password)
      .locations(locations)
      .load();
  }

  protected List<SchemaHistoryEntry> getSchemaHistory(JdbcDatabaseContainer<?> container) {
    return withConnection(container, connection -> {
      String query = "select * from flyway_schema_history order by installed_rank";
      try (ResultSet resultSet = connection.createStatement().executeQuery(query)) {
        return SchemaHistoryEntry.MAPPER.apply(resultSet);
      }
    });
  }

  protected void withContainers(JdbcDatabaseContainer<?> containerA,
                                JdbcDatabaseContainer<?> containerB,
                                Runnable runnable) {
    CompletableFuture.allOf(
      CompletableFuture.runAsync(containerA::start),
      CompletableFuture.runAsync(containerB::start)
    ).join();

    try {
      runnable.run();
    } finally {
      CompletableFuture.allOf(
        CompletableFuture.runAsync(containerA::stop),
        CompletableFuture.runAsync(containerB::stop)
      ).join();
    }
  }

  private <T> T withConnection(JdbcDatabaseContainer<?> container, ThrowingFunction<Connection, T> function) {
    try (Connection connection = container.createConnection("")) {
      return function.apply(connection);
    } catch (Throwable e) {
      Assertions.fail(e);
      throw new RuntimeException(e);
    }
  }

  protected Migx getMigx(Vertx vertx, JdbcDatabaseContainer<?> container, List<String> locations) {
    String jdbcUrl = container.getJdbcUrl();
    String username = container.getUsername();
    String password = container.getPassword();

    SqlConnectOptions connectOptions = SqlConnectOptions.fromUri(jdbcUrl.substring("jdbc:".length()))
      .setUser(username)
      .setPassword(password);
    PoolOptions poolOptions = new PoolOptions().setMaxSize(4);
    Pool client = Pool.pool(vertx, connectOptions, poolOptions);

    return Migx.create(vertx, client, locations);
  }
}
