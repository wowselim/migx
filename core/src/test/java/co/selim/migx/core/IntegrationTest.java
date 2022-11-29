package co.selim.migx.core;

import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;

@Testcontainers
public abstract class IntegrationTest {

  private static final String PG_IMAGE_NAME = "postgres:14.3";

  @Container
  protected final PostgreSQLContainer<?> flywayContainer = new PostgreSQLContainer<>(PG_IMAGE_NAME);

  @Container
  protected final PostgreSQLContainer<?> migxContainer = new PostgreSQLContainer<>(PG_IMAGE_NAME);

  protected Jdbi flywayJdbi;

  protected Jdbi migxJdbi;

  protected Flyway getFlyway() {
    String jdbcUrl = flywayContainer.getJdbcUrl();
    String username = flywayContainer.getUsername();
    String password = flywayContainer.getPassword();

    flywayJdbi = Jdbi.create(jdbcUrl, username, password)
      .registerRowMapper(new SchemaHistoryEntry.Mapper());

    return new FluentConfiguration()
      .dataSource(jdbcUrl, username, password)
      .load();
  }

  protected Migx getMigx(Vertx vertx) {
    String jdbcUrl = migxContainer.getJdbcUrl();
    String username = migxContainer.getUsername();
    String password = migxContainer.getPassword();

    migxJdbi = Jdbi.create(jdbcUrl, username, password)
      .registerRowMapper(new SchemaHistoryEntry.Mapper());

    PgConnectOptions connectOptions = PgConnectOptions.fromUri(jdbcUrl.substring("jdbc:".length()))
      .setUser(username)
      .setPassword(password);
    PoolOptions poolOptions = new PoolOptions().setMaxSize(4);
    SqlClient client = PgPool.client(vertx, connectOptions, poolOptions);
    return Migx.create(vertx, client);
  }
}
