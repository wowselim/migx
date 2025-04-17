package co.selim.migx.core;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.ResultSet;
import java.util.List;

@ExtendWith(VertxExtension.class)
public class MigrationTest extends IntegrationTest {

  @BeforeEach
  void migrate(Vertx vertx) {
    try {
      getFlyway().migrate();
      getMigx(vertx).migrate().toCompletionStage().toCompletableFuture().join();
    } catch (Throwable t) {
      t.printStackTrace(System.err);
    }
  }

  @Test
  @DisplayName("The migration history tables match")
  void migrationHistoriesMatch() {
    List<SchemaHistoryEntry> flywaySchemaHistory = getSchemaHistory(Database.FLYWAY);
    List<SchemaHistoryEntry> migxSchemaHistory = getSchemaHistory(Database.MIGX);
    Assertions.assertEquals(flywaySchemaHistory, migxSchemaHistory);
  }

  private List<SchemaHistoryEntry> getSchemaHistory(Database database) {
    return withConnection(database, connection -> {
      String query = "select * from flyway_schema_history order by installed_rank";
      try (ResultSet resultSet = connection.createStatement().executeQuery(query)) {
        return SchemaHistoryEntry.MAPPER.apply(resultSet);
      }
    });
  }
}
