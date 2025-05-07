package co.selim.migx.core;

import co.selim.migx.core.impl.util.Pair;
import co.selim.migx.core.output.MigrationOutput;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import org.flywaydb.core.api.output.MigrateOutput;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static co.selim.migx.core.IntegrationTest.Database.FLYWAY;
import static co.selim.migx.core.IntegrationTest.Database.MIGX;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

@ExtendWith(VertxExtension.class)
public class MigrationTest extends IntegrationTest {

  private final Vertx vertx;

  public MigrationTest(Vertx vertx) {
    this.vertx = vertx;
  }

  private Pair<List<MigrateOutput>, List<MigrationOutput>> migrate(List<String> locations) {
    List<MigrateOutput> flywayOutput = getFlyway(locations.toArray(String[]::new)).migrate().migrations;
    try {
      List<MigrationOutput> migxOutput = getMigx(vertx, locations)
        .migrate()
        .toCompletionStage()
        .toCompletableFuture()
        .get();

      return new Pair<>(flywayOutput, migxOutput);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  @DisplayName("The migration history tables match")
  void migrationHistoriesMatch() {
    migrate(List.of("db/migration"));
    List<SchemaHistoryEntry> flywaySchemaHistory = getSchemaHistory(FLYWAY);
    List<SchemaHistoryEntry> migxSchemaHistory = getSchemaHistory(MIGX);
    assertIterableEquals(flywaySchemaHistory, migxSchemaHistory);
  }

  @Test
  @DisplayName("Migrations fail when table already exists")
  void migrationsFailWhenTableAlreadyExists() {
    Assertions.assertThrows(Throwable.class, () -> migrate(List.of("db/migration", "db/migration2")));
  }
}
