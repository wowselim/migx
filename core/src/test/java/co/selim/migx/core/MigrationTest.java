package co.selim.migx.core;

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
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
public class MigrationTest extends IntegrationTest {

  private final Vertx vertx;

  public MigrationTest(Vertx vertx) {
    this.vertx = vertx;
  }

  private List<MigrateOutput> migrateFlyway(List<String> locations) {
    return getFlyway(locations.toArray(String[]::new)).migrate().migrations;
  }

  private List<MigrationOutput> migrateMigx(List<String> locations) {
    try {
      return getMigx(vertx, locations)
        .migrate()
        .toCompletionStage()
        .toCompletableFuture()
        .get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  @DisplayName("The migration history tables match")
  void migrationHistoriesMatch() {
    List<String> migrationPaths = List.of("db/migration");
    migrateFlyway(migrationPaths);
    migrateMigx(migrationPaths);
    List<SchemaHistoryEntry> flywaySchemaHistory = getSchemaHistory(FLYWAY);
    List<SchemaHistoryEntry> migxSchemaHistory = getSchemaHistory(MIGX);
    assertIterableEquals(flywaySchemaHistory, migxSchemaHistory);
  }

  @Test
  @DisplayName("Duplicate files are ignored")
  void duplicateFilesAreIgnored() {
    List<String> migrationPaths = List.of("db/migration", "db/migration");
    migrateFlyway(migrationPaths);
    migrateMigx(migrationPaths);
  }

  @Test
  @DisplayName("Migrations fail if duplicate versions exist")
  void migrationsFailIfDuplicateVersionsExist() {
    List<String> migrationPaths = List.of("db/duplicate-versions");
    Throwable flywayError = Assertions.assertThrows(Throwable.class, () -> {
      migrateFlyway(migrationPaths);
    });
    Throwable migxError = Assertions.assertThrows(Throwable.class, () -> {
      migrateMigx(migrationPaths);
    });

    String message = "Found more than one migration with version 1.2";
    assertTrue(flywayError.getMessage().contains(message));
    assertTrue(migxError.getMessage().contains(message));
  }

  @Test
  @DisplayName("Listing a path twice only runs the migrations once")
  void listingPathTwiceRunsMigrationsOnlyOnce() {
    List<String> migrationPaths = List.of("db/migration", "db/migration");
    List<MigrateOutput> flywayMigrations = migrateFlyway(migrationPaths);
    List<MigrationOutput> migxMigrations = migrateMigx(migrationPaths);

    assertEquals(2, flywayMigrations.size());
    assertEquals(flywayMigrations.size(), migxMigrations.size());
  }
}
