package co.selim.migx.core;

import co.selim.migx.core.output.MigrationOutput;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import org.flywaydb.core.api.output.MigrateOutput;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;

import static co.selim.migx.core.IntegrationTest.MigrationTool.FLYWAY;
import static co.selim.migx.core.IntegrationTest.MigrationTool.MIGX;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
public class MigrationTest extends IntegrationTest {

  private final Vertx vertx;

  public MigrationTest(Vertx vertx) {
    this.vertx = vertx;
  }

  private List<MigrateOutput> migrateFlyway(Database db, List<String> locations) {
    return getFlyway(db, locations.toArray(String[]::new)).migrate().migrations;
  }

  private List<MigrationOutput> migrateMigx(Database db, List<String> locations) {
    try {
      return getMigx(vertx, db, locations)
        .migrate()
        .toCompletionStage()
        .toCompletableFuture()
        .get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @ParameterizedTest
  @EnumSource(Database.class)
  @DisplayName("The migration history tables match")
  void migrationHistoriesMatch(Database db) {
    List<String> migrationPaths = List.of("db/migration");
    migrateFlyway(db, migrationPaths);
    migrateMigx(db, migrationPaths);
    List<SchemaHistoryEntry> flywaySchemaHistory = getSchemaHistory(db, FLYWAY);
    List<SchemaHistoryEntry> migxSchemaHistory = getSchemaHistory(db, MIGX);
    assertIterableEquals(flywaySchemaHistory, migxSchemaHistory);
  }

  @ParameterizedTest
  @EnumSource(Database.class)
  @DisplayName("Duplicate files are ignored")
  void duplicateFilesAreIgnored(Database db) {
    List<String> migrationPaths = List.of("db/migration", "db/migration");
    migrateFlyway(db, migrationPaths);
    migrateMigx(db, migrationPaths);
  }

  @ParameterizedTest
  @EnumSource(Database.class)
  @DisplayName("Migrations fail if duplicate versions exist")
  void migrationsFailIfDuplicateVersionsExist(Database db) {
    List<String> migrationPaths = List.of("db/duplicate-versions");
    Throwable flywayError = Assertions.assertThrows(Throwable.class, () -> {
      migrateFlyway(db, migrationPaths);
    });
    Throwable migxError = Assertions.assertThrows(Throwable.class, () -> {
      migrateMigx(db, migrationPaths);
    });

    String message = "Found more than one migration with version 1.2";
    assertTrue(flywayError.getMessage().contains(message));
    assertTrue(migxError.getMessage().contains(message));
  }

  @ParameterizedTest
  @EnumSource(Database.class)
  @DisplayName("Listing a path twice only runs the migrations once")
  void listingPathTwiceRunsMigrationsOnlyOnce(Database db) {
    List<String> migrationPaths = List.of("db/migration", "db/migration");
    List<MigrateOutput> flywayMigrations = migrateFlyway(db, migrationPaths);
    List<MigrationOutput> migxMigrations = migrateMigx(db, migrationPaths);

    assertEquals(2, flywayMigrations.size());
    assertEquals(flywayMigrations.size(), migxMigrations.size());
  }

  @ParameterizedTest
  @EnumSource(Database.class)
  @DisplayName("Migrations can be run twice with no effect")
  void migrationsCanBeRunTwiceWithNoEffect(Database db) {
    List<String> migrationPaths = List.of("db/migration");
    List<MigrateOutput> flywayMigrationsFirstRun = migrateFlyway(db, migrationPaths);
    List<MigrationOutput> migxMigrationsFirstRun = migrateMigx(db, migrationPaths);
    List<MigrateOutput> flywayMigrationsSecondRun = migrateFlyway(db, migrationPaths);
    List<MigrationOutput> migxMigrationsSecondRun = migrateMigx(db, migrationPaths);

    assertEquals(2, flywayMigrationsFirstRun.size());
    assertEquals(2, migxMigrationsFirstRun.size());
    assertEquals(0, flywayMigrationsSecondRun.size());
    assertEquals(0, migxMigrationsSecondRun.size());
  }
}
