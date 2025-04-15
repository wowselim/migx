package co.selim.migx.core;

import co.selim.migx.core.output.MigrationResult;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import org.flywaydb.core.api.output.MigrateResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.ResultSet;
import java.util.List;

@ExtendWith(VertxExtension.class)
public class MigrationTest extends IntegrationTest {

  private MigrateResult flywayResult;
  private MigrationResult migxResult;

  @BeforeEach
  void migrate(Vertx vertx) {
    flywayResult = getFlyway().migrate();
    flywayResult.migrations.forEach(mig -> {
      System.out.println("mig.category = " + mig.category);
      System.out.println("mig.description = " + mig.description);
      System.out.println("mig.executionTime = " + mig.executionTime);
      System.out.println("mig.filepath = " + mig.filepath);
      System.out.println("mig.type = " + mig.type);
      System.out.println("mig.version = " + mig.version);
      System.out.println("----------------------------------------");
    });
    migxResult = getMigx(vertx).migrate().toCompletionStage().toCompletableFuture().join();
    migxResult.migrations().forEach(mig -> {
      System.out.println("mig.category = " + mig.category());
      System.out.println("mig.description = " + mig.description());
      System.out.println("mig.executionTime = " + mig.executionTime());
      System.out.println("mig.filepath = " + mig.filepath());
      System.out.println("mig.type = " + mig.type());
      System.out.println("mig.version = " + mig.version());
      System.out.println("----------------------------------------");
    });
  }

  @Test
  void x() {
    List<SchemaHistoryEntry> flywaySchemaHistory = getSchemaHistory(Database.FLYWAY);
    System.out.println("flywaySchemaHistory = " + flywaySchemaHistory);
    List<SchemaHistoryEntry> migxSchemaHistory = getSchemaHistory(Database.MIGX);
    System.out.println("migxSchemaHistory = " + migxSchemaHistory);
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
