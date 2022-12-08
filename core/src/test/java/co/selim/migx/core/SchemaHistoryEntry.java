package co.selim.migx.core;

import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public record SchemaHistoryEntry(
  int installedRank,
  String version,
  String description,
  String type,
  String script,
  long checksum,
  String installedBy,
  LocalDateTime installedOn,
  long executionTime,
  boolean success
) {

  public static final class Mapper implements ThrowingFunction<ResultSet, List<SchemaHistoryEntry>> {
    @Override
    public List<SchemaHistoryEntry> apply(ResultSet resultSet) throws Throwable {
      List<SchemaHistoryEntry> schemaHistory = new ArrayList<>();
      while (resultSet.next()) {
        int installedRank = resultSet.getInt("installed_rank");
        String version = resultSet.getString("version");
        String description = resultSet.getString("description");
        String type = resultSet.getString("type");
        String script = resultSet.getString("script");
        long checksum = resultSet.getLong("checksum");
        String installedBy = resultSet.getString("installed_by");
        LocalDateTime installedOn = resultSet.getTimestamp("installed_on").toLocalDateTime();
        long executionTime = resultSet.getLong("execution_time");
        boolean success = resultSet.getBoolean("success");
        SchemaHistoryEntry entry = new SchemaHistoryEntry(
          installedRank,
          version,
          description,
          type,
          script,
          checksum,
          installedBy,
          installedOn,
          executionTime,
          success
        );
        schemaHistory.add(entry);
      }
      return schemaHistory;
    }
  }
}
