package co.selim.migx.core.impl.util;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import java.util.zip.CRC32;

public final class Checksums {
  private Checksums() {
  }

  public static int calculateChecksum(String s) {
    CRC32 crc32 = new CRC32();
    try (Stream<String> lines = s.lines()) {
      lines.forEach(line -> crc32.update(line.getBytes(StandardCharsets.UTF_8)));
    }
    return (int) crc32.getValue();
  }
}
