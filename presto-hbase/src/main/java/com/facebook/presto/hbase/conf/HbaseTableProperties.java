package com.facebook.presto.hbase.conf;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

/**
 * Class contains all table properties for the Hbase connector. Used when creating a table:
 * <p>
 * CREATE TABLE foo (a VARCHAR, b INT) WITH (column_mapping = 'b:md:b');
 */
public final class HbaseTableProperties {
  public static final String COLUMN_MAPPING = "column_mapping";
  public static final String INDEX_COLUMNS = "index_columns";
  public static final String LOCALITY_GROUPS = "locality_groups";
  public static final String ROW_ID = "row_id";
  public static final String SCAN_AUTHS = "scan_auths";
  private static final Splitter COLON_SPLITTER = Splitter.on(':').trimResults();
  private static final Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();
  private static final Splitter PIPE_SPLITTER = Splitter.on('|').omitEmptyStrings().trimResults();

  private final List<PropertyMetadata<?>> tableProperties;

  public HbaseTableProperties() {
    PropertyMetadata<String> s1 = stringProperty(COLUMN_MAPPING,
        "Comma-delimited list of column metadata: col_name:col_family:col_qualifier,[...]. Required for external tables. Not setting this property results in auto-generated column names.",
        null, false);

    PropertyMetadata<String> s2 = stringProperty(INDEX_COLUMNS,
        "A comma-delimited list of Presto columns that are indexed in this table's corresponding index table. Default is no indexed columns.",
        "", false);

    PropertyMetadata<String> s3 = stringProperty(LOCALITY_GROUPS,
        "List of locality groups to set on the Hbase table. Only valid on internal tables. String format is locality group name, colon, comma delimited list of Presto column names in the group. Groups are delimited by pipes. Example: group1:colA,colB,colC|group2:colD,colE,colF|etc.... Default is no locality groups.",
        null, false);

    PropertyMetadata<String> s4 = stringProperty(ROW_ID,
        "Presto column name that maps to the Hbase row ID. Default is the first column.", null,
        false);

    PropertyMetadata<String> s5 = stringProperty(SCAN_AUTHS,
        "Scan-time authorizations set on the batch scanner. Default is all scan authorizations for the user",
        null, false);

    tableProperties = ImmutableList.of(s1, s2, s3, s4, s5);
  }

  public List<PropertyMetadata<?>> getTableProperties() {
    return tableProperties;
  }

  /**
   * Gets the value of the column_mapping property, or Optional.empty() if not set.
   * <p>
   * Parses the value into a map of Presto column name to a pair of strings, the Hbase column family
   * and qualifier.
   *
   * @param tableProperties The map of table properties
   * @return The column mapping, presto name to (Hbase column family, qualifier)
   */
  public static Optional<Map<String, Pair<String, String>>> getColumnMapping(
      Map<String, Object> tableProperties) {
    requireNonNull(tableProperties);

    String strMapping = (String) tableProperties.get(COLUMN_MAPPING);
    if (strMapping == null) {
      return Optional.empty();
    }

    // Parse out the column mapping
    // This is a comma-delimited list of "(presto, column:Hbase, fam:Hbase qualifier)" triplets
    ImmutableMap.Builder<String, Pair<String, String>> mapping = ImmutableMap.builder();
    for (String m : COMMA_SPLITTER.split(strMapping)) {
      String[] tokens = Iterables.toArray(COLON_SPLITTER.split(m), String.class);
      checkState(tokens.length == 3,
          format("Mapping of %s contains %d tokens instead of 3", m, tokens.length));
      mapping.put(tokens[0], Pair.of(tokens[1], tokens[2]));
    }

    return Optional.of(mapping.build());
  }

  public static Optional<List<String>> getIndexColumns(Map<String, Object> tableProperties) {
    requireNonNull(tableProperties);

    String indexColumns = (String) tableProperties.get(INDEX_COLUMNS);
    if (indexColumns == null) {
      return Optional.empty();
    }

    return Optional.of(Arrays.asList(StringUtils.split(indexColumns, ',')));
  }

  /**
   * Gets the configured locality groups for the table, or Optional.empty() if not set.
   * <p>
   * All strings are lowercase.
   *
   * @param tableProperties The map of table properties
   * @return Optional map of locality groups
   */
  public static Optional<Map<String, Set<String>>> getLocalityGroups(
      Map<String, Object> tableProperties) {
    requireNonNull(tableProperties);

    String groupStr = (String) tableProperties.get(LOCALITY_GROUPS);
    if (groupStr == null) {
      return Optional.empty();
    }

    ImmutableMap.Builder<String, Set<String>> groups = ImmutableMap.builder();

    // Split all configured locality groups
    for (String group : PIPE_SPLITTER.split(groupStr)) {
      String[] locGroups = Iterables.toArray(COLON_SPLITTER.split(group), String.class);

      if (locGroups.length != 2) {
        throw new PrestoException(INVALID_TABLE_PROPERTY,
            "Locality groups string is malformed. See documentation for proper format.");
      }

      String grpName = locGroups[0];
      ImmutableSet.Builder<String> colSet = ImmutableSet.builder();

      for (String f : COMMA_SPLITTER.split(locGroups[1])) {
        colSet.add(f.toLowerCase(Locale.ENGLISH));
      }

      groups.put(grpName.toLowerCase(Locale.ENGLISH), colSet.build());
    }

    return Optional.of(groups.build());
  }

  public static Optional<String> getRowId(Map<String, Object> tableProperties) {
    requireNonNull(tableProperties);

    String rowId = (String) tableProperties.get(ROW_ID);
    return Optional.ofNullable(rowId);
  }

  public static Optional<String> getScanAuthorizations(Map<String, Object> tableProperties) {
    requireNonNull(tableProperties);

    String scanAuths = (String) tableProperties.get(SCAN_AUTHS);
    return Optional.ofNullable(scanAuths);
  }

}
