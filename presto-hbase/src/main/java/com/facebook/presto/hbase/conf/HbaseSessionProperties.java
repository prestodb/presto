package com.facebook.presto.hbase.conf;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;

/**
 * Class contains all session-based properties for the Hbase connector. Use SHOW SESSION to view all
 * available properties in the Presto CLI.
 * <p>
 * Can set the property using:
 * <p>
 * SET SESSION &lt;property&gt; = &lt;value&gt;;
 */
public final class HbaseSessionProperties {
  private static final String OPTIMIZE_LOCALITY_ENABLED = "optimize_locality_enabled";
  private static final String OPTIMIZE_SPLIT_RANGES_ENABLED = "optimize_split_ranges_enabled";

  private static final String SCAN_USERNAME = "scan_username";

  private static final String SCAN_BATCH_SIZE = "scan_batch_size";
  private static final String SCAN_CACHING = "scan_caching_number";
  private static final String SCAN_MAX_RESULT_SIZE = "scan_max_result_size";
  private static final String SCAN_MAX_VERSIONS = "scan_max_versions";

  private final List<PropertyMetadata<?>> sessionProperties;

  @Inject
  public HbaseSessionProperties() {
    PropertyMetadata<Boolean> s1 = booleanProperty(OPTIMIZE_LOCALITY_ENABLED,
        "Set to true to enable data locality for non-indexed scans. Default true.", true, false);

    PropertyMetadata<Boolean> s2 = booleanProperty(OPTIMIZE_SPLIT_RANGES_ENABLED,
        "Set to true to split non-indexed queries by tablet splits. Should generally be true.",
        true, false);

    PropertyMetadata<String> s3 = stringProperty(SCAN_USERNAME,
        "User to impersonate when scanning the tables. This property trumps the scan_auths table property. Default is the user in the configuration file.",
        null, false);

    PropertyMetadata<Integer> s4 = integerProperty(SCAN_BATCH_SIZE,
        "Set the maximum number of values to return for each call to next(). Default 100.", 100,
        false);

    PropertyMetadata<Integer> s5 = integerProperty(SCAN_CACHING,
        "Set the number of rows for caching that will be passed to scanners. Default 50.", 50,
        false);

    PropertyMetadata<Integer> s6 = integerProperty(SCAN_MAX_RESULT_SIZE,
        "Set the maximum result size. The default is -1; this means that no specific. The maximum result size in bytes . Default -1.",
        -1, false);

    PropertyMetadata<Integer> s7 = integerProperty(SCAN_MAX_VERSIONS,
        "Get up to the specified number of versions of each column. Default 1.", 1, false);

    sessionProperties = ImmutableList.of(s1, s2, s3, s4, s5, s6, s7);
  }

  public List<PropertyMetadata<?>> getSessionProperties() {
    return sessionProperties;
  }

  public static boolean isOptimizeLocalityEnabled(ConnectorSession session) {
    return session.getProperty(OPTIMIZE_LOCALITY_ENABLED, Boolean.class);
  }

  public static boolean isOptimizeSplitRangesEnabled(ConnectorSession session) {
    return session.getProperty(OPTIMIZE_SPLIT_RANGES_ENABLED, Boolean.class);
  }

  public static String getScanUsername(ConnectorSession session) {
    return session.getProperty(SCAN_USERNAME, String.class);
  }

  public static int getScanBatchSize(ConnectorSession session) {
    return session.getProperty(SCAN_BATCH_SIZE, Integer.class);
  }

  public static int getScanBatchCaching(ConnectorSession session) {
    return session.getProperty(SCAN_CACHING, Integer.class);
  }

  public static int getScanMaxResultSize(ConnectorSession session) {
    return session.getProperty(SCAN_MAX_RESULT_SIZE, Integer.class);
  }

  public static int getScanMaxVersions(ConnectorSession session) {
    return session.getProperty(SCAN_MAX_VERSIONS, Integer.class);
  }
}
