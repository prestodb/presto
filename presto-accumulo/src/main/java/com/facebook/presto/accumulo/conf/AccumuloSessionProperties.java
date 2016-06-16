/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.accumulo.conf;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.longSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

/**
 * Class contains all session-based properties for the Accumulo connector.
 * Use SHOW SESSION to view all available properties in the Presto CLI.
 * <p>
 * Can set the property using:
 * <p>
 * SET SESSION &lt;property&gt; = &lt;value&gt;;
 */
public final class AccumuloSessionProperties
{
    private static final String OPTIMIZE_LOCALITY_ENABLED = "optimize_locality_enabled";
    private static final String OPTIMIZE_SPLIT_RANGES_ENABLED = "optimize_split_ranges_enabled";
    private static final String OPTIMIZE_INDEX_ENABLED = "optimize_index_enabled";
    private static final String INDEX_ROWS_PER_SPLIT = "index_rows_per_split";
    private static final String INDEX_THRESHOLD = "index_threshold";
    private static final String INDEX_LOWEST_CARDINALITY_THRESHOLD = "index_lowest_cardinality_threshold";
    private static final String INDEX_LOWEST_CARDINALITY_ROW_THRESHOLD = "index_lowest_cardinality_row_threshold";
    private static final String INDEX_METRICS_ENABLED = "index_metrics_enabled";
    private static final String SCAN_USERNAME = "scan_username";
    private static final String INDEX_SHORT_CIRCUIT_CARDINALITY_FETCH = "index_short_circuit_cardinality_fetch";
    private static final String INDEX_CARDINALITY_CACHE_POLLING_DURATION = "index_cardinality_cache_polling_duration";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public AccumuloSessionProperties()
    {
        PropertyMetadata<Boolean> s1 = booleanSessionProperty(
                OPTIMIZE_LOCALITY_ENABLED,
                "Set to true to enable data locality for non-indexed scans. Default true.", true,
                false);

        PropertyMetadata<Boolean> s2 = booleanSessionProperty(
                OPTIMIZE_SPLIT_RANGES_ENABLED,
                "Set to true to split non-indexed queries by tablet splits. Should generally be true.",
                true, false);

        PropertyMetadata<String> s3 = stringSessionProperty(
                SCAN_USERNAME,
                "User to impersonate when scanning the tables. This property trumps the scan_auths table property. Default is the user in the configuration file.", null, false);

        PropertyMetadata<Boolean> s4 = booleanSessionProperty(
                OPTIMIZE_INDEX_ENABLED,
                "Set to true to enable usage of the secondary index on query. Default true.",
                true,
                false);

        PropertyMetadata<Integer> s5 = integerSessionProperty(
                INDEX_ROWS_PER_SPLIT,
                "The number of Accumulo row IDs that are packed into a single Presto split. Default 10000",
                10000,
                false);

        PropertyMetadata<Double> s6 = doubleSessionProperty(
                INDEX_THRESHOLD,
                "The ratio between number of rows to be scanned based on the index over the total number of rows. If the ratio is below this threshold, the index will be used. Default .2",
                0.2,
                false);

        PropertyMetadata<Double> s7 = doubleSessionProperty(
                INDEX_LOWEST_CARDINALITY_THRESHOLD,
                "The threshold (as a percentage) where the column with the lowest cardinality will be used instead of computing an intersection of ranges in the secondary index. The minimum value of this value times the number of rows in the table and the row threshold will be used. Secondary index must be enabled. Default .01",
                0.01,
                false);

        PropertyMetadata<Long> s8 = longSessionProperty(
                INDEX_LOWEST_CARDINALITY_ROW_THRESHOLD,
                "The threshold (as number of rows) where the column with the lowest cardinality will be used instead of computing an intersection of ranges in the secondary index. The minimum value of this value and the percentage threshold will be used. Secondary index must be enabled. Default 500000",
                500_000L,
                false);

        PropertyMetadata<Boolean> s9 = booleanSessionProperty(
                INDEX_METRICS_ENABLED,
                "Set to true to enable usage of the metrics table to optimize usage of the index. Default true",
                true,
                false);

        PropertyMetadata<Boolean> s10 = booleanSessionProperty(
                INDEX_SHORT_CIRCUIT_CARDINALITY_FETCH,
                "Short circuit the retrieval of index metrics once any column is less than the lowest cardinality threshold. Default true",
                true,
                false);

        PropertyMetadata<String> s11 = new PropertyMetadata<>(
                INDEX_CARDINALITY_CACHE_POLLING_DURATION,
                "Sets the cardinality cache polling duration for short circuit retrieval of index metrics. Default 10ms",
                VARCHAR, String.class,
                "10ms",
                false,
                duration -> Duration.valueOf(duration.toString()).toString(),
                object -> object);

        sessionProperties = ImmutableList.of(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11);
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isOptimizeLocalityEnabled(ConnectorSession session)
    {
        return session.getProperty(OPTIMIZE_LOCALITY_ENABLED, Boolean.class);
    }

    public static boolean isOptimizeSplitRangesEnabled(ConnectorSession session)
    {
        return session.getProperty(OPTIMIZE_SPLIT_RANGES_ENABLED, Boolean.class);
    }

    public static boolean isOptimizeIndexEnabled(ConnectorSession session)
    {
        return session.getProperty(OPTIMIZE_INDEX_ENABLED, Boolean.class);
    }

    public static double getIndexThreshold(ConnectorSession session)
    {
        return session.getProperty(INDEX_THRESHOLD, Double.class);
    }

    public static int getNumIndexRowsPerSplit(ConnectorSession session)
    {
        return session.getProperty(INDEX_ROWS_PER_SPLIT, Integer.class);
    }

    /**
     * Gets the configured threshold (as a percentage) for using the column with the smallest
     * cardinality, instead of computing the index.
     * <br>
     * The connector typically computes an intersection of row IDs across all indexed columns,
     * but if the column with the smallest cardinality is significantly small, we can just use these
     * rows and let Presto filter out the rows that do not match the remaining predicates.
     * <br>
     * The minimum value of this value times the number of rows in the table and the row threshold will be used.
     *
     * @param session The current session
     * @return The index threshold, 0 - 1
     */
    public static double getIndexSmallCardThreshold(ConnectorSession session)
    {
        return session.getProperty(INDEX_LOWEST_CARDINALITY_THRESHOLD, Double.class);
    }

    /**
     * Gets the configured threshold (as number of rows) for using the column with the smallest
     * cardinality, instead of computing the index.
     * <br>
     * The connector typically computes an intersection of row IDs across all indexed columns,
     * but if the column with the lowest cardinality is significantly small, we can just use these
     * rows and let Presto filter out the rows that do not match the remaining predicates.
     * <br>
     * The minimum value of this value and the percentage threshold will be used.
     *
     * @param session The current session
     * @return The index threshold as number of rows
     */
    public static long getIndexSmallCardRowThreshold(ConnectorSession session)
    {
        return session.getProperty(INDEX_LOWEST_CARDINALITY_ROW_THRESHOLD, Long.class);
    }

    /**
     * Gets the polling interval for the completion service that fetches cardinalities from Accumulo
     * <br>
     * The LoadingCache is not ordered and, as a result, some cached results (or a result retrieved
     * from Accumulo in a short time) that have higher cardinalities are returned a few milliseconds
     * before a significantly lower result. This parametmer controls the poll duration, adding 'waves
     * of result retrieval from the LoadingCache. The results of any completed tasks are taken,
     * and the smallest cardinality, if below the threshold, is used while the other tasks complete.
     *
     * @param session The current session
     * @return The cardinality cache polling duration
     */
    public static Duration getIndexCardinalityCachePollingDuration(ConnectorSession session)
    {
        return Duration.valueOf(session.getProperty(INDEX_CARDINALITY_CACHE_POLLING_DURATION, String.class));
    }

    public static boolean isIndexMetricsEnabled(ConnectorSession session)
    {
        return session.getProperty(INDEX_METRICS_ENABLED, Boolean.class);
    }

    public static String getScanUsername(ConnectorSession session)
    {
        return session.getProperty(SCAN_USERNAME, String.class);
    }

    public static boolean isIndexShortCircuitEnabled(ConnectorSession session)
    {
        return session.getProperty(INDEX_SHORT_CIRCUIT_CARDINALITY_FETCH, Boolean.class);
    }
}
