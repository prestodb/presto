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
import com.google.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;

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
    private static final String INDEX_METRICS_ENABLED = "index_metrics_enabled";
    private static final String SCAN_USERNAME = "scan_username";

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
                "The threshold where the column with the lowest cardinality will be used instead of computing an intersection of ranges in the secondary index. Secondary index must be enabled. Default .01",
                0.01,
                false);

        PropertyMetadata<Boolean> s8 = booleanSessionProperty(
                INDEX_METRICS_ENABLED,
                "Set to true to enable usage of the metrics table to optimize usage of the index. Default true",
                true,
                false);

        sessionProperties = ImmutableList.of(s1, s2, s3, s4, s5, s6, s7, s8);
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

    public static double getIndexSmallCardThreshold(ConnectorSession session)
    {
        return session.getProperty(INDEX_LOWEST_CARDINALITY_THRESHOLD, Double.class);
    }

    public static boolean isIndexMetricsEnabled(ConnectorSession session)
    {
        return session.getProperty(INDEX_METRICS_ENABLED, Boolean.class);
    }

    public static String getScanUsername(ConnectorSession session)
    {
        return session.getProperty(SCAN_USERNAME, String.class);
    }
}
