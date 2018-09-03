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
package com.facebook.presto.elasticsearch.conf;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.longSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;

/**
 * Class contains all session-based properties for the Elasticsearch connector.
 * Use SHOW SESSION to view all available properties in the Presto CLI.
 * <p>
 * Can set the property using:
 * <p>
 * SET SESSION &lt;property&gt; = &lt;value&gt;;
 */
public final class ElasticsearchSessionProperties
{
    private static final String OPTIMIZE_LOCALITY_ENABLED = "optimize_locality_enabled";
    private static final String OPTIMIZE_SPLIT_SHARDS_ENABLED = "optimize_split_shards_enabled";
    private static final String INDEX_ROWS_PER_SPLIT = "index_rows_per_split";
    private static final String SCAN_USERNAME = "scan_username";
    private static final String SCROLL_SEARCH_TIMEOUT = "scroll_search_timeout";
    private static final String SCROLL_SEARCH_BATCH_SIZE = "scroll_search_batch_size";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public ElasticsearchSessionProperties()
    {
        PropertyMetadata<Boolean> s1 = booleanSessionProperty(
                OPTIMIZE_LOCALITY_ENABLED,
                "Set to true to enable data locality for non-indexed scans. Default true.", true,
                false);

        PropertyMetadata<Boolean> s2 = booleanSessionProperty(
                OPTIMIZE_SPLIT_SHARDS_ENABLED,
                "Set to true to split non-indexed queries by shards splits. Should generally be true.",
                true, false);

        PropertyMetadata<String> s3 = stringSessionProperty(
                SCAN_USERNAME,
                "User to impersonate when scanning the index. This property trumps the scan_auths table property. Default is the user in the configuration file.",
                null,
                false);

        PropertyMetadata<Integer> s4 = integerSessionProperty(
                INDEX_ROWS_PER_SPLIT,
                "The number of Elasticsearch IDs that are packed into a single Presto split. Default 10000",
                10000,
                false);

        PropertyMetadata<Long> s5 = longSessionProperty(
                SCROLL_SEARCH_TIMEOUT,
                "If set, will enable scrolling of the search request for the specified timeout. Default 1m",
                60_000L,
                false);

        PropertyMetadata<Integer> s6 = integerSessionProperty(
                SCROLL_SEARCH_BATCH_SIZE,
                "max of 100 hits will be returned for each scroll. Default 100",
                100,
                false);

        sessionProperties = ImmutableList.of(s1, s2, s3, s4, s5, s6);
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isOptimizeLocalityEnabled(ConnectorSession session)
    {
        return session.getProperty(OPTIMIZE_LOCALITY_ENABLED, Boolean.class);
    }

    public static boolean isOptimizeSplitShardsEnabled(ConnectorSession session)
    {
        return session.getProperty(OPTIMIZE_SPLIT_SHARDS_ENABLED, Boolean.class);
    }

    public static int getNumIndexRowsPerSplit(ConnectorSession session)
    {
        return session.getProperty(INDEX_ROWS_PER_SPLIT, Integer.class);
    }

    public static long getScrollSearchTimeout(ConnectorSession session)
    {
        return session.getProperty(SCROLL_SEARCH_TIMEOUT, Long.class);
    }

    public static int getScrollSearchBatchSize(ConnectorSession session)
    {
        return session.getProperty(SCROLL_SEARCH_BATCH_SIZE, Integer.class);
    }

    public static String getScanUsername(ConnectorSession session)
    {
        return session.getProperty(SCAN_USERNAME, String.class);
    }
}
