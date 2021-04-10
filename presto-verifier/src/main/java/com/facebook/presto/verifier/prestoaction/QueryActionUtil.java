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
package com.facebook.presto.verifier.prestoaction;

import com.facebook.presto.verifier.framework.QueryStage;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_BROADCAST_MEMORY;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_EXECUTION_TIME;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_MEMORY;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_MEMORY_PER_NODE;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_RUN_TIME;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_TOTAL_MEMORY;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_TOTAL_MEMORY_PER_NODE;
import static com.facebook.presto.verifier.framework.QueryStage.DETERMINISM_ANALYSIS_MAIN;

public class QueryActionUtil
{
    private QueryActionUtil()
    {
    }

    public static Map<String, String> mangleSessionProperties(
            Map<String, String> mainQuerySessionProperty,
            QueryStage queryStage,
            Duration queryTimeout,
            boolean removeMemoryRelatedSessionProperty)
    {
        // configure session properties
        Map<String, String> sessionProperties = queryStage.isMain() || queryStage == DETERMINISM_ANALYSIS_MAIN
                ? new HashMap<>(mainQuerySessionProperty)
                : new HashMap<>();

        // Add or override query max execution time to enforce the timeout.
        sessionProperties.put(QUERY_MAX_EXECUTION_TIME, queryTimeout.toString());

        // Remove query max run time to respect execution time limit.
        sessionProperties.remove(QUERY_MAX_RUN_TIME);

        if (removeMemoryRelatedSessionProperty) {
            // Allow verifier clusters to provide their own memory limits to reduce noise from
            // CBO making different decisions based on cluster size
            sessionProperties.remove(QUERY_MAX_BROADCAST_MEMORY);

            sessionProperties.remove(QUERY_MAX_MEMORY_PER_NODE);
            sessionProperties.remove(QUERY_MAX_TOTAL_MEMORY_PER_NODE);

            sessionProperties.remove(QUERY_MAX_MEMORY);
            sessionProperties.remove(QUERY_MAX_TOTAL_MEMORY);
        }

        return ImmutableMap.copyOf(sessionProperties);
    }
}
