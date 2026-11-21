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
package com.facebook.presto.lance;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;

public final class LanceSessionProperties
{
    public static final String FILTER_PUSHDOWN_ENABLED = "filter_pushdown_enabled";
    public static final String VECTOR_SEARCH_PUSHDOWN_ENABLED = "vector_search_pushdown_enabled";
    public static final String VECTOR_SEARCH_NPROBES = "vector_search_nprobes";
    public static final String VECTOR_SEARCH_EF = "vector_search_ef";
    public static final String VECTOR_SEARCH_REFINE_FACTOR = "vector_search_refine_factor";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public LanceSessionProperties()
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(booleanProperty(
                        FILTER_PUSHDOWN_ENABLED,
                        "Enable SQL predicate pushdown to the Lance scanner",
                        true,
                        false))
                .add(booleanProperty(
                        VECTOR_SEARCH_PUSHDOWN_ENABLED,
                        "Rewrite ORDER BY <distance-fn> ... LIMIT into a Lance ANN index search",
                        true,
                        false))
                .add(integerProperty(
                        VECTOR_SEARCH_NPROBES,
                        "Number of IVF partitions to probe per ANN search (0 = Lance default)",
                        0,
                        false))
                .add(integerProperty(
                        VECTOR_SEARCH_EF,
                        "HNSW ef search parameter for ANN search (0 = Lance default)",
                        0,
                        false))
                .add(integerProperty(
                        VECTOR_SEARCH_REFINE_FACTOR,
                        "Refine factor for ANN search re-ranking (0 = Lance default)",
                        0,
                        false))
                .build();
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isFilterPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(FILTER_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isVectorSearchPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(VECTOR_SEARCH_PUSHDOWN_ENABLED, Boolean.class);
    }

    /** A positive value, or empty when the property is 0 (use Lance's default). */
    public static Optional<Integer> getVectorSearchNprobes(ConnectorSession session)
    {
        return positiveOrEmpty(session.getProperty(VECTOR_SEARCH_NPROBES, Integer.class));
    }

    public static Optional<Integer> getVectorSearchEf(ConnectorSession session)
    {
        return positiveOrEmpty(session.getProperty(VECTOR_SEARCH_EF, Integer.class));
    }

    public static Optional<Integer> getVectorSearchRefineFactor(ConnectorSession session)
    {
        return positiveOrEmpty(session.getProperty(VECTOR_SEARCH_REFINE_FACTOR, Integer.class));
    }

    private static Optional<Integer> positiveOrEmpty(Integer value)
    {
        return (value != null && value > 0) ? Optional.of(value) : Optional.empty();
    }
}
