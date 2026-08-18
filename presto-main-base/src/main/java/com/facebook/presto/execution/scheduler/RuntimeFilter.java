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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * Polymorphic carrier for a single-column dynamic filter contribution produced by one
 * build-side partition.
 *
 * <p>Each implementation represents a filter over exactly one join column. The two
 * current representations are:
 * <ul>
 *   <li>{@link DomainRuntimeFilter} — a {@link TupleDomain} of discrete values or ranges,
 *       used for all current filter delivery.</li>
 *   <li>Future: {@code BloomRuntimeFilter} — a Bloom filter for high-cardinality keys;
 *       handled by {@link RuntimeFilterDeserializer} via {@code @kind: "bloom"}.</li>
 * </ul>
 *
 * <p>Wire form: a bare {@code TupleDomain} JSON object (no {@code @kind} field) decodes
 * to {@link DomainRuntimeFilter} for back-compat. An unknown {@code @kind} value causes
 * deserialization to fail fast.
 */
@JsonDeserialize(using = RuntimeFilterDeserializer.class)
public interface RuntimeFilter
{
    /** Union-merges this filter with {@code other}. Throws if representations are incompatible. */
    RuntimeFilter mergeWith(RuntimeFilter other);

    /**
     * Re-keys this filter's single column entry to {@code column} for use by the connector.
     *
     * @param column  the probe-side column name to use as the {@link TupleDomain} key
     * @param type    the probe column's {@link Type}; may be used by implementations
     *                that need type-specific conversion (e.g. coercion for Bloom filters)
     */
    TupleDomain<String> toTupleDomain(String column, Type type);

    boolean isAll();

    boolean isNone();

    long estimatedRetainedSizeInBytes();
}
