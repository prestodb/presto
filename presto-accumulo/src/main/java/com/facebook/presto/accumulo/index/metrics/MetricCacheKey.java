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
package com.facebook.presto.accumulo.index.metrics;

import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Key used to retrieve metrics from {@link MetricsStorage}, used by {@link com.facebook.presto.accumulo.index.ColumnCardinalityCache}
 * to locally cache entries retrieved from the external source.
 */
public class MetricCacheKey
{
    public final MetricsStorage storage;
    public final String schema;
    public final String table;
    public final String family;
    public final Optional<String> qualifier;
    public final boolean truncateTimestamps;
    public final Authorizations auths;
    public final Range range;

    /**
     * Creates a new instance of a MetricCacheKey
     *
     * @param storage Metrics storage instance
     * @param schema Schema name
     * @param table Table name
     * @param family Column family
     * @param qualifier Column qualifier
     * @param truncateTimestamps True if timestamps are truncated AND this is a Timestamp type, false otherwise
     * @param auths Authorizations for this metric
     * @param range Range representing the cell value
     */
    public MetricCacheKey(MetricsStorage storage, String schema, String table, String family, String qualifier, boolean truncateTimestamps, Authorizations auths, Range range)
    {
        this.storage = storage;
        this.schema = schema;
        this.table = table;
        this.family = family;
        this.qualifier = Optional.ofNullable(qualifier);
        this.truncateTimestamps = truncateTimestamps;
        this.auths = auths;
        this.range = range;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(storage, schema, table, family, qualifier, truncateTimestamps, auths, range);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        MetricCacheKey other = (MetricCacheKey) obj;
        return Objects.equals(this.range, other.range)
                && Objects.equals(this.schema, other.schema)
                && Objects.equals(this.table, other.table)
                && Objects.equals(this.family, other.family)
                && Objects.equals(this.truncateTimestamps, other.truncateTimestamps)
                && Objects.equals(this.auths, other.auths)
                && Objects.equals(this.qualifier, other.qualifier)
                && Objects.equals(this.storage, other.storage);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("storage", storage.getClass())
                .add("schema", schema)
                .add("table", table)
                .add("family", family)
                .add("qualifier", qualifier)
                .add("truncateTimestamps", truncateTimestamps)
                .add("auths", auths)
                .add("range", range)
                .toString();
    }
}
