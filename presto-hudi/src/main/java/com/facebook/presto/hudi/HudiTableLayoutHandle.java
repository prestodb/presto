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

package com.facebook.presto.hudi;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.org.apache.avro.Schema;
import org.apache.hudi.util.Lazy;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HudiTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private static final Logger log = Logger.get(HudiTableHandle.class);
    private final HudiTableHandle tableHandle;
    private final List<HudiColumnHandle> dataColumns;
    private final List<HudiColumnHandle> partitionColumns;
    private final Map<String, String> tableParameters;
    private final TupleDomain<HudiColumnHandle> regularPredicates;
    private final TupleDomain<HudiColumnHandle> partitionPredicates;
    private final Optional<Lazy<Schema>> hudiTableSchemaOpt;

    @JsonCreator
    public HudiTableLayoutHandle(
            @JsonProperty("table") HudiTableHandle tableHandle,
            @JsonProperty("dataColumns") List<HudiColumnHandle> dataColumns,
            @JsonProperty("partitionColumns") List<HudiColumnHandle> partitionColumns,
            @JsonProperty("tableParameters") Map<String, String> tableParameters,
            @JsonProperty("regularPredicates") TupleDomain<HudiColumnHandle> regularPredicates,
            @JsonProperty("partitionPredicates") TupleDomain<HudiColumnHandle> partitionPredicates,
            @JsonProperty("tableSchemaStr") String tableSchemaStr)
    {
        this(tableHandle, dataColumns, partitionColumns, tableParameters, regularPredicates, partitionPredicates, buildTableSchema(tableSchemaStr));
    }

    public HudiTableLayoutHandle(
            HudiTableHandle tableHandle,
            List<HudiColumnHandle> dataColumns,
            List<HudiColumnHandle> partitionColumns,
            Map<String, String> tableParameters,
            TupleDomain<HudiColumnHandle> partitionPredicates,
            TupleDomain<HudiColumnHandle> regularPredicates,
            Optional<Lazy<Schema>> hudiTableSchemaOpt)
    {
        this.tableHandle = requireNonNull(tableHandle, "table is null");
        this.dataColumns = requireNonNull(dataColumns, "dataColumns is null");
        this.partitionColumns = requireNonNull(partitionColumns, "partitionColumns is null");
        this.tableParameters = requireNonNull(tableParameters, "tableParameters is null");
        this.regularPredicates = requireNonNull(regularPredicates, "regularPredicates is null");
        this.partitionPredicates = requireNonNull(partitionPredicates, "partitionPredicates is null");
        this.hudiTableSchemaOpt = requireNonNull(hudiTableSchemaOpt, "hudiTableSchemaOpt is null");
    }

    /**
     * Builds a lazily-parsed Avro schema from the given schema string.
     * <p>
     * Returns {@code Optional.empty()} if the input string is null/empty
     * or if parsing the schema fails.
     */
    private static Optional<Lazy<Schema>> buildTableSchema(String tableSchemaStr)
    {
        if (StringUtils.isNullOrEmpty(tableSchemaStr)) {
            return Optional.empty();
        }

        try {
            Lazy<Schema> lazySchema = Lazy.lazily(() -> new Schema.Parser().parse(tableSchemaStr));
            return Optional.of(lazySchema);
        }
        catch (Exception e) {
            log.warn(e, "Failed to parse table schema: %s", tableSchemaStr);
            return Optional.empty();
        }
    }

    @JsonProperty
    public HudiTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public List<HudiColumnHandle> getDataColumns()
    {
        return dataColumns;
    }

    @JsonProperty
    public List<HudiColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    @JsonProperty
    public Map<String, String> getTableParameters()
    {
        return tableParameters;
    }

    @JsonProperty
    public TupleDomain<HudiColumnHandle> getRegularPredicates()
    {
        return regularPredicates;
    }

    @JsonProperty
    public TupleDomain<HudiColumnHandle> getPartitionPredicates()
    {
        return partitionPredicates;
    }

    @JsonProperty
    public String getTableSchemaStr()
    {
        return hudiTableSchemaOpt
                .map(Lazy::get)
                .map(Schema::toString)
                .orElse("");
    }

    @JsonIgnore
    public Schema getTableSchema()
    {
        return hudiTableSchemaOpt.map(Lazy::get).orElse(null);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HudiTableLayoutHandle that = (HudiTableLayoutHandle) o;
        return Objects.equals(tableHandle, that.tableHandle) &&
                Objects.equals(regularPredicates, that.regularPredicates) &&
                Objects.equals(partitionPredicates, that.partitionPredicates);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableHandle, regularPredicates, partitionPredicates);
    }

    @Override
    public String toString()
    {
        return tableHandle.toString();
    }
}
