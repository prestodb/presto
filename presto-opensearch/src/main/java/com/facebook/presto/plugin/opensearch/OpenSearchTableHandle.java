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
package com.facebook.presto.plugin.opensearch;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Table handle for OpenSearch connector.
 * Represents an OpenSearch index as a Presto table.
 * Can optionally represent a k-NN vector search query.
 */
public class OpenSearchTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final String indexName;
    private final Optional<String> vectorField;
    private final Optional<float[]> queryVector;
    private final Optional<Integer> k;
    private final Optional<String> spaceType;
    private final Optional<Integer> efSearch;

    @JsonCreator
    public OpenSearchTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("indexName") String indexName,
            @JsonProperty("vectorField") Optional<String> vectorField,
            @JsonProperty("queryVector") Optional<float[]> queryVector,
            @JsonProperty("k") Optional<Integer> k,
            @JsonProperty("spaceType") Optional<String> spaceType,
            @JsonProperty("efSearch") Optional<Integer> efSearch)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.indexName = requireNonNull(indexName, "indexName is null");
        this.vectorField = requireNonNull(vectorField, "vectorField is null");
        this.queryVector = requireNonNull(queryVector, "queryVector is null");
        this.k = requireNonNull(k, "k is null");
        this.spaceType = requireNonNull(spaceType, "spaceType is null");
        this.efSearch = requireNonNull(efSearch, "efSearch is null");
    }

    // Constructor for regular table scans (backward compatibility)
    public OpenSearchTableHandle(String schemaName, String tableName, String indexName)
    {
        this(schemaName, tableName, indexName, Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty());
    }

    // Constructor for k-NN searches
    public OpenSearchTableHandle(
            String schemaName,
            String tableName,
            String indexName,
            String vectorField,
            float[] queryVector,
            int k,
            String spaceType,
            Integer efSearch)
    {
        this(schemaName, tableName, indexName, Optional.of(vectorField), Optional.of(queryVector),
                Optional.of(k), Optional.of(spaceType), Optional.ofNullable(efSearch));
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getIndexName()
    {
        return indexName;
    }

    @JsonProperty
    public Optional<String> getVectorField()
    {
        return vectorField;
    }

    @JsonProperty
    public Optional<float[]> getQueryVector()
    {
        return queryVector;
    }

    @JsonProperty
    public Optional<Integer> getK()
    {
        return k;
    }

    @JsonProperty
    public Optional<String> getSpaceType()
    {
        return spaceType;
    }

    @JsonProperty
    public Optional<Integer> getEfSearch()
    {
        return efSearch;
    }

    public boolean isKnnSearch()
    {
        return vectorField.isPresent();
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
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
        OpenSearchTableHandle other = (OpenSearchTableHandle) obj;
        return Objects.equals(schemaName, other.schemaName) &&
                Objects.equals(tableName, other.tableName) &&
                Objects.equals(indexName, other.indexName) &&
                Objects.equals(vectorField, other.vectorField) &&
                Objects.deepEquals(queryVector.orElse(null), other.queryVector.orElse(null)) &&
                Objects.equals(k, other.k) &&
                Objects.equals(spaceType, other.spaceType) &&
                Objects.equals(efSearch, other.efSearch);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, indexName, vectorField,
                           Objects.hashCode(queryVector.orElse(null)), k, spaceType, efSearch);
    }

    @Override
    public String toString()
    {
        if (isKnnSearch()) {
            return schemaName + "." + tableName + " (" + indexName + ") [k-NN search]";
        }
        return schemaName + "." + tableName + " (" + indexName + ")";
    }
}
