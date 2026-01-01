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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.table.AbstractConnectorTableFunction;
import com.facebook.presto.spi.function.table.Argument;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.facebook.presto.spi.function.table.Descriptor;
import com.facebook.presto.spi.function.table.ScalarArgument;
import com.facebook.presto.spi.function.table.ScalarArgumentSpecification;
import com.facebook.presto.spi.function.table.TableFunctionAnalysis;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.Slice;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static java.util.Objects.requireNonNull;

/**
 * Table function for k-NN vector search using OpenSearch k-NN plugin.
 *
 * Usage:
 * SELECT * FROM TABLE(opensearch.system.knn_search(
 *   index_name => 'my_index',
 *   vector_field => 'embedding',
 *   query_vector => ARRAY[0.1, 0.2, 0.3],
 *   k => 10,
 *   space_type => 'cosine',
 *   ef_search => 100
 *))
 */
public class KnnSearchTableFunction
        extends AbstractConnectorTableFunction
{
    private static final Logger log = Logger.get(KnnSearchTableFunction.class);
    private static final String SCHEMA_NAME = "system";
    private static final String FUNCTION_NAME = "knn_search";

    private final OpenSearchClient client;
    private final OpenSearchConfig config;

    @Inject
    public KnnSearchTableFunction(OpenSearchClient client, OpenSearchConfig config)
    {
        super(
                SCHEMA_NAME,
                FUNCTION_NAME,
                List.of(
                        ScalarArgumentSpecification.builder()
                                .name("INDEX_NAME")
                                .type(VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name("VECTOR_FIELD")
                                .type(VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name("QUERY_VECTOR")
                                .type(new ArrayType(DOUBLE))
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name("K")
                                .type(INTEGER)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name("SPACE_TYPE")
                                .type(VARCHAR)
                                .defaultValue(null)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name("EF_SEARCH")
                                .type(INTEGER)
                                .defaultValue(null)
                                .build()),
                GENERIC_TABLE);

        this.client = requireNonNull(client, "client is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public TableFunctionAnalysis analyze(
            ConnectorSession session,
            ConnectorTransactionHandle transaction,
            Map<String, Argument> arguments)
    {
        // Extract arguments
        String indexName = ((Slice) ((ScalarArgument) arguments.get("INDEX_NAME")).getValue()).toStringUtf8();
        String vectorField = ((Slice) ((ScalarArgument) arguments.get("VECTOR_FIELD")).getValue()).toStringUtf8();
        float[] queryVector = extractVectorFromArgument((ScalarArgument) arguments.get("QUERY_VECTOR"));

        int k = arguments.containsKey("K") ?
                ((Long) ((ScalarArgument) arguments.get("K")).getValue()).intValue() :
                config.getVectorSearchDefaultK();

        String spaceType = arguments.containsKey("SPACE_TYPE") && ((ScalarArgument) arguments.get("SPACE_TYPE")).getValue() != null ?
                ((Slice) ((ScalarArgument) arguments.get("SPACE_TYPE")).getValue()).toStringUtf8() :
                config.getVectorSearchDefaultSpaceType();

        Integer efSearch = arguments.containsKey("EF_SEARCH") && ((ScalarArgument) arguments.get("EF_SEARCH")).getValue() != null ?
                ((Long) ((ScalarArgument) arguments.get("EF_SEARCH")).getValue()).intValue() :
                config.getVectorSearchDefaultEfSearch();

        log.debug("k-NN search: index=%s, field=%s, k=%d, space_type=%s",
                indexName, vectorField, k, spaceType);
        // Validate k value
        if (k <= 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "k must be positive, got: " + k);
        }

        // Get index mapping to determine result schema
        Map<String, Object> indexMapping = client.getIndexMapping(indexName);

        // Validate that the vector field exists and is of type knn_vector
        validateVectorField(indexMapping, vectorField, queryVector.length);

        List<ColumnHandle> columns = buildColumnsFromMapping(indexMapping);

        // Create descriptor for return type
        List<Descriptor.Field> fields = new ArrayList<>();
        fields.add(new Descriptor.Field("_id", Optional.of(VARCHAR)));
        fields.add(new Descriptor.Field("_score", Optional.of(DOUBLE)));

        // Add fields from index mapping (skip _id and _score as they're already added)
        for (ColumnHandle columnHandle : columns) {
            OpenSearchColumnHandle col = (OpenSearchColumnHandle) columnHandle;
            if (!col.getColumnName().equals("_id") && !col.getColumnName().equals("_score")) {
                fields.add(new Descriptor.Field(col.getColumnName(), Optional.of(col.getColumnType())));
            }
        }

        Descriptor returnedType = new Descriptor(fields);

        // Create handle with search parameters
        KnnSearchHandle handle = new KnnSearchHandle(
                indexName, vectorField, queryVector, k, spaceType, efSearch, columns);

        return TableFunctionAnalysis.builder()
                .returnedType(returnedType)
                .handle(handle)
                .build();
    }

    private float[] extractVectorFromArgument(ScalarArgument argument)
    {
        Block block = (Block) argument.getValue();
        int size = block.getPositionCount();

        float[] vector = new float[size];

        for (int i = 0; i < size; i++) {
            if (block.isNull(i)) {
                throw new IllegalArgumentException("Query vector cannot contain null values");
            }
            vector[i] = (float) DOUBLE.getDouble(block, i);
        }

        return vector;
    }

    private List<ColumnHandle> buildColumnsFromMapping(Map<String, Object> indexMapping)
    {
        List<ColumnHandle> columns = new ArrayList<>();

        // Add _id and _score columns (must match descriptor order)
        columns.add(new OpenSearchColumnHandle("_id", VARCHAR, "keyword"));
        columns.add(new OpenSearchColumnHandle("_score", DOUBLE, "double"));

        // Add columns from mapping
        for (Map.Entry<String, Object> entry : indexMapping.entrySet()) {
            String fieldName = entry.getKey();
            // Skip knn_vector fields as they can't be retrieved
            if (entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> fieldMapping = (Map<String, Object>) entry.getValue();
                Object typeObj = fieldMapping.get("type");
                if (typeObj instanceof String) {
                    String fieldType = (String) typeObj;
                    if (!"knn_vector".equals(fieldType)) {
                        columns.add(new OpenSearchColumnHandle(fieldName, VARCHAR, fieldType));
                    }
                }
            }
        }

        return columns;
    }

    /**
     * Validates that the specified field exists in the index mapping and is of type knn_vector.
     *
     * @param indexMapping The index mapping
     * @param vectorField The name of the vector field
     * @param queryVectorDimension The dimension of the query vector
     * @throws PrestoException if the field doesn't exist, is not a knn_vector, or has dimension mismatch
     */
    private void validateVectorField(Map<String, Object> indexMapping, String vectorField, int queryVectorDimension)
    {
        // Check if field exists in mapping
        if (!indexMapping.containsKey(vectorField)) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    String.format("Vector field '%s' does not exist in index mapping. Available fields: %s",
                            vectorField, indexMapping.keySet()));
        }

        Object fieldValue = indexMapping.get(vectorField);
        if (!(fieldValue instanceof Map)) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    String.format("Vector field '%s' has invalid mapping structure", vectorField));
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> fieldMapping = (Map<String, Object>) fieldValue;
        Object typeObj = fieldMapping.get("type");

        if (!(typeObj instanceof String)) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    String.format("Vector field '%s' does not have a type specified in mapping", vectorField));
        }

        String fieldType = (String) typeObj;
        if (!"knn_vector".equals(fieldType)) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    String.format("Field '%s' is not a knn_vector field. Found type: %s. " +
                            "k-NN search can only be performed on fields of type 'knn_vector'.",
                            vectorField, fieldType));
        }

        // Validate vector dimensions if specified in mapping
        Object dimensionObj = fieldMapping.get("dimension");
        if (dimensionObj instanceof Number) {
            int fieldDimension = ((Number) dimensionObj).intValue();
            if (fieldDimension != queryVectorDimension) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        String.format("Query vector dimension (%d) does not match field '%s' dimension (%d)",
                                queryVectorDimension, vectorField, fieldDimension));
            }
        }

        log.debug("Validated vector field '%s': type=knn_vector, dimension=%s",
                vectorField, dimensionObj != null ? dimensionObj : "unspecified");
    }

    /**
     * Handle for k-NN search table function.
     */
    public static class KnnSearchHandle
            implements ConnectorTableFunctionHandle
    {
        private final String indexName;
        private final String vectorField;
        private final float[] queryVector;
        private final int k;
        private final String spaceType;
        private final Integer efSearch;
        private final List<ColumnHandle> columns;

        @JsonCreator
        public KnnSearchHandle(
                @JsonProperty("indexName") String indexName,
                @JsonProperty("vectorField") String vectorField,
                @JsonProperty("queryVector") float[] queryVector,
                @JsonProperty("k") int k,
                @JsonProperty("spaceType") String spaceType,
                @JsonProperty("efSearch") Integer efSearch,
                @JsonProperty("columns") List<ColumnHandle> columns)
        {
            this.indexName = requireNonNull(indexName, "indexName is null");
            this.vectorField = requireNonNull(vectorField, "vectorField is null");
            this.queryVector = requireNonNull(queryVector, "queryVector is null");
            this.k = k;
            this.spaceType = requireNonNull(spaceType, "spaceType is null");
            this.efSearch = efSearch;
            this.columns = requireNonNull(columns, "columns is null");
        }

        @JsonProperty
        public String getIndexName()
        {
            return indexName;
        }

        @JsonProperty
        public String getVectorField()
        {
            return vectorField;
        }

        @JsonProperty
        public float[] getQueryVector()
        {
            return queryVector;
        }

        @JsonProperty
        public int getK()
        {
            return k;
        }

        @JsonProperty
        public String getSpaceType()
        {
            return spaceType;
        }

        @JsonProperty
        public Integer getEfSearch()
        {
            return efSearch;
        }

        @JsonProperty
        public List<ColumnHandle> getColumns()
        {
            return columns;
        }
    }
}
