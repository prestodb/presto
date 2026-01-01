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

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Page source for OpenSearch connector.
 * Fetches data from OpenSearch and converts to Presto pages.
 * Supports nested field extraction using NestedValueExtractor.
 */
public class OpenSearchPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(OpenSearchPageSource.class);
    private static final int BATCH_SIZE = 1000;
    private static final ObjectMapper OBJECT_MAPPER = new JsonObjectMapperProvider().get();

    private final OpenSearchClient client;
    private final OpenSearchSplit split;
    private final List<OpenSearchColumnHandle> columns;
    private final QueryBuilder queryBuilder;
    private final NestedValueExtractor nestedExtractor;

    private boolean finished;
    private long completedBytes;
    private long completedPositions;
    private int currentOffset;

    public OpenSearchPageSource(
            OpenSearchClient client,
            OpenSearchSplit split,
            List<OpenSearchColumnHandle> columns,
            NestedValueExtractor nestedExtractor,
            QueryBuilder queryBuilder)
    {
        this.client = requireNonNull(client, "client is null");
        this.split = requireNonNull(split, "split is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.nestedExtractor = requireNonNull(nestedExtractor, "nestedExtractor is null");
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder is null");
        this.finished = false;
        this.completedBytes = 0;
        this.completedPositions = 0;
        this.currentOffset = 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        if (finished) {
            return null;
        }

        try {
            List<Map<String, Object>> documents;

            // Check if this is a k-NN search
            if (split.isKnnSearch()) {
                // For k-NN searches, we execute once and return all results
                if (currentOffset > 0) {
                    finished = true;
                    return null;
                }

                // Execute k-NN search
                documents = client.executeVectorSearch(
                        split.getIndexName(),
                        split.getVectorField().get(),
                        split.getQueryVector().get(),
                        split.getK().get().intValue(),
                        split.getSpaceType().orElse(null),
                        split.getEfSearch().isPresent() ? split.getEfSearch().get().intValue() : null,
                        null); // No filter for now

                log.debug("k-NN search returned %d documents from index %s",
                        documents.size(), split.getIndexName());
            }
            else {
                // Regular query execution
                // Build query from split's TupleDomain
                String query = queryBuilder.buildQuery(split.getTupleDomain());

                // Get field names for source filtering
                // For nested fields, we need to request only the top-level parent objects,
                // not the dot-notation paths, because OpenSearch's _source filter doesn't
                // work correctly when you mix parent and child paths
                List<String> fieldNames = new ArrayList<>();
                Set<String> topLevelFields = new HashSet<>();

                for (OpenSearchColumnHandle column : columns) {
                    if (!"_id".equals(column.getColumnName())) {
                        String fieldName = column.getColumnName();

                        // Extract top-level field name (before first dot)
                        String topLevelField;
                        int dotIndex = fieldName.indexOf('.');
                        if (dotIndex > 0) {
                            topLevelField = fieldName.substring(0, dotIndex);
                        }
                        else {
                            topLevelField = fieldName;
                        }

                        // Only add top-level field once
                        if (topLevelFields.add(topLevelField)) {
                            fieldNames.add(topLevelField);
                        }
                    }
                }

                // Execute query with shard routing
                documents = client.executeQuery(
                        split.getIndexName(),
                        query,
                        fieldNames,
                        currentOffset,
                        BATCH_SIZE,
                        split.getShardId());

                log.debug("Fetched %d documents from index %s (offset: %d)",
                        documents.size(), split.getIndexName(), currentOffset - documents.size());
            }

            if (documents.isEmpty()) {
                finished = true;
                return null;
            }

            // Build page from documents
            Page page = buildPage(documents);

            // Update state
            currentOffset += documents.size();
            completedPositions += documents.size();
            completedBytes += estimatePageSize(page);

            // For k-NN searches, we're done after one batch
            // For regular queries, check if we got fewer documents than requested
            if (split.isKnnSearch() || documents.size() < BATCH_SIZE) {
                finished = true;
            }

            return page;
        }
        catch (Exception e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to fetch data from OpenSearch", e);
        }
    }

    private Page buildPage(List<Map<String, Object>> documents)
    {
        int positionCount = documents.size();
        Block[] blocks = new Block[columns.size()];

        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            OpenSearchColumnHandle column = columns.get(columnIndex);
            Type type = column.getColumnType();
            BlockBuilder builder = type.createBlockBuilder(null, positionCount);

            for (Map<String, Object> document : documents) {
                Object value;

                // Check if this is a nested field
                if (column.isNestedField()) {
                    // Extract nested field value using NestedValueExtractor
                    value = nestedExtractor.extractNestedValue(
                            document,
                            column.getFieldPathList(),
                            type);
                }
                else {
                    // Extract flat field value (existing logic)
                    value = document.get(column.getColumnName());

                    // For ROW types, keep the Map as-is (will be handled by writeValue)
                    // For VARCHAR with Map/List, convert to JSON string for backward compatibility
                    if (value != null && type.equals(VARCHAR) && (value instanceof Map || value instanceof List)) {
                        try {
                            value = OBJECT_MAPPER.writeValueAsString(value);
                        }
                        catch (Exception e) {
                            log.warn(e, "Failed to convert object to JSON for column %s, using toString()",
                                    column.getColumnName());
                            value = value.toString();
                        }
                    }
                    // For ROW types with Map values, keep as-is (handled in writeValue)
                }

                writeValue(builder, type, value);
            }

            blocks[columnIndex] = builder.build();
        }

        return new Page(positionCount, blocks);
    }

    private void writeValue(BlockBuilder builder, Type type, Object value)
    {
        if (value == null) {
            builder.appendNull();
            return;
        }

        try {
            if (type.equals(VARCHAR)) {
                type.writeSlice(builder, Slices.utf8Slice(value.toString()));
            }
            else if (type.equals(BIGINT)) {
                type.writeLong(builder, ((Number) value).longValue());
            }
            else if (type.equals(INTEGER)) {
                type.writeLong(builder, ((Number) value).intValue());
            }
            else if (type.equals(SMALLINT)) {
                type.writeLong(builder, ((Number) value).shortValue());
            }
            else if (type.equals(TINYINT)) {
                type.writeLong(builder, ((Number) value).byteValue());
            }
            else if (type.equals(DOUBLE)) {
                type.writeDouble(builder, ((Number) value).doubleValue());
            }
            else if (type.equals(REAL)) {
                type.writeLong(builder, Float.floatToIntBits(((Number) value).floatValue()));
            }
            else if (type.equals(BOOLEAN)) {
                type.writeBoolean(builder, (Boolean) value);
            }
            else if (type.equals(DATE)) {
                // Parse date - OpenSearch can return either:
                // 1. Milliseconds since epoch (as Number)
                // 2. ISO 8601 formatted date string (e.g., "1996-01-02")
                long days;
                if (value instanceof Number) {
                    // Convert milliseconds to days since epoch
                    days = ((Number) value).longValue() / (24 * 60 * 60 * 1000);
                }
                else if (value instanceof String) {
                    try {
                        // Parse ISO 8601 date string (yyyy-MM-dd)
                        String dateStr = (String) value;
                        // Handle date strings that may have time component
                        if (dateStr.contains("T")) {
                            dateStr = dateStr.substring(0, dateStr.indexOf('T'));
                        }
                        Instant instant = Instant.parse(dateStr + "T00:00:00Z");
                        // Convert to days since epoch
                        days = instant.toEpochMilli() / (24 * 60 * 60 * 1000);
                    }
                    catch (DateTimeParseException e) {
                        log.warn("Failed to parse date string: %s, setting to null", value);
                        builder.appendNull();
                        return;
                    }
                }
                else {
                    log.warn("Unexpected date type: %s for value: %s, setting to null",
                            value.getClass().getName(), value);
                    builder.appendNull();
                    return;
                }
                type.writeLong(builder, days);
            }
            else if (type.equals(TIMESTAMP)) {
                // Parse timestamp - OpenSearch can return either:
                // 1. Milliseconds since epoch (as Number)
                // 2. ISO 8601 formatted string (e.g., "2025-12-05T10:55:49.526757")
                long timestamp;
                if (value instanceof Number) {
                    timestamp = ((Number) value).longValue();
                }
                else if (value instanceof String) {
                    try {
                        // Parse ISO 8601 timestamp string
                        String timestampStr = (String) value;

                        // Handle timestamps with microsecond precision by padding to nanoseconds
                        // OpenSearch may return timestamps like "2025-12-04T12:37:32.007263"
                        // which have 6 decimal places (microseconds) instead of 9 (nanoseconds)
                        if (timestampStr.contains(".")) {
                            int dotIndex = timestampStr.indexOf('.');
                            int endIndex = timestampStr.length();
                            // Find where the fractional seconds end (before 'Z' or '+' or '-')
                            for (int i = dotIndex + 1; i < timestampStr.length(); i++) {
                                char c = timestampStr.charAt(i);
                                if (c == 'Z' || c == '+' || c == '-') {
                                    endIndex = i;
                                    break;
                                }
                            }

                            String fractionalPart = timestampStr.substring(dotIndex + 1, endIndex);
                            String suffix = endIndex < timestampStr.length() ? timestampStr.substring(endIndex) : "Z";

                            // Pad or truncate to 9 digits (nanoseconds)
                            if (fractionalPart.length() < 9) {
                                fractionalPart = String.format("%-9s", fractionalPart).replace(' ', '0');
                            }
                            else if (fractionalPart.length() > 9) {
                                fractionalPart = fractionalPart.substring(0, 9);
                            }

                            timestampStr = timestampStr.substring(0, dotIndex + 1) + fractionalPart + suffix;
                        }
                        else if (!timestampStr.endsWith("Z")) {
                            // Add 'Z' if no timezone specified
                            timestampStr += "Z";
                        }

                        Instant instant = Instant.parse(timestampStr);
                        // Convert to milliseconds since epoch
                        timestamp = instant.toEpochMilli();
                    }
                    catch (DateTimeParseException e) {
                        log.warn("Failed to parse timestamp string: %s, setting to null", value);
                        builder.appendNull();
                        return;
                    }
                }
                else {
                    log.warn("Unexpected timestamp type: %s for value: %s, setting to null",
                            value.getClass().getName(), value);
                    builder.appendNull();
                    return;
                }
                type.writeLong(builder, timestamp);
            }
            else if (type instanceof RowType) {
                // Handle ROW types (nested objects)
                RowType rowType = (RowType) type;
                List<RowType.Field> fields = rowType.getFields();

                if (value instanceof Map) {
                    Map<String, Object> map = (Map<String, Object>) value;
                    BlockBuilder rowBlockBuilder = builder.beginBlockEntry();

                    for (int i = 0; i < fields.size(); i++) {
                        RowType.Field field = fields.get(i);
                        String fieldName = field.getName().orElse("field" + i);
                        Type fieldType = field.getType();
                        Object fieldValue = map.get(fieldName);

                        writeValue(rowBlockBuilder, fieldType, fieldValue);
                    }

                    builder.closeEntry();
                }
                else {
                    builder.appendNull();
                }
            }
            else if (type instanceof ArrayType) {
                // Handle arrays (including vectors)
                ArrayType arrayType = (ArrayType) type;
                Type elementType = arrayType.getElementType();

                if (value instanceof List) {
                    List<?> list = (List<?>) value;
                    BlockBuilder arrayBuilder = builder.beginBlockEntry();

                    for (Object element : list) {
                        writeValue(arrayBuilder, elementType, element);
                    }

                    builder.closeEntry();
                }
                else if (value instanceof Object[]) {
                    // Handle Object arrays (Jackson sometimes returns Object[] instead of List)
                    Object[] array = (Object[]) value;
                    BlockBuilder arrayBuilder = builder.beginBlockEntry();

                    for (Object element : array) {
                        writeValue(arrayBuilder, elementType, element);
                    }

                    builder.closeEntry();
                }
                else {
                    builder.appendNull();
                }
            }
            else {
                // Default: convert to string
                type.writeSlice(builder, Slices.utf8Slice(value.toString()));
            }
        }
        catch (Exception e) {
            log.warn(e, "Failed to write value of type %s: %s", type, value);
            builder.appendNull();
        }
    }

    private long estimatePageSize(Page page)
    {
        return page.getSizeInBytes();
    }

    @Override
    public long getCompletedPositions()
    {
        return completedPositions;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
        // Cleanup resources if needed
        finished = true;
    }
}
