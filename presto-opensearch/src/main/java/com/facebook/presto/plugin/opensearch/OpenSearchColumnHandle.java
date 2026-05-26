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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Column handle for OpenSearch connector.
 * Represents a field in an OpenSearch index, including support for nested fields.
 *
 * For nested fields (e.g., token_usage.total_tokens):
 * - columnName: "token_usage.total_tokens"
 * - parentFieldPath: "token_usage"
 * - fieldPathList: ["token_usage", "total_tokens"]
 * - isNestedField: true
 */
public class OpenSearchColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final Type columnType;
    private final String openSearchType;
    private final boolean isVector;
    private final int vectorDimension;

    // Nested field support
    private final boolean isNestedField;
    private final String parentFieldPath;
    private final List<String> fieldPathList;
    private final String jsonPath;

    @JsonCreator
    public OpenSearchColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("openSearchType") String openSearchType,
            @JsonProperty("isVector") boolean isVector,
            @JsonProperty("vectorDimension") int vectorDimension,
            @JsonProperty("isNestedField") boolean isNestedField,
            @JsonProperty("parentFieldPath") String parentFieldPath,
            @JsonProperty("fieldPathList") List<String> fieldPathList,
            @JsonProperty("jsonPath") String jsonPath)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.openSearchType = requireNonNull(openSearchType, "openSearchType is null");
        this.isVector = isVector;
        this.vectorDimension = vectorDimension;
        this.isNestedField = isNestedField;
        this.parentFieldPath = parentFieldPath;
        this.fieldPathList = fieldPathList != null ?
                Collections.unmodifiableList(fieldPathList) :
                Collections.emptyList();
        this.jsonPath = jsonPath;
    }

    // Constructor for backward compatibility (non-nested fields)
    public OpenSearchColumnHandle(
            String columnName,
            Type columnType,
            String openSearchType,
            boolean isVector,
            int vectorDimension)
    {
        this(columnName, columnType, openSearchType, isVector, vectorDimension,
                false, null, null, null);
    }

    // Convenience constructor for simple fields
    public OpenSearchColumnHandle(String columnName, Type columnType, String openSearchType)
    {
        this(columnName, columnType, openSearchType, false, 0, false, null, null, null);
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public String getOpenSearchType()
    {
        return openSearchType;
    }

    @JsonProperty
    public boolean isVector()
    {
        return isVector;
    }

    @JsonProperty
    public int getVectorDimension()
    {
        return vectorDimension;
    }

    @JsonProperty
    public boolean isNestedField()
    {
        // Compute from fieldPathList instead of using stored field
        // This ensures correctness even after JSON deserialization
        return fieldPathList != null && fieldPathList.size() > 1;
    }

    @JsonProperty
    public String getParentFieldPath()
    {
        return parentFieldPath;
    }

    @JsonProperty
    public List<String> getFieldPathList()
    {
        return fieldPathList;
    }

    @JsonProperty
    public String getJsonPath()
    {
        return jsonPath;
    }

    public ColumnMetadata toColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(columnName)
                .setType(columnType)
                .build();
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
        OpenSearchColumnHandle other = (OpenSearchColumnHandle) obj;
        return Objects.equals(columnName, other.columnName) &&
                Objects.equals(columnType, other.columnType) &&
                Objects.equals(openSearchType, other.openSearchType) &&
                isVector == other.isVector &&
                vectorDimension == other.vectorDimension &&
                isNestedField == other.isNestedField &&
                Objects.equals(parentFieldPath, other.parentFieldPath) &&
                Objects.equals(fieldPathList, other.fieldPathList);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, columnType, openSearchType, isVector, vectorDimension,
                isNestedField, parentFieldPath, fieldPathList);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(columnName).append(":").append(columnType)
          .append(" (").append(openSearchType);

        if (isVector) {
            sb.append(", vector[").append(vectorDimension).append("]");
        }

        if (isNestedField) {
            sb.append(", nested, parent=").append(parentFieldPath);
        }

        sb.append(")");
        return sb.toString();
    }
}
