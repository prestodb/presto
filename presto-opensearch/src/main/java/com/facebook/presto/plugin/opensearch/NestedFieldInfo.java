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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Information about a nested field discovered in OpenSearch mapping.
 * Represents metadata for fields that may be nested within object structures.
 *
 * Example:
 * For a field "token_usage.total_tokens":
 * - fieldPath: "token_usage.total_tokens"
 * - parentPath: "token_usage"
 * - fieldName: "total_tokens"
 * - fieldPathList: ["token_usage", "total_tokens"]
 */
public class NestedFieldInfo
{
    private final String fieldPath;           // Full path: "token_usage.total_tokens"
    private final String parentPath;          // Parent path: "token_usage" (empty for top-level)
    private final String fieldName;           // Field name: "total_tokens"
    private final Type prestoType;            // Presto type: BIGINT or ROW type for objects
    private final String openSearchType;      // OpenSearch type: "long"
    private final int nestingLevel;           // Nesting depth: 0 for top-level, 1+ for nested
    private final boolean isLeafField;        // true if primitive type, false if object/nested
    private final List<String> fieldPathList; // Path as list: ["token_usage", "total_tokens"]

    public NestedFieldInfo(
            String fieldPath,
            String parentPath,
            String fieldName,
            Type prestoType,
            String openSearchType,
            int nestingLevel,
            boolean isLeafField,
            List<String> fieldPathList)
    {
        this.fieldPath = requireNonNull(fieldPath, "fieldPath is null");
        this.parentPath = parentPath != null ? parentPath : "";
        this.fieldName = requireNonNull(fieldName, "fieldName is null");
        this.prestoType = requireNonNull(prestoType, "prestoType is null");
        this.openSearchType = requireNonNull(openSearchType, "openSearchType is null");
        this.nestingLevel = nestingLevel;
        this.isLeafField = isLeafField;
        this.fieldPathList = fieldPathList != null ?
            Collections.unmodifiableList(fieldPathList) :
            Collections.emptyList();
    }

    public String getFieldPath()
    {
        return fieldPath;
    }

    public String getParentPath()
    {
        return parentPath;
    }

    public String getFieldName()
    {
        return fieldName;
    }

    public Type getPrestoType()
    {
        return prestoType;
    }

    public String getOpenSearchType()
    {
        return openSearchType;
    }

    public int getNestingLevel()
    {
        return nestingLevel;
    }

    public boolean isLeafField()
    {
        return isLeafField;
    }

    public List<String> getFieldPathList()
    {
        return fieldPathList;
    }

    /**
     * Returns true if this field is nested (has more than one path segment)
     * A field is considered nested if its path contains dots, indicating
     * it's a child of a parent object (e.g., token_usage.total_tokens)
     */
    public boolean isNestedField()
    {
        return fieldPathList != null && fieldPathList.size() > 1;
    }

    /**
     * Returns the JSON path for this field (e.g., "$.token_usage.total_tokens")
     */
    public String getJsonPath()
    {
        return "$." + fieldPath;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NestedFieldInfo that = (NestedFieldInfo) obj;
        return Objects.equals(fieldPath, that.fieldPath) &&
                Objects.equals(prestoType, that.prestoType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fieldPath, prestoType);
    }

    @Override
    public String toString()
    {
        return "NestedFieldInfo{" +
                "fieldPath='" + fieldPath + '\'' +
                ", parentPath='" + parentPath + '\'' +
                ", prestoType=" + prestoType +
                ", openSearchType='" + openSearchType + '\'' +
                ", nestingLevel=" + nestingLevel +
                ", isLeafField=" + isLeafField +
                '}';
    }
}
