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
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.plugin.opensearch.types.TypeMapper;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;

/**
 * Discovers and maps nested fields from OpenSearch index mappings.
 * Recursively traverses object and nested types to create virtual columns
 * that can be queried using dot notation (e.g., token_usage.total_tokens).
 *
 * Example mapping:
 * {
 *   "token_usage": {
 *     "type": "object",
 *     "properties": {
 *       "total_tokens": {"type": "long"},
 *       "input_tokens": {"type": "long"},
 *       "output_tokens": {"type": "long"}
 *     }
 *   }
 * }
 *
 * Produces fields:
 * - token_usage.total_tokens (BIGINT)
 * - token_usage.input_tokens (BIGINT)
 * - token_usage.output_tokens (BIGINT)
 */
public class NestedFieldMapper
{
    private static final Logger log = Logger.get(NestedFieldMapper.class);

    private final int maxDepth;

    @Inject
    public NestedFieldMapper(OpenSearchConfig config)
    {
        this.maxDepth = config.getNestedMaxDepth();
    }

    // Constructor for testing
    public NestedFieldMapper(int maxDepth)
    {
        this.maxDepth = maxDepth;
    }

    /**
     * Discovers all nested fields in the mapping.
     *
     * @param mapping The OpenSearch index mapping (properties section)
     * @return Map of field path to NestedFieldInfo
     */
    public Map<String, NestedFieldInfo> discoverNestedFields(Map<String, Object> mapping)
    {
        Map<String, NestedFieldInfo> result = new LinkedHashMap<>();
        discoverFieldsRecursive(mapping, "", new ArrayList<>(), 0, result);
        log.debug("Discovered %d nested fields", result.size());
        return result;
    }

    /**
     * Recursively discovers fields in the mapping.
     *
     * @param properties Current level properties
     * @param currentPath Current field path (e.g., "token_usage")
     * @param pathList Current path as list (e.g., ["token_usage"])
     * @param depth Current nesting depth
     * @param result Accumulator for discovered fields
     */
    private void discoverFieldsRecursive(
            Map<String, Object> properties,
            String currentPath,
            List<String> pathList,
            int depth,
            Map<String, NestedFieldInfo> result)
    {
        // Check depth limit to prevent infinite recursion
        if (depth > maxDepth) {
            log.warn("Maximum nesting depth (%d) exceeded at path: %s", maxDepth, currentPath);
            return;
        }

        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String fieldName = entry.getKey();

            // Skip if not a map (invalid structure)
            if (!(entry.getValue() instanceof Map)) {
                log.debug("Skipping non-map field: %s", fieldName);
                continue;
            }

            Map<String, Object> fieldProps = (Map<String, Object>) entry.getValue();
            String fieldType = (String) fieldProps.getOrDefault("type", "object");

            // Build field path
            String fieldPath = currentPath.isEmpty()
                    ? fieldName
                    : currentPath + "." + fieldName;

            List<String> newPathList = new ArrayList<>(pathList);
            newPathList.add(fieldName);

            // Determine if this is a leaf field (primitive type)
            boolean isLeaf = !fieldType.equals("object") && !fieldType.equals("nested");

            Type prestoType;
            if (isLeaf) {
                // Leaf field - use TypeMapper to get Presto type
                prestoType = TypeMapper.toPrestoType(fieldType, fieldProps, fieldName);

                // Add leaf field - these are queryable columns
                NestedFieldInfo fieldInfo = new NestedFieldInfo(
                        fieldPath,
                        currentPath,
                        fieldName,
                        prestoType,
                        fieldType,
                        depth,
                        true,
                        newPathList);
                result.put(fieldPath, fieldInfo);
                log.debug("Discovered leaf field: %s (type: %s, depth: %d)",
                         fieldPath, fieldType, depth);
            }
            else {
                // Parent field (object/nested type) - build proper ROW type for SQL dereference support
                if (fieldProps.containsKey("properties")) {
                    Map<String, Object> nestedProps = (Map<String, Object>) fieldProps.get("properties");

                    // Build ROW type for the nested object to enable SQL dereference (e.g., details.address)
                    Optional<RowType> rowType = buildRowType(nestedProps);
                    if (rowType.isPresent()) {
                        prestoType = rowType.get();

                        NestedFieldInfo fieldInfo = new NestedFieldInfo(
                                fieldPath,
                                currentPath,
                                fieldName,
                                prestoType,
                                fieldType,
                                depth,
                                false,
                                newPathList);
                        result.put(fieldPath, fieldInfo);
                        log.debug("Discovered parent field as ROW: %s (type: %s, depth: %d)",
                                 fieldPath, fieldType, depth);
                    }
                    else {
                        // If ROW type building failed, fall back to VARCHAR
                        prestoType = VARCHAR;

                        NestedFieldInfo fieldInfo = new NestedFieldInfo(
                                fieldPath,
                                currentPath,
                                fieldName,
                                prestoType,
                                fieldType,
                                depth,
                                false,
                                newPathList);
                        result.put(fieldPath, fieldInfo);
                        log.debug("Discovered parent field as VARCHAR (ROW build failed): %s (type: %s, depth: %d)",
                                 fieldPath, fieldType, depth);
                    }

                    // Recurse into nested properties to discover leaf fields
                    // Leaf fields will be added with their full paths
                    discoverFieldsRecursive(
                            nestedProps,
                            fieldPath,
                            newPathList,
                            depth + 1,
                            result);
                }
                else {
                    // Object with no properties - expose as VARCHAR
                    log.debug("Object field %s has no properties, exposing as VARCHAR", fieldPath);
                    prestoType = VARCHAR;

                    NestedFieldInfo fieldInfo = new NestedFieldInfo(
                            fieldPath,
                            currentPath,
                            fieldName,
                            prestoType,
                            fieldType,
                            depth,
                            false,
                            newPathList);
                    result.put(fieldPath, fieldInfo);
                    log.debug("Discovered parent field without properties as VARCHAR: %s (type: %s, depth: %d)",
                             fieldPath, fieldType, depth);
                }
            }
        }
    }

    /**
     * Filters discovered fields to only include leaf fields (queryable columns).
     * Parent object fields are excluded as they cannot be directly queried.
     *
     * @param allFields All discovered fields
     * @return Map containing only leaf fields
     */
    public Map<String, NestedFieldInfo> getLeafFieldsOnly(
            Map<String, NestedFieldInfo> allFields)
    {
        Map<String, NestedFieldInfo> leafFields = new HashMap<>();
        for (Map.Entry<String, NestedFieldInfo> entry : allFields.entrySet()) {
            if (entry.getValue().isLeafField()) {
                leafFields.put(entry.getKey(), entry.getValue());
            }
        }
        log.debug("Filtered to %d leaf fields from %d total fields",
                 leafFields.size(), allFields.size());
        return leafFields;
    }

    /**
     * Gets the maximum nesting depth configured for this mapper.
     */
    public int getMaxDepth()
    {
        return maxDepth;
    }

    /**
     * Builds a ROW type for a nested object field based on its properties.
     * This allows nested objects to be accessed using SQL dereference syntax.
     *
     * @param properties The properties map from the OpenSearch mapping
     * @return A RowType representing the nested structure, or empty if no valid properties
     */
    public Optional<RowType> buildRowType(Map<String, Object> properties)
    {
        if (properties == null || properties.isEmpty()) {
            return Optional.empty();
        }

        List<RowType.Field> fields = new ArrayList<>();

        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String fieldName = entry.getKey();

            if (!(entry.getValue() instanceof Map)) {
                continue;
            }

            Map<String, Object> fieldProps = (Map<String, Object>) entry.getValue();
            String fieldType = (String) fieldProps.getOrDefault("type", "object");

            Type prestoType;
            if (fieldType.equals("object") || fieldType.equals("nested")) {
                // Recursively build ROW type for nested objects
                if (fieldProps.containsKey("properties")) {
                    Map<String, Object> nestedProps = (Map<String, Object>) fieldProps.get("properties");
                    Optional<RowType> nestedRowType = buildRowType(nestedProps);
                    if (nestedRowType.isPresent()) {
                        prestoType = nestedRowType.get();
                    }
                    else {
                        // If no valid nested properties, skip this field
                        continue;
                    }
                }
                else {
                    // Object with no properties - skip
                    continue;
                }
            }
            else {
                // Leaf field - use TypeMapper to get Presto type
                prestoType = TypeMapper.toPrestoType(fieldType, fieldProps, fieldName);
            }

            fields.add(RowType.field(fieldName, prestoType));
        }

        if (fields.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(RowType.from(fields));
    }
}
