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
package com.facebook.presto.plugin.clp.metadata;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.clp.ClpColumnHandle;
import com.facebook.presto.plugin.clp.ClpErrorCode;
import com.facebook.presto.spi.PrestoException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ClpSchemaTree
{
    static class ClpNode
    {
        Type type; // Only non-null for leaf nodes
        String originalName;
        Map<String, ClpNode> children = new HashMap<>();
        Set<String> conflictingBaseNames = new HashSet<>();

        ClpNode(String originalName)
        {
            this.originalName = originalName;
        }

        ClpNode(String originalName, Type type)
        {
            this.originalName = originalName;
            this.type = type;
        }

        boolean isLeaf()
        {
            return children.isEmpty();
        }
    }

    private final ClpNode root;
    private final boolean polymorphicTypeEnabled;
    ClpSchemaTree(boolean polymorphicTypeEnabled)
    {
        this.polymorphicTypeEnabled = polymorphicTypeEnabled;
        this.root = new ClpNode(""); // Root doesn't have an original name
    }

    private Type mapColumnType(byte type)
    {
        switch (ClpNodeType.fromType(type)) {
            case Integer:
                return BigintType.BIGINT;
            case Float:
                return DoubleType.DOUBLE;
            case ClpString:
            case VarString:
            case DateString:
            case NullValue:
                return VarcharType.VARCHAR;
            case UnstructuredArray:
                return new ArrayType(VarcharType.VARCHAR);
            case Boolean:
                return BooleanType.BOOLEAN;
            default:
                throw new PrestoException(ClpErrorCode.CLP_UNSUPPORTED_TYPE, "Unsupported type: " + type);
        }
    }

    /**
     * Adds a column to the internal CLP schema tree, creating intermediate nested nodes as needed.
     * Handles potential name conflicts when polymorphic types are enabled by suffixing column names
     * with type display names.
     *
     * @param fullName Fully qualified column name using dot notation (e.g., "a.b.c").
     * @param type     Serialized byte value representing the CLP column's type.
     */
    public void addColumn(String fullName, byte type)
    {
        Type prestoType = mapColumnType(type);
        String[] path = fullName.split("\\.");
        ClpNode current = root;

        for (int i = 0; i < path.length - 1; i++) {
            String segment = path[i];
            ClpNode existingNode = current.children.get(segment);

            if (polymorphicTypeEnabled && existingNode != null && existingNode.type != null) {
                // Conflict: An intermediate segment already exists as a leaf node. Rename it.
                String existingSuffix = getTypeSuffix(existingNode.type);
                String renamedExisting = segment + "_" + existingSuffix;
                current.children.remove(segment);
                current.children.put(renamedExisting, existingNode);
            }
            current = current.children.computeIfAbsent(segment, ClpNode::new);
            current.type = null;
        }

        String leafName = path[path.length - 1];
        String finalLeafName = resolvePolymorphicConflicts(current, leafName, prestoType);

        ClpNode leaf = new ClpNode(leafName, prestoType);
        current.children.put(finalLeafName, leaf);
    }

    /**
     * Traverses the CLP schema tree and collects all leaf and nested structure nodes
     * into a flat list of column handles. For nested structures, builds a RowType
     * from child nodes.
     *
     * @return List of ClpColumnHandle objects representing the full schema.
     */
    public List<ClpColumnHandle> collectColumnHandles()
    {
        List<ClpColumnHandle> columns = new ArrayList<>();
        for (Map.Entry<String, ClpNode> entry : root.children.entrySet()) {
            String name = entry.getKey();
            ClpNode child = entry.getValue();
            if (child.isLeaf()) {
                columns.add(new ClpColumnHandle(name, child.originalName, child.type, true));
            }
            else {
                Type rowType = buildRowType(child);
                columns.add(new ClpColumnHandle(name, child.originalName, rowType, true));
            }
        }
        return columns;
    }

    private String resolvePolymorphicConflicts(ClpNode parent, String baseName, Type newType)
    {
        if (!polymorphicTypeEnabled) {
            return baseName;
        }

        boolean conflictDetected = false;
        if (parent.children.containsKey(baseName)) {
            ClpNode existing = parent.children.get(baseName);
            if (existing.type == null) {
                conflictDetected = true;
            }
            else if (!existing.type.equals(newType)) {
                String existingSuffix = getTypeSuffix(existing.type);
                String renamedExisting = baseName + "_" + existingSuffix;
                parent.children.remove(baseName);
                parent.children.put(renamedExisting, existing);
                parent.conflictingBaseNames.add(baseName);
                conflictDetected = true;
            }
        }
        else if (parent.conflictingBaseNames.contains(baseName)) {
            conflictDetected = true;
        }

        if (conflictDetected) {
            String newSuffix = getTypeSuffix(newType);
            return baseName + "_" + newSuffix;
        }

        return baseName;
    }

    private String getTypeSuffix(Type type)
    {
        return (type instanceof ArrayType) ? "array" : type.getDisplayName();
    }

    private Type buildRowType(ClpNode node)
    {
        List<RowType.Field> fields = new ArrayList<>();
        List<String> sortedKeys = new ArrayList<>(node.children.keySet());
        Collections.sort(sortedKeys);

        for (String name : sortedKeys) {
            ClpNode child = node.children.get(name);
            Type fieldType = child.isLeaf() ? child.type : buildRowType(child);
            fields.add(new RowType.Field(Optional.of(name), fieldType));
        }
        return RowType.from(fields);
    }
}
