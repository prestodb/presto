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
package com.facebook.presto.spi.constraints;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class TableConstraintsHolder
{
    private final List<TableConstraint<String>> tableConstraints;
    private final Map<String, ColumnHandle> columnNameToHandleAssignments;

    public TableConstraintsHolder(List<TableConstraint<String>> tableConstraints, Map<String, ColumnHandle> columnNameToHandleAssignments)
    {
        requireNonNull(tableConstraints, "tableConstraints is null");
        requireNonNull(columnNameToHandleAssignments, "columnNameToHandleAssignments is null");
        validateTableConstraints(tableConstraints);
        this.tableConstraints = Collections.unmodifiableList(new ArrayList<>(tableConstraints));
        this.columnNameToHandleAssignments = Collections.unmodifiableMap(columnNameToHandleAssignments);
    }

    public static void validateTableConstraints(Collection<TableConstraint<String>> constraints)
    {
        constraints.forEach(constraint -> {
            if (!(constraint instanceof UniqueConstraint || constraint instanceof NotNullConstraint)) {
                throw new PrestoException(NOT_SUPPORTED,
                        format("Constraint %s of unknown type (%s) is not supported", constraint.getName().orElse(""), constraint.getClass().getName()));
            }
        });
    }

    public List<TableConstraint<String>> getTableConstraints()
    {
        return tableConstraints;
    }

    public List<TableConstraint<ColumnHandle>> getTableConstraintsWithColumnHandles()
    {
        if (columnNameToHandleAssignments.isEmpty()) {
            return emptyList();
        }
        return rebaseTableConstraints(tableConstraints, columnNameToHandleAssignments);
    }

    //rebase a list ot table constraints of column reference type T on column reference type R
    private <T, R> List<TableConstraint<R>> rebaseTableConstraints(List<TableConstraint<T>> tableConstraints, Map<T, R> assignments)
    {
        List<TableConstraint<R>> mappedTableConstraints = new ArrayList<>();
        tableConstraints.stream().forEach(tableConstraint ->
        {
            Optional<TableConstraint<R>> mappedConstraint = tableConstraint.rebaseConstraint(assignments);
            if (mappedConstraint.isPresent()) {
                mappedTableConstraints.add(mappedConstraint.get());
            }
        });
        return mappedTableConstraints;
    }
}
