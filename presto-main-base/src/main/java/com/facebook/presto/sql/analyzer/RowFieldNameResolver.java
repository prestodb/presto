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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.common.type.RowType;

import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Keeps row-field resolution consistent across analysis, interpretation, and row-expression translation.
 *
 * <p>The "prefer the exact-case field, fall back to a unique case-insensitive match, otherwise ambiguous" policy
 * used here is mirrored for physical Parquet paths by {@code ParquetTypeUtils.lookupDescriptor}. The two cannot
 * share code because {@code presto-main-base} and {@code presto-parquet} must not depend on each other, so keep
 * the two implementations in sync when the policy changes.
 */
public final class RowFieldNameResolver
{
    private RowFieldNameResolver() {}

    /**
     * Resolves a row field by preferring its original spelling while retaining the existing
     * case-insensitive behavior when there is a single compatible field.
     */
    public static int resolveFieldIndex(RowType rowType, String fieldName)
    {
        requireNonNull(rowType, "rowType is null");
        requireNonNull(fieldName, "fieldName is null");

        List<RowType.Field> fields = rowType.getFields();

        int exactMatch = -1;
        for (int index = 0; index < fields.size(); index++) {
            if (fieldName.equals(fields.get(index).getName().orElse(null))) {
                if (exactMatch >= 0) {
                    throw ambiguousField(fieldName, rowType);
                }
                exactMatch = index;
            }
        }
        if (exactMatch >= 0) {
            return exactMatch;
        }

        int caseInsensitiveMatch = -1;
        for (int index = 0; index < fields.size(); index++) {
            if (fieldName.equalsIgnoreCase(fields.get(index).getName().orElse(null))) {
                if (caseInsensitiveMatch >= 0) {
                    throw ambiguousField(fieldName, rowType);
                }
                caseInsensitiveMatch = index;
            }
        }
        return caseInsensitiveMatch;
    }

    private static IllegalArgumentException ambiguousField(String fieldName, RowType rowType)
    {
        return new IllegalArgumentException(format("Ambiguous field '%s' in type %s", fieldName, rowType.getDisplayName()));
    }
}
