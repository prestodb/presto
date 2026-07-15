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

        int exactMatch = -1;
        boolean exactMatchIsAmbiguous = false;
        int caseInsensitiveMatch = -1;
        boolean caseInsensitiveMatchIsAmbiguous = false;

        List<RowType.Field> fields = rowType.getFields();
        for (int index = 0; index < fields.size(); index++) {
            String candidateName = fields.get(index).getName().orElse(null);
            if (candidateName == null || !fieldName.equalsIgnoreCase(candidateName)) {
                continue;
            }

            if (caseInsensitiveMatch >= 0) {
                caseInsensitiveMatchIsAmbiguous = true;
            }
            else {
                caseInsensitiveMatch = index;
            }

            if (fieldName.equals(candidateName)) {
                if (exactMatch >= 0) {
                    exactMatchIsAmbiguous = true;
                }
                else {
                    exactMatch = index;
                }
            }
        }

        if (exactMatchIsAmbiguous || (exactMatch < 0 && caseInsensitiveMatchIsAmbiguous)) {
            throw new IllegalArgumentException(format("Ambiguous field '%s' in type %s", fieldName, rowType.getDisplayName()));
        }
        if (exactMatch >= 0) {
            return exactMatch;
        }
        return caseInsensitiveMatch;
    }
}
