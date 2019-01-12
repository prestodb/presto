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
package io.prestosql.parquet.reader;

import io.prestosql.parquet.Field;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;

import static io.prestosql.parquet.ParquetTypeUtils.isValueNull;

public class StructColumnReader
{
    private StructColumnReader()
    {
    }

    /**
     * Each struct has three variants of presence:
     * 1) Struct is not defined, because one of it's optional parent fields is null
     * 2) Struct is null
     * 3) Struct is defined and not empty.
     */
    public static BooleanList calculateStructOffsets(
            Field field,
            int[] fieldDefinitionLevels,
            int[] fieldRepetitionLevels)
    {
        int maxDefinitionLevel = field.getDefinitionLevel();
        int maxRepetitionLevel = field.getRepetitionLevel();
        BooleanList structIsNull = new BooleanArrayList();
        boolean required = field.isRequired();
        if (fieldDefinitionLevels == null) {
            return structIsNull;
        }
        for (int i = 0; i < fieldDefinitionLevels.length; i++) {
            if (fieldRepetitionLevels[i] <= maxRepetitionLevel) {
                if (isValueNull(required, fieldDefinitionLevels[i], maxDefinitionLevel)) {
                    // Struct is null
                    structIsNull.add(true);
                }
                else if (fieldDefinitionLevels[i] >= maxDefinitionLevel) {
                    // Struct is defined and not empty
                    structIsNull.add(false);
                }
            }
        }
        return structIsNull;
    }
}
