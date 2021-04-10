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
package com.facebook.presto.verifier.resolver;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.verifier.checksum.ArrayColumnChecksum;
import com.facebook.presto.verifier.checksum.ColumnMatchResult;
import com.facebook.presto.verifier.checksum.MapColumnChecksum;
import com.facebook.presto.verifier.checksum.StructureColumnChecksum;
import com.facebook.presto.verifier.framework.DataMatchResult;
import com.facebook.presto.verifier.framework.QueryBundle;

import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.common.type.RowType.Field;
import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.COLUMN_MISMATCH;
import static com.google.common.base.Preconditions.checkArgument;

public class StructuredColumnMismatchResolver
        implements FailureResolver
{
    public static final String NAME = "structured-column";

    @Override
    public Optional<String> resolveResultMismatch(DataMatchResult matchResult, QueryBundle control)
    {
        checkArgument(!matchResult.isMatched(), "Expect not matched");
        if (matchResult.getMatchType() != COLUMN_MISMATCH) {
            return Optional.empty();
        }

        for (ColumnMatchResult<?> mismatchedColumn : matchResult.getMismatchedColumns()) {
            Type columnType = mismatchedColumn.getColumn().getType();

            if (columnType instanceof ArrayType) {
                ArrayColumnChecksum controlChecksum = (ArrayColumnChecksum) mismatchedColumn.getControlChecksum();
                ArrayColumnChecksum testChecksum = (ArrayColumnChecksum) mismatchedColumn.getTestChecksum();

                if (!containsFloatingPointType(((ArrayType) columnType).getElementType())
                        || !isCardinalityMatched(controlChecksum, testChecksum)) {
                    return Optional.empty();
                }
            }
            else if (columnType instanceof MapType) {
                MapColumnChecksum controlChecksum = (MapColumnChecksum) mismatchedColumn.getControlChecksum();
                MapColumnChecksum testChecksum = (MapColumnChecksum) mismatchedColumn.getTestChecksum();

                if (!isCardinalityMatched(controlChecksum, testChecksum)) {
                    return Optional.empty();
                }

                boolean keyContainsFloatingPoint = containsFloatingPointType(((MapType) columnType).getKeyType());
                boolean valueContainsFloatingPoint = containsFloatingPointType(((MapType) columnType).getValueType());
                if (!keyContainsFloatingPoint && !valueContainsFloatingPoint) {
                    return Optional.empty();
                }
                if (!keyContainsFloatingPoint &&
                        !Objects.equals(controlChecksum.getKeysChecksum(), testChecksum.getKeysChecksum())) {
                    return Optional.empty();
                }
                if (!valueContainsFloatingPoint &&
                        !Objects.equals(controlChecksum.getValuesChecksum(), testChecksum.getValuesChecksum())) {
                    return Optional.empty();
                }
            }
            else {
                return Optional.empty();
            }
        }

        return Optional.of("Structured columns auto-resolved");
    }

    private static boolean containsFloatingPointType(Type type)
    {
        if (type instanceof DoubleType || type instanceof RealType) {
            return true;
        }
        if (type instanceof ArrayType) {
            return containsFloatingPointType(((ArrayType) type).getElementType());
        }
        if (type instanceof MapType) {
            return containsFloatingPointType(((MapType) type).getKeyType()) || containsFloatingPointType(((MapType) type).getValueType());
        }
        if (type instanceof RowType) {
            for (Field field : ((RowType) type).getFields()) {
                if (containsFloatingPointType(field.getType())) {
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    private static <T extends StructureColumnChecksum> boolean isCardinalityMatched(T controlChecksum, T testChecksum)
    {
        return Objects.equals(controlChecksum.getCardinalityChecksum(), testChecksum.getCardinalityChecksum())
                && Objects.equals(controlChecksum.getCardinalitySum(), testChecksum.getCardinalitySum());
    }
}
