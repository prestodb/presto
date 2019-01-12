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

package io.prestosql.type.setdigest;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.prestosql.spi.function.OperatorType.CAST;

public final class SetDigestOperators
{
    private SetDigestOperators()
    {
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice castToBinary(@SqlType(SetDigestType.NAME) Slice slice)
    {
        return slice;
    }

    @ScalarOperator(CAST)
    @SqlType(SetDigestType.NAME)
    public static Slice castFromBinary(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return slice;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.HYPER_LOG_LOG)
    public static Slice castToHyperLogLog(@SqlType(SetDigestType.NAME) Slice slice)
    {
        checkArgument(slice.getByte(0) == 1, "Legacy version of SetDigest cannot cast to HyperLogLog");
        int hllLength = slice.getInt(SIZE_OF_BYTE);
        return Slices.wrappedBuffer(slice.getBytes(SIZE_OF_BYTE + SIZE_OF_INT, hllLength));
    }
}
