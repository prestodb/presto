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
package com.facebook.presto.statistics;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.function.OperatorType.CAST;

/**
 * Casts between {@link GaussianType} and {@code varbinary}, which is how a fitted
 * distribution is written to a table: no storage format has a Gaussian type, so the
 * serialized form is stored as binary and cast back on the way out.
 * <p>
 * Both directions are free, because the varbinary is the serialized Gaussian. A cast from
 * varbinary is therefore unchecked, and a value that was not produced by this type fails
 * when something first reads it rather than at the cast.
 */
public final class GaussianOperators
{
    private GaussianOperators() {}

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice castToVarbinary(@SqlType(GaussianType.NAME) Slice slice)
    {
        return slice;
    }

    @ScalarOperator(CAST)
    @SqlType(GaussianType.NAME)
    public static Slice castFromVarbinary(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return slice;
    }
}
