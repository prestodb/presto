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
package io.prestosql.operator.scalar;

import com.google.common.hash.Hashing;
import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.airlift.slice.Slices.wrappedBuffer;

public final class HmacFunctions
{
    private HmacFunctions() {}

    @Description("Compute HMAC with MD5")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice hmacMd5(@SqlType(StandardTypes.VARBINARY) Slice slice, @SqlType(StandardTypes.VARBINARY) Slice key)
    {
        return wrappedBuffer(Hashing.hmacMd5(key.getBytes()).hashBytes(slice.getBytes()).asBytes());
    }

    @Description("Compute HMAC with SHA1")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice hmacSha1(@SqlType(StandardTypes.VARBINARY) Slice slice, @SqlType(StandardTypes.VARBINARY) Slice key)
    {
        return wrappedBuffer(Hashing.hmacSha1(key.getBytes()).hashBytes(slice.getBytes()).asBytes());
    }

    @Description("Compute HMAC with SHA256")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice hmacSha256(@SqlType(StandardTypes.VARBINARY) Slice slice, @SqlType(StandardTypes.VARBINARY) Slice key)
    {
        return wrappedBuffer(Hashing.hmacSha256(key.getBytes()).hashBytes(slice.getBytes()).asBytes());
    }

    @Description("Compute HMAC with SHA512")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice hmacSha512(@SqlType(StandardTypes.VARBINARY) Slice slice, @SqlType(StandardTypes.VARBINARY) Slice key)
    {
        return wrappedBuffer(Hashing.hmacSha512(key.getBytes()).hashBytes(slice.getBytes()).asBytes());
    }
}
