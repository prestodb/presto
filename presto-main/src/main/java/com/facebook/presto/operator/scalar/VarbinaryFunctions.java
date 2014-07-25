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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.operator.Description;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.SqlType;
import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public final class VarbinaryFunctions
{
    private VarbinaryFunctions() {}

    @Description("length of the given binary")
    @ScalarFunction
    @SqlType(BigintType.class)
    public static long length(@SqlType(VarbinaryType.class) Slice slice)
    {
        return slice.length();
    }

    @Description("encode binary data as base64")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice toBase64(@SqlType(VarbinaryType.class) Slice slice)
    {
        return Slices.utf8Slice(BaseEncoding.base64().encode(slice.getBytes()));
    }

    @Description("decode base64 encoded binary data")
    @ScalarFunction("from_base64")
    @SqlType(VarbinaryType.class)
    public static Slice fromBase64Varchar(@SqlType(VarcharType.class) Slice slice)
    {
        return Slices.wrappedBuffer(BaseEncoding.base64().decode(slice.toStringUtf8()));
    }

    @Description("decode base64 encoded binary data")
    @ScalarFunction("from_base64")
    @SqlType(VarbinaryType.class)
    public static Slice fromBase64Varbinary(@SqlType(VarbinaryType.class) Slice slice)
    {
        return Slices.wrappedBuffer(BaseEncoding.base64().decode(slice.toStringUtf8()));
    }

    @Description("encode binary data as base64 using the URL safe alphabet")
    @ScalarFunction("to_base64url")
    @SqlType(VarcharType.class)
    public static Slice toBase64Url(@SqlType(VarbinaryType.class) Slice slice)
    {
        return Slices.utf8Slice(BaseEncoding.base64Url().encode(slice.getBytes()));
    }

    @Description("decode URL safe base64 encoded binary data")
    @ScalarFunction("from_base64url")
    @SqlType(VarbinaryType.class)
    public static Slice fromBase64UrlVarchar(@SqlType(VarcharType.class) Slice slice)
    {
        return Slices.wrappedBuffer(BaseEncoding.base64Url().decode(slice.toStringUtf8()));
    }

    @Description("decode URL safe base64 encoded binary data")
    @ScalarFunction("from_base64url")
    @SqlType(VarbinaryType.class)
    public static Slice fromBase64UrlVarbinary(@SqlType(VarbinaryType.class) Slice slice)
    {
        return Slices.wrappedBuffer(BaseEncoding.base64Url().decode(slice.toStringUtf8()));
    }

    @Description("encode binary data as hex")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice toHex(@SqlType(VarbinaryType.class) Slice slice)
    {
        return Slices.utf8Slice(BaseEncoding.base16().encode(slice.getBytes()));
    }

    @Description("decode hex encoded binary data")
    @ScalarFunction("from_hex")
    @SqlType(VarbinaryType.class)
    public static Slice fromHexVarchar(@SqlType(VarcharType.class) Slice slice)
    {
        return Slices.wrappedBuffer(BaseEncoding.base16().decode(slice.toStringUtf8()));
    }

    @Description("decode hex encoded binary data")
    @ScalarFunction("from_hex")
    @SqlType(VarbinaryType.class)
    public static Slice fromHexVarbinary(@SqlType(VarbinaryType.class) Slice slice)
    {
        return Slices.wrappedBuffer(BaseEncoding.base16().decode(slice.toStringUtf8()));
    }
}
