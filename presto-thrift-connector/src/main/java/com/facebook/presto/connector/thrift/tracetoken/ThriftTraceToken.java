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
package com.facebook.presto.connector.thrift.tracetoken;

import com.google.common.base.Strings;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class ThriftTraceToken
{
    private final String value;

    public ThriftTraceToken(String value)
    {
        requireNonNull(value, "value is null");
        checkArgument(Strings.isNullOrEmpty(value), "value is null or empty");
        checkArgument(value.length() <= 22, "value must have <= 22 characters");
        this.value = value;
    }

    public String getValue()
    {
        return value;
    }

    @Nullable
    public static ThriftTraceToken valueOf(String value)
    {
        return value == null ? null : new ThriftTraceToken(value);
    }
}
