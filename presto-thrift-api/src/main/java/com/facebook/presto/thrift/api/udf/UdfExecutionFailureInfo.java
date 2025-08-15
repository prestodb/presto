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
package com.facebook.presto.thrift.api.udf;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.facebook.drift.annotations.ThriftField.Recursiveness.TRUE;
import static com.facebook.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static java.util.Objects.requireNonNull;

@Immutable
@ThriftStruct
public class UdfExecutionFailureInfo
{
    private final String type;
    private final String message;
    private final UdfExecutionFailureInfo cause;
    private final List<UdfExecutionFailureInfo> suppressed;
    private final List<String> stack;

    @ThriftConstructor
    public UdfExecutionFailureInfo(
            String type,
            String message,
            @Nullable UdfExecutionFailureInfo cause,
            List<UdfExecutionFailureInfo> suppressed,
            List<String> stack)
    {
        this.type = requireNonNull(type, "type is null");
        this.message = requireNonNull(message, "message is null");
        this.cause = cause;
        this.suppressed = ImmutableList.copyOf(suppressed);
        this.stack = ImmutableList.copyOf(stack);
    }

    @ThriftField(1)
    public String getType()
    {
        return type;
    }

    @Nullable
    @ThriftField(2)
    public String getMessage()
    {
        return message;
    }

    @Nullable
    @ThriftField(value = 3, isRecursive = TRUE, requiredness = OPTIONAL)
    public UdfExecutionFailureInfo getCause()
    {
        return cause;
    }

    @ThriftField(4)
    public List<UdfExecutionFailureInfo> getSuppressed()
    {
        return suppressed;
    }

    @ThriftField(5)
    public List<String> getStack()
    {
        return stack;
    }
}
