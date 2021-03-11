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
import com.facebook.presto.spi.ErrorCode;

import java.util.Arrays;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThriftStruct("UdfServiceException")
public final class ThriftUdfServiceException
        extends Exception
{
    private final boolean retryable;
    private final ErrorCode errorCode;
    private final UdfExecutionFailureInfo failureInfo;

    @ThriftConstructor
    public ThriftUdfServiceException(boolean retryable, ErrorCode errorCode, UdfExecutionFailureInfo failureInfo)
    {
        super(failureInfo.getMessage());
        this.retryable = retryable;
        this.errorCode = requireNonNull(errorCode, "errorCode is null");
        this.failureInfo = requireNonNull(failureInfo, "failureInfo is null");
    }

    @Override
    @ThriftField(1)
    public String getMessage()
    {
        return format("ThriftUdfServiceException(%s, %s): %s", errorCode, retryable ? "RETRYABLE" : "NON-RETRYABLE", super.getMessage());
    }

    @ThriftField(2)
    public boolean isRetryable()
    {
        return retryable;
    }

    @ThriftField(3)
    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    @ThriftField(4)
    public UdfExecutionFailureInfo getFailureInfo()
    {
        return failureInfo;
    }

    public static ThriftUdfServiceException toThriftUdfServiceException(boolean retryable, ErrorCode errorCode, Throwable throwable)
    {
        return new ThriftUdfServiceException(retryable, errorCode, toFailureInfo(throwable));
    }

    private static UdfExecutionFailureInfo toFailureInfo(Throwable throwable)
    {
        Class<?> clazz = throwable.getClass();
        String type = firstNonNull(clazz.getCanonicalName(), clazz.getName());

        UdfExecutionFailureInfo cause = throwable.getCause() == null ? null : toFailureInfo(throwable.getCause());

        return new UdfExecutionFailureInfo(
                type,
                firstNonNull(throwable.getMessage(), ""),
                cause,
                Arrays.stream(throwable.getSuppressed())
                        .map(ThriftUdfServiceException::toFailureInfo)
                        .collect(toImmutableList()),
                Arrays.stream(throwable.getStackTrace())
                        .map(StackTraceElement::toString)
                        .collect(toImmutableList()));
    }
}
