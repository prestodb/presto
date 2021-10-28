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
package com.facebook.presto.functionNamespace.execution;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.thrift.api.udf.ThriftUdfErrorCodeSupplier;
import com.facebook.presto.thrift.api.udf.ThriftUdfServiceException;
import com.facebook.presto.thrift.api.udf.UdfExecutionFailureInfo;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;

public class ExceptionUtils
{
    private static final Pattern STACK_TRACE_PATTERN = Pattern.compile("(.*)\\.(.*)\\(([^:]*)(?::(.*))?\\)");

    private ExceptionUtils()
    {
    }

    public static PrestoException toPrestoException(ThriftUdfServiceException thriftException)
    {
        ThriftUdfServiceException exception = formatException(thriftException);
        PrestoException prestoException = new PrestoException(
                toThriftUdfErrorCodeSupplier(thriftException.getErrorCode()),
                exception);
        return prestoException;
    }

    private static ThriftUdfErrorCodeSupplier toThriftUdfErrorCodeSupplier(ErrorCode errorCode)
    {
        ErrorType errorType = errorCode.getType();
        switch (errorType) {
            case USER_ERROR:
                return ThriftUdfErrorCodeSupplier.THRIFT_UDF_USER_ERROR;
            case INSUFFICIENT_RESOURCES:
                return ThriftUdfErrorCodeSupplier.THRIFT_UDF_INSUFFICIENT_RESOURCES;
            case EXTERNAL:
            case INTERNAL_ERROR:
                return ThriftUdfErrorCodeSupplier.THRIFT_UDF_EXTERNAL_ERROR;
            default:
                throw new IllegalStateException(format("Unknown error code type %s", errorCode.getType()));
        }
    }

    private static ThriftUdfServiceException formatException(ThriftUdfServiceException exception)
    {
        UdfExecutionFailureInfo failureInfo = exception.getFailureInfo();
        for (UdfExecutionFailureInfo suppressed : failureInfo.getSuppressed()) {
            exception.addSuppressed(toThrowable(suppressed));
        }
        exception.setStackTrace(failureInfo.getStack().stream()
                .map(ExceptionUtils::toStackTraceElement)
                .toArray(StackTraceElement[]::new));
        return exception;
    }

    private static Throwable toThrowable(UdfExecutionFailureInfo failureInfo)
    {
        return new Throwable(format("%s: %s", failureInfo.getType(), failureInfo.getMessage()), failureInfo.getCause() == null ? null : toThrowable(failureInfo.getCause()));
    }

    private static StackTraceElement toStackTraceElement(String stack)
    {
        Matcher matcher = STACK_TRACE_PATTERN.matcher(stack);
        if (matcher.matches()) {
            String declaringClass = matcher.group(1);
            String methodName = matcher.group(2);
            String fileName = matcher.group(3);
            int number = -1;
            if (fileName.equals("Native Method")) {
                fileName = null;
                number = -2;
            }
            else if (matcher.group(4) != null) {
                number = Integer.parseInt(matcher.group(4));
            }
            return new StackTraceElement(declaringClass, methodName, fileName, number);
        }
        return new StackTraceElement("Unknown", stack, null, -1);
    }
}
