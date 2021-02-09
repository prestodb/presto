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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.thrift.api.udf.ThriftUdfServiceException;
import com.facebook.presto.thrift.api.udf.UdfExecutionFailureInfo;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.firstNonNull;

public class ExceptionUtils
{
    private static final Pattern STACK_TRACE_PATTERN = Pattern.compile("(.*)\\.(.*)\\(([^:]*)(?::(.*))?\\)");

    private ExceptionUtils()
    {
    }

    public static PrestoException toPrestoException(ThriftUdfServiceException thriftException)
    {
        UdfExecutionFailure cause = toUdfExecutionException(thriftException.getFailureInfo());
        PrestoException prestoException = new PrestoException(
                thriftException::getErrorCode,
                firstNonNull(cause.getMessage(), "Error executing remote function"),
                cause);
        prestoException.addSuppressed(thriftException);
        return prestoException;
    }

    private static UdfExecutionFailure toUdfExecutionException(UdfExecutionFailureInfo failureInfo)
    {
        UdfExecutionFailure exception = new UdfExecutionFailure(
                failureInfo.getType(),
                failureInfo.getMessage(),
                failureInfo.getCause() == null ? null : toUdfExecutionException(failureInfo.getCause()));
        for (UdfExecutionFailureInfo suppressed : failureInfo.getSuppressed()) {
            exception.addSuppressed(toUdfExecutionException(suppressed));
        }
        exception.setStackTrace(failureInfo.getStack().stream()
                .map(ExceptionUtils::toStackTraceElement)
                .toArray(StackTraceElement[]::new));
        return exception;
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
