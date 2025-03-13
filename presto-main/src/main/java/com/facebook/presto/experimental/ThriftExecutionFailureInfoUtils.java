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
package com.facebook.presto.experimental;

import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.experimental.auto_gen.ThriftExecutionFailureInfo;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.experimental.ThriftErrorCauseUtils.fromErrorCause;
import static com.facebook.presto.experimental.ThriftErrorCauseUtils.toErrorCause;
import static com.facebook.presto.experimental.ThriftErrorCodeUtils.fromErrorCode;
import static com.facebook.presto.experimental.ThriftErrorCodeUtils.toErrorCode;
import static com.facebook.presto.experimental.ThriftErrorLocationUtils.fromErrorLocation;
import static com.facebook.presto.experimental.ThriftErrorLocationUtils.toErrorLocation;
import static com.facebook.presto.experimental.ThriftHostAddressUtils.fromHostAddress;
import static com.facebook.presto.experimental.ThriftHostAddressUtils.toHostAddress;

public class ThriftExecutionFailureInfoUtils
{
    private ThriftExecutionFailureInfoUtils() {}

    public static ExecutionFailureInfo toExecutionFailureInfo(ThriftExecutionFailureInfo failureInfo)
    {
        if (failureInfo == null) {
            return null;
        }
        return new ExecutionFailureInfo(failureInfo.getType(),
                failureInfo.getMessage(),
                toExecutionFailureInfo(failureInfo.getCause()),
                failureInfo.getSuppressed().stream().map(ThriftExecutionFailureInfoUtils::toExecutionFailureInfo).collect(Collectors.toList()),
                failureInfo.getStack(),
                toErrorLocation(failureInfo.getErrorLocation()),
                toErrorCode(failureInfo.getErrorCode()),
                toHostAddress(failureInfo.getRemoteHost()),
                toErrorCause(failureInfo.getErrorCause()));
    }

    public static ThriftExecutionFailureInfo fromExecutionFailureInfo(ExecutionFailureInfo executionFailureInfo)
    {
        if (executionFailureInfo == null) {
            return null;
        }
        List<ThriftExecutionFailureInfo> suppressed = executionFailureInfo.getSuppressed().stream().map(ThriftExecutionFailureInfoUtils::fromExecutionFailureInfo).collect(Collectors.toList());

        return new ThriftExecutionFailureInfo(
                executionFailureInfo.getType(),
                executionFailureInfo.getMessage(),
                suppressed,
                executionFailureInfo.getStack(),
                fromErrorLocation(executionFailureInfo.getErrorLocation()),
                fromErrorCode(executionFailureInfo.getErrorCode()),
                fromHostAddress(executionFailureInfo.getRemoteHost()),
                fromErrorCause(executionFailureInfo.getErrorCause()));
    }
}
