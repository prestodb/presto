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
package com.facebook.presto.spark.classloader_interface;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class PrestoSparkFailure
        extends RuntimeException
{
    private final String type;
    private final String errorCode;
    private final List<ExecutionStrategy> retryExecutionStrategies;

    public PrestoSparkFailure(String message, Throwable cause, String type, String errorCode, List<ExecutionStrategy> retryExecutionStrategies)
    {
        super(message, cause);
        this.type = requireNonNull(type, "type is null");
        this.errorCode = requireNonNull(errorCode, "errorCode is null");
        this.retryExecutionStrategies = requireNonNull(retryExecutionStrategies, "retryExecutionStrategies is null");
    }

    public String getType()
    {
        return type;
    }

    public String getErrorCode()
    {
        return errorCode;
    }

    public List<ExecutionStrategy> getRetryExecutionStrategies()
    {
        return retryExecutionStrategies;
    }

    @Override
    public String toString()
    {
        String message = getMessage();
        if (message != null) {
            return type + ": " + message;
        }
        return type;
    }
}
