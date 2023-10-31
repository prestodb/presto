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
package com.facebook.presto.spark;

import com.facebook.presto.spark.classloader_interface.ExecutionStrategy;
import com.facebook.presto.spi.PrestoException;

import java.util.Arrays;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static java.lang.String.format;

public class ExecutionStrategyValidator
        implements Consumer<String>
{
    private static final Set<String> validStrategies = Arrays.stream(ExecutionStrategy.values())
            .map(strategy -> strategy.name())
            .collect(Collectors.toSet());

    @Override
    public void accept(String strategy)
    {
        if (!validStrategies.contains(strategy)) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, format("Invalid value for execution strategy [%s]. Valid values: %s", strategy, String.join(",", validStrategies)));
        }
    }
}
