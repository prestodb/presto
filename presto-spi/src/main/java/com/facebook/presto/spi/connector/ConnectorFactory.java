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
package com.facebook.presto.spi.connector;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.function.TableFunctionHandleResolver;
import com.facebook.presto.spi.function.TableFunctionSplitResolver;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.facebook.presto.spi.function.table.TableFunctionProcessorProvider;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public interface ConnectorFactory
{
    String getName();

    ConnectorHandleResolver getHandleResolver();

    Connector create(String catalogName, Map<String, String> config, ConnectorContext context);

    default Function<ConnectorTableFunctionHandle, TableFunctionProcessorProvider> getTableFunctionProcessorProvider()
    {
        return null;
    }

    default Optional<TableFunctionHandleResolver> getTableFunctionHandleResolver()
    {
        return Optional.empty();
    }

    default Optional<TableFunctionSplitResolver> getTableFunctionSplitResolver()
    {
        return Optional.empty();
    }
}
