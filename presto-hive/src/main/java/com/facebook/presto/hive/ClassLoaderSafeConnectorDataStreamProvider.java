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
package com.facebook.presto.hive;

import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.split.ConnectorDataStreamProvider;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class ClassLoaderSafeConnectorDataStreamProvider
        implements ConnectorDataStreamProvider
{
    private final ConnectorDataStreamProvider delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeConnectorDataStreamProvider(ConnectorDataStreamProvider delegate, ClassLoader classLoader)
    {
        this.delegate = checkNotNull(delegate, "delegate is null");
        this.classLoader = checkNotNull(classLoader, "classLoader is null");
    }

    @Override
    public Operator createNewDataStream(OperatorContext operatorContext, ConnectorSplit split, List<ConnectorColumnHandle> columns)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.createNewDataStream(operatorContext, split, columns);
        }
    }

    @Override
    public String toString()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.toString();
        }
    }
}
