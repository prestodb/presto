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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.ConnectorOutputHandleResolver;
import com.facebook.presto.spi.OutputTableHandle;

import javax.inject.Inject;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class OutputTableHandleResolver
{
    private final ConcurrentMap<String, ConnectorOutputHandleResolver> handleIdResolvers = new ConcurrentHashMap<>();

    @Inject
    public OutputTableHandleResolver(Map<String, ConnectorOutputHandleResolver> handleIdResolvers)
    {
        this.handleIdResolvers.putAll(handleIdResolvers);
    }

    public void addHandleResolver(String id, ConnectorOutputHandleResolver resolver)
    {
        ConnectorOutputHandleResolver existingResolver = handleIdResolvers.putIfAbsent(id, resolver);
        checkState(existingResolver == null, "Id %s is already assigned to resolver %s", id, existingResolver);
    }

    public String getId(OutputTableHandle tableHandle)
    {
        for (Entry<String, ConnectorOutputHandleResolver> entry : handleIdResolvers.entrySet()) {
            if (entry.getValue().canHandle(tableHandle)) {
                return entry.getKey();
            }
        }
        throw new IllegalArgumentException("No connector for output table handle: " + tableHandle);
    }

    public Class<? extends OutputTableHandle> getOutputTableHandleClass(String id)
    {
        ConnectorOutputHandleResolver resolver = handleIdResolvers.get(id);
        checkArgument(resolver != null, "No handle resolver for %s", id);
        return resolver.getOutputTableHandleClass();
    }
}
