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

import com.facebook.presto.spi.ConnectorTableHandle;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class TableHandleJacksonModule
        extends AbstractTypedJacksonModule<ConnectorTableHandle>
{
    @Inject
    public TableHandleJacksonModule(HandleResolver handleResolver)
    {
        super(ConnectorTableHandle.class, new TableHandleJsonTypeIdResolver(handleResolver));
    }

    private static class TableHandleJsonTypeIdResolver
            implements JsonTypeIdResolver<ConnectorTableHandle>
    {
        private final HandleResolver handleResolver;

        private TableHandleJsonTypeIdResolver(HandleResolver handleResolver)
        {
            this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        }

        @Override
        public String getId(ConnectorTableHandle tableHandle)
        {
            return handleResolver.getId(tableHandle);
        }

        @Override
        public Class<? extends ConnectorTableHandle> getType(String id)
        {
            return handleResolver.getTableHandleClass(id);
        }
    }
}
