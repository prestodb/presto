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

import com.facebook.presto.spi.TableHandle;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public class TableHandleJacksonModule
        extends AbstractTypedJacksonModule<TableHandle>
{
    @Inject
    public TableHandleJacksonModule(HandleResolver handleResolver)
    {
        super(TableHandle.class, "type", new TableHandleJsonTypeIdResolver(handleResolver));
    }

    private static class TableHandleJsonTypeIdResolver
            implements JsonTypeIdResolver<TableHandle>
    {
        private final HandleResolver handleResolver;

        private TableHandleJsonTypeIdResolver(HandleResolver HandleResolver)
        {
            this.handleResolver = checkNotNull(HandleResolver, "handleIdResolvers is null");
        }

        @Override
        public String getId(TableHandle tableHandle)
        {
            return handleResolver.getId(tableHandle);
        }

        @Override
        public Class<? extends TableHandle> getType(String id)
        {
            return handleResolver.getTableHandleClass(id);
        }
    }
}
