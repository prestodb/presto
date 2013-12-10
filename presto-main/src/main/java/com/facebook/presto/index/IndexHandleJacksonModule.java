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
package com.facebook.presto.index;

import com.facebook.presto.metadata.AbstractTypedJacksonModule;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.JsonTypeIdResolver;
import com.facebook.presto.spi.IndexHandle;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public class IndexHandleJacksonModule
        extends AbstractTypedJacksonModule<IndexHandle>
{
    @Inject
    public IndexHandleJacksonModule(HandleResolver handleResolver)
    {
        super(IndexHandle.class, "type", new IndexHandleJsonTypeIdResolver(handleResolver));
    }

    private static class IndexHandleJsonTypeIdResolver
            implements JsonTypeIdResolver<IndexHandle>
    {
        private final HandleResolver handleResolver;

        private IndexHandleJsonTypeIdResolver(HandleResolver handleResolver)
        {
            this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
        }

        @Override
        public String getId(IndexHandle indexHandle)
        {
            return handleResolver.getId(indexHandle);
        }

        @Override
        public Class<? extends IndexHandle> getType(String id)
        {
            return handleResolver.getIndexHandleClass(id);
        }
    }
}
