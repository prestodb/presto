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

import com.facebook.presto.spi.ConnectorSplit;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public class SplitJacksonModule
        extends AbstractTypedJacksonModule<ConnectorSplit>
{
    @Inject
    public SplitJacksonModule(HandleResolver handleResolver)
    {
        super(ConnectorSplit.class, "type", new SplitJsonTypeIdResolver(handleResolver));
    }

    private static class SplitJsonTypeIdResolver
            implements JsonTypeIdResolver<ConnectorSplit>
    {
        private final HandleResolver handleResolver;

        private SplitJsonTypeIdResolver(HandleResolver handleResolver)
        {
            this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
        }

        @Override
        public String getId(ConnectorSplit split)
        {
            return handleResolver.getId(split);
        }

        @Override
        public Class<? extends ConnectorSplit> getType(String id)
        {
            return handleResolver.getSplitClass(id);
        }
    }
}
