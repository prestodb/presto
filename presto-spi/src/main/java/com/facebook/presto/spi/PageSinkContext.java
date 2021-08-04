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
package com.facebook.presto.spi;

import com.facebook.presto.spi.connector.ConnectorMetadataUpdater;

import java.util.Optional;
import java.util.function.Supplier;

public class PageSinkContext
{
    private static final PageSinkContext DEFAULT_PAGE_SINK_CONTEXT = PageSinkContext.builder().build();

    private final boolean commitRequired;
    private final Optional<ConnectorMetadataUpdater> metadataUpdater;
    private Supplier<Boolean> isDriverDone;

    private PageSinkContext(boolean commitRequired,
                            Optional<ConnectorMetadataUpdater> metadataUpdater,
                            Supplier<Boolean> isDriverDone)
    {
        this.commitRequired = commitRequired;
        this.metadataUpdater = metadataUpdater;
        this.isDriverDone = isDriverDone;
    }

    public static PageSinkContext defaultContext()
    {
        return DEFAULT_PAGE_SINK_CONTEXT;
    }

    public boolean isCommitRequired()
    {
        return commitRequired;
    }

    public Optional<ConnectorMetadataUpdater> getMetadataUpdater()
    {
        return metadataUpdater;
    }

    public boolean isDriverDone()
    {
        return isDriverDone.get();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private boolean commitRequired;
        private Optional<ConnectorMetadataUpdater> metadataUpdater = Optional.empty();
        private Supplier<Boolean> isDriverDone;

        public Builder setCommitRequired(boolean commitRequired)
        {
            this.commitRequired = commitRequired;
            return this;
        }

        public Builder setConnectorMetadataUpdater(ConnectorMetadataUpdater metadataUpdater)
        {
            this.metadataUpdater = Optional.of(metadataUpdater);
            return this;
        }

        public Builder setDriverDone(Supplier<Boolean> isDriverDone)
        {
            this.isDriverDone = isDriverDone;
            return this;
        }

        public PageSinkContext build()
        {
            return new PageSinkContext(commitRequired, metadataUpdater, isDriverDone);
        }
    }
}
