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

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.presto.spi.connector.ConnectorMetadataUpdater;
import com.facebook.presto.spi.connector.ConnectorMetadataUpdaterProvider;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;

public class HiveMetadataUpdaterProvider
        implements ConnectorMetadataUpdaterProvider
{
    private final ExecutorService executorService;
    private final Executor boundedExecutor;

    @Inject
    public HiveMetadataUpdaterProvider(
            @ForUpdatingHiveMetadata ExecutorService executorService,
            HiveClientConfig hiveClientConfig)
    {
        this.executorService = requireNonNull(executorService, "executorService is null");
        int maxMetadataUpdaterThreads = requireNonNull(hiveClientConfig, "hiveClientConfig is null").getMaxMetadataUpdaterThreads();
        this.boundedExecutor = new BoundedExecutor(executorService, maxMetadataUpdaterThreads);
    }

    @Override
    public ConnectorMetadataUpdater getMetadataUpdater()
    {
        return new HiveMetadataUpdater(boundedExecutor);
    }

    @PreDestroy
    public void stop()
    {
        executorService.shutdownNow();
    }
}
