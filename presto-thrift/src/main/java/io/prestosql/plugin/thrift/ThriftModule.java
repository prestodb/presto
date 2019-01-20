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
package io.prestosql.plugin.thrift;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.drift.client.ExceptionClassification;
import io.airlift.drift.client.ExceptionClassification.HostStatus;
import io.prestosql.plugin.thrift.annotations.ForMetadataRefresh;
import io.prestosql.plugin.thrift.api.PrestoThriftService;
import io.prestosql.plugin.thrift.api.PrestoThriftServiceException;

import javax.inject.Singleton;

import java.util.Optional;
import java.util.concurrent.Executor;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.drift.client.ExceptionClassification.NORMAL_EXCEPTION;
import static io.airlift.drift.client.guice.DriftClientBinder.driftClientBinder;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class ThriftModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        driftClientBinder(binder)
                .bindDriftClient(PrestoThriftService.class)
                .withExceptionClassifier(t -> {
                    if (t instanceof PrestoThriftServiceException) {
                        boolean retryable = ((PrestoThriftServiceException) t).isRetryable();
                        return new ExceptionClassification(Optional.of(retryable), HostStatus.NORMAL);
                    }
                    return NORMAL_EXCEPTION;
                });

        binder.bind(ThriftConnector.class).in(Scopes.SINGLETON);
        binder.bind(ThriftMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ThriftSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ThriftPageSourceProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(ThriftConnectorConfig.class);
        binder.bind(ThriftSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(ThriftIndexProvider.class).in(Scopes.SINGLETON);
        binder.bind(ThriftConnectorStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ThriftConnectorStats.class).withGeneratedName();
    }

    @Provides
    @Singleton
    @ForMetadataRefresh
    public Executor createMetadataRefreshExecutor(ThriftConnectorConfig config)
    {
        return newFixedThreadPool(config.getMetadataRefreshThreads(), daemonThreadsNamed("metadata-refresh-%s"));
    }
}
