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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.connector.thrift.annotations.ForMetadataRefresh;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.connector.thrift.api.PrestoThriftServiceException;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.drift.client.ExceptionClassification;
import io.airlift.drift.client.ExceptionClassification.HostStatus;

import javax.inject.Singleton;

import java.util.Optional;
import java.util.concurrent.Executor;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.drift.client.ExceptionClassification.NORMAL_EXCEPTION;
import static io.airlift.drift.client.guice.DriftClientBinder.driftClientBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class ThriftModule
        implements Module
{
    private final String connectorId;

    public ThriftModule(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

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
        newExporter(binder).export(ThriftConnectorStats.class)
                .as(generatedNameOf(ThriftConnectorStats.class, connectorId));
    }

    @Provides
    @Singleton
    @ForMetadataRefresh
    public Executor createMetadataRefreshExecutor(ThriftConnectorConfig config)
    {
        return newFixedThreadPool(config.getMetadataRefreshThreads(), daemonThreadsNamed("metadata-refresh-%s"));
    }
}
