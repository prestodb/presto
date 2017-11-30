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
import com.facebook.presto.connector.thrift.annotations.ForRetryDriver;
import com.facebook.presto.connector.thrift.annotations.NonRetrying;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.connector.thrift.clientproviders.DefaultPrestoThriftServiceProvider;
import com.facebook.presto.connector.thrift.clientproviders.PrestoThriftServiceProvider;
import com.facebook.presto.connector.thrift.clientproviders.RetryingPrestoThriftServiceProvider;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import javax.inject.Singleton;

import java.util.concurrent.Executor;

import static com.facebook.swift.service.guice.ThriftClientBinder.thriftClientBinder;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class ThriftModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(ThriftConnector.class).in(Scopes.SINGLETON);
        thriftClientBinder(binder).bindThriftClient(PrestoThriftService.class);
        binder.bind(ThriftMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ThriftSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ThriftPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(PrestoThriftServiceProvider.class).to(RetryingPrestoThriftServiceProvider.class).in(Scopes.SINGLETON);
        binder.bind(PrestoThriftServiceProvider.class).annotatedWith(NonRetrying.class).to(DefaultPrestoThriftServiceProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(ThriftConnectorConfig.class);
        binder.bind(ThriftSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(ThriftIndexProvider.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForMetadataRefresh
    public Executor createMetadataRefreshExecutor(ThriftConnectorConfig config)
    {
        return newFixedThreadPool(config.getMetadataRefreshThreads(), daemonThreadsNamed("metadata-refresh-%s"));
    }

    @Provides
    @Singleton
    @ForRetryDriver
    public ListeningScheduledExecutorService createRetryDriverScheduledExecutor(ThriftConnectorConfig config)
    {
        return listeningDecorator(newScheduledThreadPool(config.getRetryDriverThreads(), threadsNamed("thrift-retry-driver-%s")));
    }
}
