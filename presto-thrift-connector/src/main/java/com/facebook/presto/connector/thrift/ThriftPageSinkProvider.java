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

import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.airlift.drift.client.DriftClient;
import io.airlift.drift.client.address.AddressSelector;
import io.airlift.drift.client.guice.DefaultClient;
import io.airlift.drift.client.guice.DriftClientAnnotationFactory;
import io.airlift.drift.transport.client.Address;

import static java.util.Objects.requireNonNull;

public class ThriftPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final DriftClient<PrestoThriftService> client;
    private final AddressSelector<Address> addressSelector;
    private final ThriftHeaderProvider thriftHeaderProvider;

    @Inject
    public ThriftPageSinkProvider(DriftClient<PrestoThriftService> client, Injector injector, ThriftHeaderProvider thriftHeaderProvider)
    {
        this.client = requireNonNull(client, "client is null");
        requireNonNull(injector, "injector is null");
        this.addressSelector = injector.getInstance(Key.get(AddressSelector.class, DriftClientAnnotationFactory.getDriftClientAnnotation(PrestoThriftService.class, DefaultClient.class)));
        this.thriftHeaderProvider = requireNonNull(thriftHeaderProvider, "thriftHeaderProvider is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        // the method called when calling the 'CREATE TABLE' command.
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        // the method called when calling the 'INSERT INTO' command
        return new ThriftPageSink(client, addressSelector, thriftHeaderProvider.getHeaders(session), (ThriftInsertTableHandle) insertTableHandle);
    }
}
