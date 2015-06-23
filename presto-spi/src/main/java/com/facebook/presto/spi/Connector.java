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

import com.facebook.presto.spi.session.SessionPropertyMetadata;

import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

public interface Connector
{
    ConnectorHandleResolver getHandleResolver();

    ConnectorMetadata getMetadata();

    ConnectorSplitManager getSplitManager();

    /**
     * @throws UnsupportedOperationException if this connector does not support reading tables page at a time
     */
    default ConnectorPageSourceProvider getPageSourceProvider()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support reading tables record at a time
     */
    default ConnectorRecordSetProvider getRecordSetProvider()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support writing tables page at a time
     */
    default ConnectorPageSinkProvider getPageSinkProvider()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support writing tables record at a time
     */
    default ConnectorRecordSinkProvider getRecordSinkProvider()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support indexes
     */
    default ConnectorIndexResolver getIndexResolver()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @return the set of system tables provided by this connector
     */
    default Set<SystemTable> getSystemTables()
    {
        return emptySet();
    }

    /**
     * @return the system properties for this connector
     */
    default List<SessionPropertyMetadata<?>> getSessionProperties()
    {
        return emptyList();
    }

    /**
     * Shutdown the connector by releasing any held resources such as
     * threads, sockets, etc. This method will only be called when no
     * queries are using the connector. After this method is called,
     * no methods will be called on the connector or any objects that
     * have been returned from the connector.
     */
    default void shutdown() {}
}
