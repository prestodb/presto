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

public interface Connector
{
    ConnectorHandleResolver getHandleResolver();

    ConnectorMetadata getMetadata();

    ConnectorSplitManager getSplitManager();

    /**
     * @throws UnsupportedOperationException if this connector does not support reading tables page at a time
     */
    ConnectorPageSourceProvider getPageSourceProvider();

    /**
     * @throws UnsupportedOperationException if this connector does not support reading tables record at a time
     */
    ConnectorRecordSetProvider getRecordSetProvider();

    /**
     * @throws UnsupportedOperationException if this connector does not support writing tables
     */
    ConnectorRecordSinkProvider getRecordSinkProvider();

    ConnectorIndexResolver getIndexResolver();

    default ConnectorPageSinkProvider getPageSinkProvider()
    {
        throw new UnsupportedOperationException();
    }
}
