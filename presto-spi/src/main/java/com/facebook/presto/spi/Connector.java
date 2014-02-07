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

    /**
     * @throws UnsupportedOperationException if this connector doesn't support writing tables
     */
    ConnectorOutputHandleResolver getOutputHandleResolver();

    ConnectorMetadata getMetadata();
    ConnectorSplitManager getSplitManager();

    /**
     * @throws UnsupportedOperationException if this connector doesn't support reading tables
     */
    ConnectorRecordSetProvider getRecordSetProvider();

    /**
     * @throws UnsupportedOperationException if this connector doesn't support writing tables
     */
    ConnectorRecordSinkProvider getRecordSinkProvider();
    ConnectorIndexResolver getIndexResolver();
}
