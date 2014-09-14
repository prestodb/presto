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

public abstract class ReadOnlySafeConnector<
        TH extends ConnectorTableHandle,
        CH extends ConnectorColumnHandle,
        S extends ConnectorSplit,
        IH extends ConnectorIndexHandle,
        P extends ConnectorPartition>
        implements SafeConnector<TH, CH, S, IH, ConnectorOutputTableHandle, ConnectorInsertTableHandle, P>
{
    @Override
    public abstract ReadOnlySafeMetadata<TH, CH> getMetadata();

    @Override
    public SafeRecordSinkProvider<ConnectorOutputTableHandle, ConnectorInsertTableHandle> getRecordSinkProvider()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SafeIndexResolver<TH, CH, IH> getIndexResolver()
    {
        throw new UnsupportedOperationException();
    }
}
