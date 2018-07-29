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
package com.facebook.presto.pulsar;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PulsarHandleResolver implements ConnectorHandleResolver {
    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass() {
        return PulsarTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
        return PulsarTableLayoutHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass() {
        return PulsarColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass() {
        return PulsarSplit.class;
    }

    static PulsarTableHandle convertTableHandle(ConnectorTableHandle tableHandle) {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof PulsarTableHandle, "tableHandle is not an instance of PulsarTableHandle");
        return (PulsarTableHandle) tableHandle;
    }

    static PulsarColumnHandle convertColumnHandle(ColumnHandle columnHandle) {
        requireNonNull(columnHandle, "columnHandle is null");
        checkArgument(columnHandle instanceof PulsarColumnHandle, "columnHandle is not an instance of " +
                "PulsarColumnHandle");
        return (PulsarColumnHandle) columnHandle;
    }

    static PulsarSplit convertSplit(ConnectorSplit split) {
        requireNonNull(split, "split is null");
        checkArgument(split instanceof PulsarSplit, "split is not an instance of PulsarSplit");
        return (PulsarSplit) split;
    }

    static PulsarTableLayoutHandle convertLayout(ConnectorTableLayoutHandle layout) {
        requireNonNull(layout, "layout is null");
        checkArgument(layout instanceof PulsarTableLayoutHandle, "layout is not an instance of " +
                "PulsarTableLayoutHandle");
        return (PulsarTableLayoutHandle) layout;
    }

    @Override
    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass() {
        return PulsarTransactionHandle.class;
    }
}
