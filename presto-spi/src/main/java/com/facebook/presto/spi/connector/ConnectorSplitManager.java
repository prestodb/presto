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
package com.facebook.presto.spi.connector;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;

import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;

public interface ConnectorSplitManager
{
    /**
     * @deprecated use {@link #getSplits(ConnectorTransactionHandle, ConnectorSession, ConnectorTableLayoutHandle, SplitSchedulingStrategy)} instead.
     */
    @Deprecated
    default ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    default ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        if (splitSchedulingStrategy == UNGROUPED_SCHEDULING) {
            return getSplits(transactionHandle, session, layout);
        }
        throw new UnsupportedOperationException();
    }

    enum SplitSchedulingStrategy
    {
        UNGROUPED_SCHEDULING,
    }
}
