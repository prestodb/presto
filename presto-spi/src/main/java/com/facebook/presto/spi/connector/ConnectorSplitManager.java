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
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;

import static java.util.Objects.requireNonNull;

public interface ConnectorSplitManager
{
    ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext);

    enum SplitSchedulingStrategy
    {
        UNGROUPED_SCHEDULING,
        GROUPED_SCHEDULING,
        REWINDABLE_GROUPED_SCHEDULING,
    }

    class SplitSchedulingContext
    {
        private final SplitSchedulingStrategy splitSchedulingStrategy;
        private final boolean schedulerUsesHostAddresses;
        private final WarningCollector warningCollector;

        /**
         * @param splitSchedulingStrategy the method by which splits are scheduled
         * @param schedulerUsesHostAddresses whether host addresses are take into account
         * when choosing where to schedule remotely accessible splits. If this is false,
         * the connector can return an empty list of addresses for remotely accessible
         * splits without any performance loss.  Non-remotely accessible splits always
         * need to provide host addresses.
         */
        public SplitSchedulingContext(SplitSchedulingStrategy splitSchedulingStrategy, boolean schedulerUsesHostAddresses, WarningCollector warningCollector)
        {
            this.splitSchedulingStrategy = requireNonNull(splitSchedulingStrategy, "splitSchedulingStrategy is null");
            this.schedulerUsesHostAddresses = schedulerUsesHostAddresses;
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null ");
        }

        public SplitSchedulingStrategy getSplitSchedulingStrategy()
        {
            return splitSchedulingStrategy;
        }

        public boolean schedulerUsesHostAddresses()
        {
            return schedulerUsesHostAddresses;
        }

        public WarningCollector getWarningCollector()
        {
            return warningCollector;
        }
    }

    default ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableFunctionHandle function)
    {
        throw new UnsupportedOperationException();
    }
}
