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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

/**
 * Exactly one of {@link #cursor} or {@link #pageSource} must be implemented.
 */
public interface SystemTable
{
    enum Distribution
    {
        ALL_NODES, ALL_COORDINATORS, SINGLE_COORDINATOR
    }

    Distribution getDistribution();

    ConnectorTableMetadata getTableMetadata();

    /**
     * Create a cursor for the data in this table.
     *
     * @param session the session to use for creating the data
     * @param constraint the constraints for the table columns (indexed from 0)
     */
    default RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Create a page source for the data in this table.
     *
     * @param session the session to use for creating the data
     * @param constraint the constraints for the table columns (indexed from 0)
     */
    default ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        throw new UnsupportedOperationException();
    }
}
