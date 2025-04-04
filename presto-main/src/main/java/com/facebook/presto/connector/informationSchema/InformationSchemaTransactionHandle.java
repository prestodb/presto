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
package com.facebook.presto.connector.informationSchema;

import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class InformationSchemaTransactionHandle
        implements ConnectorTransactionHandle
{
    private final TransactionId transactionId;

    @JsonCreator
    public InformationSchemaTransactionHandle(@JsonProperty("transactionId") TransactionId transactionId)
    {
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
    }

    @JsonProperty
    public TransactionId getTransactionId()
    {
        return transactionId;
    }

    @Override
    public int hashCode()
    {
        return transactionId.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        InformationSchemaTransactionHandle other = (InformationSchemaTransactionHandle) obj;
        return Objects.equals(transactionId, other.transactionId);
    }

    @Override
    public String toString()
    {
        return transactionId.toString();
    }
}
