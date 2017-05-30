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
package com.facebook.presto.raptorx;

import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class RaptorTransactionHandle
        implements ConnectorTransactionHandle
{
    private final long transactionId;
    private final long commitId;

    @JsonCreator
    public RaptorTransactionHandle(
            @JsonProperty("transactionId") long transactionId,
            @JsonProperty("commitId") long commitId)
    {
        this.transactionId = transactionId;
        this.commitId = commitId;
    }

    @JsonProperty
    public long getTransactionId()
    {
        return transactionId;
    }

    @JsonProperty
    public long getCommitId()
    {
        return commitId;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        RaptorTransactionHandle other = (RaptorTransactionHandle) obj;
        return (transactionId == other.transactionId) &&
                (commitId == other.commitId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(transactionId, commitId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("transactionId", transactionId)
                .add("commitId", commitId)
                .toString();
    }
}
