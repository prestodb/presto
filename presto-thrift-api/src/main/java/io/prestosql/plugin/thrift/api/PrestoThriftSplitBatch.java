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
package io.prestosql.plugin.thrift.api;

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftSplitBatch
{
    private final List<PrestoThriftSplit> splits;
    private final PrestoThriftId nextToken;

    @ThriftConstructor
    public PrestoThriftSplitBatch(List<PrestoThriftSplit> splits, @Nullable PrestoThriftId nextToken)
    {
        this.splits = requireNonNull(splits, "splits is null");
        this.nextToken = nextToken;
    }

    @ThriftField(1)
    public List<PrestoThriftSplit> getSplits()
    {
        return splits;
    }

    @Nullable
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public PrestoThriftId getNextToken()
    {
        return nextToken;
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
        PrestoThriftSplitBatch other = (PrestoThriftSplitBatch) obj;
        return Objects.equals(this.splits, other.splits) &&
                Objects.equals(this.nextToken, other.nextToken);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(splits, nextToken);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("numberOfSplits", splits.size())
                .add("nextToken", nextToken)
                .toString();
    }
}
