
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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class StartTransaction
        extends DataDefinitionStatement
{
    private final List<TransactionMode> transactionModes;

    public StartTransaction(List<TransactionMode> transactionModes)
    {
        this(Optional.empty(), transactionModes);
    }

    public StartTransaction(NodeLocation location, List<TransactionMode> transactionModes)
    {
        this(Optional.of(location), transactionModes);
    }

    private StartTransaction(Optional<NodeLocation> location, List<TransactionMode> transactionModes)
    {
        super(location);
        this.transactionModes = ImmutableList.copyOf(requireNonNull(transactionModes, "transactionModes is null"));
    }

    public List<TransactionMode> getTransactionModes()
    {
        return transactionModes;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitStartTransaction(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(transactionModes);
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
        final StartTransaction other = (StartTransaction) obj;
        return Objects.equals(this.transactionModes, other.transactionModes);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("transactionModes", transactionModes)
                .toString();
    }
}
