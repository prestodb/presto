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
package com.facebook.presto.operator;

import com.facebook.presto.operator.LookupJoinOperators.JoinType;
import com.facebook.presto.operator.LookupOuterOperator.LookupOuterOperatorFactory;
import com.facebook.presto.operator.LookupOuterOperator.OuterLookupSourceSupplier;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class LookupJoinOperatorFactory
        implements JoinOperatorFactory
{
    private final int operatorId;
    private final LookupSourceSupplier lookupSourceSupplier;
    private final List<Type> probeTypes;
    private final JoinType joinType;
    private final List<Type> types;
    private final JoinProbeFactory joinProbeFactory;
    private boolean closed;

    public LookupJoinOperatorFactory(int operatorId,
            LookupSourceSupplier lookupSourceSupplier,
            List<Type> probeTypes,
            JoinType joinType,
            JoinProbeFactory joinProbeFactory)
    {
        this.operatorId = operatorId;
        this.lookupSourceSupplier = lookupSourceSupplier;
        this.probeTypes = probeTypes;
        this.joinType = joinType;

        this.joinProbeFactory = joinProbeFactory;

        this.types = ImmutableList.<Type>builder()
                .addAll(probeTypes)
                .addAll(lookupSourceSupplier.getTypes())
                .build();
    }

    public int getOperatorId()
    {
        return operatorId;
    }

    public List<Type> getProbeTypes()
    {
        return probeTypes;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        checkState(!closed, "Factory is already closed");
        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, LookupJoinOperator.class.getSimpleName());
        return new LookupJoinOperator(operatorContext, lookupSourceSupplier, probeTypes, joinType, joinProbeFactory);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        lookupSourceSupplier.release();
    }

    @Override
    public OperatorFactory duplicate()
    {
        return new LookupJoinOperatorFactory(operatorId, lookupSourceSupplier, probeTypes, joinType, joinProbeFactory);
    }

    @Override
    public Optional<OperatorFactory> createOuterOperatorFactory()
    {
        if (lookupSourceSupplier instanceof OuterLookupSourceSupplier) {
            return Optional.of(new LookupOuterOperatorFactory(operatorId, (OuterLookupSourceSupplier) lookupSourceSupplier, probeTypes));
        }
        return Optional.empty();
    }
}
