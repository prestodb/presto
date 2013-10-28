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

import com.facebook.presto.operator.HashBuilderOperator.HashSupplier;
import com.facebook.presto.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class HashJoinOperatorFactory
        implements OperatorFactory
{
    private final int operatorId;
    private final HashSupplier hashSupplier;
    private final List<Type> probeTypes;
    private final boolean enableOuterJoin;
    private final List<Type> types;
    private final JoinProbeFactory joinProbeFactory;
    private boolean closed;

    public HashJoinOperatorFactory(int operatorId,
            HashSupplier hashSupplier,
            List<Type> probeTypes,
            boolean enableOuterJoin,
            JoinProbeFactory joinProbeFactory)
    {
        this.operatorId = operatorId;
        this.hashSupplier = hashSupplier;
        this.probeTypes = probeTypes;
        this.enableOuterJoin = enableOuterJoin;

        this.joinProbeFactory = joinProbeFactory;

        this.types = ImmutableList.<Type>builder()
                .addAll(probeTypes)
                .addAll(hashSupplier.getTypes())
                .build();
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
        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, HashJoinOperator.class.getSimpleName());
        return new HashJoinOperator(operatorContext, hashSupplier, probeTypes, enableOuterJoin, joinProbeFactory);
    }

    @Override
    public void close()
    {
        closed = true;
    }
}
