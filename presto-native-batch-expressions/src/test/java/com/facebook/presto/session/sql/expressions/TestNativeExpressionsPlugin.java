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
package com.facebook.presto.session.sql.expressions;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.NodePoolType;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.RowExpressionSerde;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.sql.planner.ExpressionOptimizerFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestNativeExpressionsPlugin
{
    @Test
    public void testLoadPlugin()
    {
        Plugin plugin = new NativeExpressionsPlugin();
        Iterable<ExpressionOptimizerFactory> serviceFactories = plugin.getRowExpressionInterpreterServiceFactories();
        ExpressionOptimizerFactory factory = getOnlyElement(serviceFactories);
        factory.createOptimizer(ImmutableMap.of(), new TestingNodeManager(), new TestingRowExpressionSerde(), new TestingFunctionMetadataManager(), new TestingFunctionResoltion());
    }

    // TODO: @tdm clean this up
    private static class TestingNodeManager
            implements NodeManager
    {
        @Override
        public Set<Node> getAllNodes()
        {
            return null;
        }

        @Override
        public Set<Node> getWorkerNodes()
        {
            return null;
        }

        @Override
        public Node getCurrentNode()
        {
            return new Node() {
                @Override
                public String getHost()
                {
                    return null;
                }

                @Override
                public HostAddress getHostAndPort()
                {
                    return null;
                }

                @Override
                public URI getHttpUri()
                {
                    return null;
                }

                @Override
                public String getNodeIdentifier()
                {
                    return null;
                }

                @Override
                public String getVersion()
                {
                    return null;
                }

                @Override
                public boolean isCoordinator()
                {
                    return false;
                }

                @Override
                public boolean isResourceManager()
                {
                    return false;
                }

                @Override
                public boolean isCatalogServer()
                {
                    return false;
                }

                @Override
                public boolean isCoordinatorSidecar()
                {
                    return false;
                }

                @Override
                public NodePoolType getPoolType()
                {
                    return null;
                }
            };
        }

        @Override
        public Node getSidecarNode()
        {
            return null;
        }

        @Override
        public String getEnvironment()
        {
            return null;
        }
    }

    // TODO: @tdm
    private class TestingRowExpressionSerde
            implements RowExpressionSerde
    {
        @Override
        public String serialize(RowExpression expression)
        {
            return "";
        }

        @Override
        public RowExpression deserialize(String value)
        {
            return null;
        }
    }

    private class TestingFunctionMetadataManager
            implements FunctionMetadataManager
    {
        @Override
        public FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle)
        {
            return null;
        }
    }

    private class TestingFunctionResoltion
            implements StandardFunctionResolution
    {
        @Override
        public FunctionHandle notFunction()
        {
            return null;
        }

        @Override
        public boolean isNotFunction(FunctionHandle functionHandle)
        {
            return false;
        }

        @Override
        public FunctionHandle negateFunction(Type type)
        {
            return null;
        }

        @Override
        public boolean isNegateFunction(FunctionHandle functionHandle)
        {
            return false;
        }

        @Override
        public FunctionHandle likeVarcharFunction()
        {
            return null;
        }

        @Override
        public FunctionHandle likeCharFunction(Type valueType)
        {
            return null;
        }

        @Override
        public boolean isLikeFunction(FunctionHandle functionHandle)
        {
            return false;
        }

        @Override
        public FunctionHandle likePatternFunction()
        {
            return null;
        }

        @Override
        public boolean isLikePatternFunction(FunctionHandle functionHandle)
        {
            return false;
        }

        @Override
        public FunctionHandle arrayConstructor(List<? extends Type> argumentTypes)
        {
            return null;
        }

        @Override
        public FunctionHandle arithmeticFunction(OperatorType operator, Type leftType, Type rightType)
        {
            return null;
        }

        @Override
        public boolean isArithmeticFunction(FunctionHandle functionHandle)
        {
            return false;
        }

        @Override
        public FunctionHandle comparisonFunction(OperatorType operator, Type leftType, Type rightType)
        {
            return null;
        }

        @Override
        public boolean isComparisonFunction(FunctionHandle functionHandle)
        {
            return false;
        }

        @Override
        public boolean isEqualsFunction(FunctionHandle functionHandle)
        {
            return false;
        }

        @Override
        public FunctionHandle betweenFunction(Type valueType, Type lowerBoundType, Type upperBoundType)
        {
            return null;
        }

        @Override
        public boolean isBetweenFunction(FunctionHandle functionHandle)
        {
            return false;
        }

        @Override
        public FunctionHandle subscriptFunction(Type baseType, Type indexType)
        {
            return null;
        }

        @Override
        public boolean isSubscriptFunction(FunctionHandle functionHandle)
        {
            return false;
        }

        @Override
        public boolean isCastFunction(FunctionHandle functionHandle)
        {
            return false;
        }

        @Override
        public boolean isCountFunction(FunctionHandle functionHandle)
        {
            return false;
        }

        @Override
        public FunctionHandle countFunction()
        {
            return null;
        }

        @Override
        public FunctionHandle countFunction(Type valueType)
        {
            return null;
        }

        @Override
        public boolean isMaxFunction(FunctionHandle functionHandle)
        {
            return false;
        }

        @Override
        public FunctionHandle maxFunction(Type valueType)
        {
            return null;
        }

        @Override
        public FunctionHandle greatestFunction(List<Type> valueTypes)
        {
            return null;
        }

        @Override
        public boolean isMinFunction(FunctionHandle functionHandle)
        {
            return false;
        }

        @Override
        public FunctionHandle minFunction(Type valueType)
        {
            return null;
        }

        @Override
        public FunctionHandle leastFunction(List<Type> valueTypes)
        {
            return null;
        }

        @Override
        public boolean isApproximateCountDistinctFunction(FunctionHandle functionHandle)
        {
            return false;
        }

        @Override
        public FunctionHandle approximateCountDistinctFunction(Type valueType)
        {
            return null;
        }

        @Override
        public boolean isApproximateSetFunction(FunctionHandle functionHandle)
        {
            return false;
        }

        @Override
        public FunctionHandle approximateSetFunction(Type valueType)
        {
            return null;
        }
    }
}
