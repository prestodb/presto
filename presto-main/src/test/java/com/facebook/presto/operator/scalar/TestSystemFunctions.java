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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.connector.system.SystemFunctions;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.spi.Node;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestSystemFunctions
        extends AbstractTestFunctions
{
    private final NodeVersion expectedVersion = new NodeVersion("1");

    public TestSystemFunctions()
    {
        Metadata metadata = functionAssertions.getMetadata();
        InternalNodeManager nodeManager = new InMemoryNodeManager() {
            @Override
            public Node getCurrentNode()
            {
                return new PrestoNode(super.getCurrentNode().getNodeIdentifier(), super.getCurrentNode().getHttpUri(), expectedVersion);
            }
        };

        List<SqlFunction> functions = new FunctionListBuilder(metadata.getTypeManager())
                .functions(new SystemFunctions(nodeManager).createVersionFunction())
                .getFunctions();
        metadata.getFunctionRegistry().addFunctions(functions);
    }

    @Test
    public void testVersion()
    {
        assertFunction("version()", VARCHAR, expectedVersion.getVersion());
    }
}
