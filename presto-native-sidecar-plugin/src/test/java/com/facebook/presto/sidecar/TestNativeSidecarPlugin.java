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
package com.facebook.presto.sidecar;

import com.facebook.presto.spi.CoordinatorPlugin;
import com.facebook.presto.spi.function.FunctionNamespaceManagerContext;
import com.facebook.presto.spi.function.FunctionNamespaceManagerFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestNativeSidecarPlugin
{
    private final CoordinatorPlugin plugin = new NativeSidecarPlugin();

    @Test
    public void testLoadNativeFunctionNamespaceManagerPlugin()
    {
        Iterable<FunctionNamespaceManagerFactory> functionNamespaceManagerFactories = plugin.getFunctionNamespaceManagerFactories();
        FunctionNamespaceManagerFactory factory = getOnlyElement(functionNamespaceManagerFactories);
        factory.create(
                "unit-test",
                ImmutableMap.of(
                        "function-implementation-type", "CPP",
                        "supported-function-languages", "CPP"),
                new FunctionNamespaceManagerContext(
                        new UnimplementedTypeManager(),
                        Optional.of(new UnimplementedNodeManager()),
                        Optional.of(new UnimplementedFunctionMetadataManager())));
    }
}
