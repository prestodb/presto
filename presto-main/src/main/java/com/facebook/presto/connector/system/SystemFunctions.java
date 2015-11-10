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
package com.facebook.presto.connector.system;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.Node;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.inject.Inject;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public final class SystemFunctions
{
    private final InternalNodeManager nodeManager;

    @Inject
    public SystemFunctions(InternalNodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    private Slice getVersion()
    {
        Node node = nodeManager.getCurrentNode();
        NodeVersion version = NodeVersion.UNKNOWN;
        if (node instanceof PrestoNode) {
            version = ((PrestoNode) node).getNodeVersion();
        }
        return Slices.utf8Slice(version.getVersion());
    }

    public SqlScalarFunction createVersionFunction()
    {
        Signature signature = new Signature("version", SCALAR, ImmutableList.of(), VARCHAR.getTypeSignature(), ImmutableList.of(), false);
        MethodHandle methodHandle;
        Class<?> clazz = this.getClass();
        try {
            Method method = clazz.getDeclaredMethod("getVersion");
            method.setAccessible(true);
            methodHandle = lookup().unreflect(method).bindTo(this);
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }

        return SqlScalarFunction.create(signature, "Presto Version", false, methodHandle, true, false, ImmutableList.of());
    }
}
