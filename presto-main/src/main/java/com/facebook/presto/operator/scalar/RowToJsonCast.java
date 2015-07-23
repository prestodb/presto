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

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.server.SliceSerializer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.Signature.withVariadicBound;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeUtils.createBlock;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

public class RowToJsonCast
        extends ParametricOperator
{
    public static final RowToJsonCast ROW_TO_JSON = new RowToJsonCast();
    private static final Supplier<ObjectMapper> OBJECT_MAPPER = Suppliers.memoize(() -> new ObjectMapperProvider().get().registerModule(new SimpleModule().addSerializer(Slice.class, new SliceSerializer())));
    private static final MethodHandle METHOD_HANDLE = methodHandle(RowToJsonCast.class, "toJson", Type.class, ConnectorSession.class, Block.class);

    private RowToJsonCast()
    {
        super(OperatorType.CAST, ImmutableList.of(withVariadicBound("T", "row")), StandardTypes.JSON, ImmutableList.of("T"));
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        Type type = types.get("T");
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(type);
        return operatorInfo(OperatorType.CAST, parseTypeSignature(StandardTypes.JSON), ImmutableList.of(type.getTypeSignature()), methodHandle, false, ImmutableList.of(false));
    }

    public static Slice toJson(Type rowType, ConnectorSession session, Block row)
    {
        Object object = rowType.getObjectValue(session, createBlock(rowType, row), 0);
        try {
            return Slices.utf8Slice(OBJECT_MAPPER.get().writeValueAsString(object));
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }
}
