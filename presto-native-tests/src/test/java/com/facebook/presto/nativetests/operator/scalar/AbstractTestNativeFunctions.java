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
package com.facebook.presto.nativetests.operator.scalar;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.log.Logger;
import com.facebook.drift.codec.guice.ThriftCodecModule;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.block.RowBlockBuilder;
import com.facebook.presto.common.block.SingleRowBlockWriter;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SqlDecimal;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.sidecar.ForSidecarInfo;
import com.facebook.presto.sidecar.NativeSidecarFailureInfo;
import com.facebook.presto.sidecar.NativeSidecarPluginQueryRunner;
import com.facebook.presto.sidecar.expressions.NativeSidecarExpressionInterpreter;
import com.facebook.presto.sidecar.expressions.RowExpressionOptimizationResult;
import com.facebook.presto.sidecar.expressions.TestNativeExpressionInterpreter;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.operator.scalar.TestFunctions;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.intellij.lang.annotations.Language;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.lang.String.format;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestNativeFunctions
        implements TestFunctions
{
    private static final Logger log = Logger.get(TestNativeExpressionInterpreter.class);
    public static final TypeProvider SYMBOL_TYPES = TypeProvider.viewOf(ImmutableMap.<String, Type>builder().build());

    private Metadata metadata;
    private TestingRowExpressionTranslator translator;
    private DistributedQueryRunner queryRunner;
    private NativeSidecarExpressionInterpreter rowExpressionInterpreter;

    @BeforeClass
    public void init()
            throws Exception
    {
        queryRunner = NativeSidecarPluginQueryRunner.getQueryRunner();
        FunctionAndTypeManager functionAndTypeManager = queryRunner.getCoordinator().getFunctionAndTypeManager();
        metadata = MetadataManager.createTestMetadataManager(functionAndTypeManager);
        translator = new TestingRowExpressionTranslator(metadata);
        rowExpressionInterpreter = getRowExpressionInterpreter(functionAndTypeManager, queryRunner.getCoordinator().getPluginNodeManager());
    }

    @AfterClass(alwaysRun = true)
    public void stopSidecar()
    {
        if (queryRunner != null) {
            try {
                queryRunner.close();
            }
            catch (Exception e) {
                log.error(e, "Failed to close query runner");
            }
            queryRunner = null;
        }
    }

    @Override
    public void assertFunction(@Language("SQL") String projection, Type expectedType, Object expected)
    {
        ConstantExpression actual = evaluate(projection);
        ConstantExpression expectedConstant = toConstantExpression(expectedType, expected);

        assertTrue(typesEqualIgnoringVarcharParameters(actual.getType(), expectedType), format("Expected type %s but got %s", expectedType, actual.getType()));

        Object actualValue = toNativeValue(expectedType, actual);
        Object expectedValue = toNativeValue(expectedType, expectedConstant);
        assertEquals(actualValue, expectedValue);
    }

    @Override
    public void assertInvalidFunction(String projection, StandardErrorCode errorCode, @Language("RegExp") String errorMessage)
    {
        RowExpression rowExpression = sqlToRowExpression(projection);
        RowExpressionOptimizationResult optimizationResult = evaluate(rowExpression);
        NativeSidecarFailureInfo failureInfo = optimizationResult.getExpressionFailureInfo();
        assertNotNull(failureInfo);
        assertNotNull(failureInfo.getErrorCode());
        Assert.assertEquals(failureInfo.getErrorCode().getCode(), errorCode.toErrorCode().getCode());
        assertNull(optimizationResult.getOptimizedExpression());
        assertNotNull(failureInfo.getMessage());
        assertTrue(failureInfo.getMessage().contains(errorMessage), format("Sidecar response: %s did not contain expected error message: %s.", failureInfo, errorMessage));
    }

    @Override
    public void assertNotSupported(String projection, @Language("RegExp") String message)
    {
        assertInvalidFunction(projection, NOT_SUPPORTED, message);
    }

    @Override
    public void assertInvalidCast(@Language("SQL") String projection, @Language("RegExp") String message)
    {
        assertInvalidFunction(projection, INVALID_CAST_ARGUMENT, message);
    }

    private boolean typesEqualIgnoringVarcharParameters(Type actual, Type expected)
    {
        if (actual == expected) {
            return true;
        }
        if (actual instanceof com.facebook.presto.common.type.VarcharType && expected instanceof com.facebook.presto.common.type.VarcharType) {
            return true;
        }
        if (!actual.getTypeSignature().getBase().equals(expected.getTypeSignature().getBase())) {
            return false;
        }
        List<Type> actualParams = actual.getTypeParameters();
        List<Type> expectedParams = expected.getTypeParameters();
        if (actualParams.size() != expectedParams.size()) {
            return false;
        }
        for (int i = 0; i < actualParams.size(); i++) {
            if (!typesEqualIgnoringVarcharParameters(actualParams.get(i), expectedParams.get(i))) {
                return false;
            }
        }
        return true;
    }

    private ConstantExpression toConstantExpression(Type expectedType, Object expected)
    {
        if (expectedType.getJavaType() == Block.class) {
            return new ConstantExpression(toBlock(expectedType, expected), expectedType);
        }
        return new ConstantExpression(toNativePrimitive(expectedType, expected), expectedType);
    }

    private Object toNativePrimitive(Type type, Object value)
    {
        if (value == null) {
            return null;
        }
        if (type instanceof DecimalType && value instanceof SqlDecimal) {
            DecimalType decimalType = (DecimalType) type;
            BigInteger unscaled = ((SqlDecimal) value).getUnscaledValue();
            if (decimalType.isShort()) {
                return unscaled.longValueExact();
            }
            return Decimals.encodeUnscaledValue(unscaled);
        }
        if (type instanceof DecimalType && value instanceof BigDecimal) {
            DecimalType decimalType = (DecimalType) type;
            BigInteger unscaled = ((BigDecimal) value).unscaledValue();
            if (decimalType.isShort()) {
                return unscaled.longValueExact();
            }
            return Decimals.encodeUnscaledValue(unscaled);
        }
        if (type.getJavaType() == Slice.class) {
            if (value instanceof Slice) {
                return value;
            }
            if (value instanceof String) {
                return Slices.utf8Slice((String) value);
            }
        }
        return value;
    }

    private Block toBlock(Type type, Object value)
    {
        if (value == null) {
            return null;
        }
        if (value instanceof Block) {
            return (Block) value;
        }
        if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            List<?> values = (List<?>) value;
            BlockBuilder elementBuilder = arrayType.getElementType().createBlockBuilder(null, values.size());
            for (Object element : values) {
                writeValue(arrayType.getElementType(), elementBuilder, element);
            }
            return elementBuilder.build();
        }
        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            Map<?, ?> mapValues = (Map<?, ?>) value;
            BlockBuilder blockBuilder = mapType.createBlockBuilder(null, 1);
            BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
            for (Map.Entry<?, ?> entry : mapValues.entrySet()) {
                writeValue(mapType.getKeyType(), entryBuilder, entry.getKey());
                writeValue(mapType.getValueType(), entryBuilder, entry.getValue());
            }
            blockBuilder.closeEntry();
            return blockBuilder.build();
        }
        if (type instanceof RowType) {
            RowType rowType = (RowType) type;
            List<?> rowValues = (List<?>) value;
            List<Type> fieldTypes = rowType.getTypeParameters();
            RowBlockBuilder rowBlockBuilder = (RowBlockBuilder) rowType.createBlockBuilder(null, 1);
            SingleRowBlockWriter rowWriter = rowBlockBuilder.beginBlockEntry();
            for (int i = 0; i < fieldTypes.size(); i++) {
                writeValue(fieldTypes.get(i), rowWriter.getFieldBlockBuilder(i), rowValues.get(i));
            }
            rowBlockBuilder.closeEntry();
            // getSingleValueBlock returns an AbstractSingleRowBlock the row writer wrote
            return rowBlockBuilder.build().getSingleValueBlock(0);
        }
        throw new IllegalArgumentException(format("Unsupported block conversion for type %s and value %s", type, value));
    }

    private void writeValue(Type type, BlockBuilder blockBuilder, Object value)
    {
        if (value == null) {
            blockBuilder.appendNull();
            return;
        }
        if (type instanceof RowType) {
            RowType rowType = (RowType) type;
            List<?> rowValues = (List<?>) value;
            List<Type> fieldTypes = rowType.getTypeParameters();
            if (blockBuilder instanceof RowBlockBuilder) {
                RowBlockBuilder rowBlockBuilder = (RowBlockBuilder) blockBuilder;
                SingleRowBlockWriter rowWriter = rowBlockBuilder.beginBlockEntry();
                for (int i = 0; i < fieldTypes.size(); i++) {
                    writeValue(fieldTypes.get(i), rowWriter.getFieldBlockBuilder(i), rowValues.get(i));
                }
                rowBlockBuilder.closeEntry();
                return;
            }
            if (blockBuilder instanceof SingleRowBlockWriter) {
                SingleRowBlockWriter rowWriter = (SingleRowBlockWriter) blockBuilder;
                for (int i = 0; i < fieldTypes.size(); i++) {
                    writeValue(fieldTypes.get(i), rowWriter.getFieldBlockBuilder(i), rowValues.get(i));
                }
                rowWriter.closeEntry();
                return;
            }
        }
        if (type.getJavaType() == Block.class) {
            type.writeObject(blockBuilder, toBlock(type, value));
            return;
        }
        writeNativeValue(type, blockBuilder, toNativePrimitive(type, value));
    }

    private Object toNativeValue(Type expectedType, ConstantExpression constant)
    {
        Object value = constant.getValue();
        if (value == null) {
            return null;
        }

        if (expectedType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) expectedType;
            BigInteger unscaled;
            if (value instanceof Slice) {
                unscaled = Decimals.decodeUnscaledValue((Slice) value);
            }
            else if (value instanceof Long) {
                unscaled = BigInteger.valueOf((Long) value);
            }
            else if (value instanceof SqlDecimal) {
                SqlDecimal sqlDecimal = (SqlDecimal) value;
                return new SqlDecimal(sqlDecimal.getUnscaledValue(), decimalType.getPrecision(), decimalType.getScale());
            }
            else {
                unscaled = new BigInteger(value.toString());
            }
            return new SqlDecimal(unscaled, decimalType.getPrecision(), decimalType.getScale());
        }

        if (expectedType.getJavaType() == Block.class) {
            if (expectedType instanceof ArrayType) {
                ArrayType arrayType = (ArrayType) expectedType;
                Block elementsBlock = (Block) value;
                List<Object> values = new ArrayList<>(elementsBlock.getPositionCount());
                for (int i = 0; i < elementsBlock.getPositionCount(); i++) {
                    values.add(arrayType.getElementType().getObjectValue(TEST_SESSION.getSqlFunctionProperties(), elementsBlock, i));
                }
                return values;
            }
            return BlockAssertions.toValues(expectedType, (Block) value);
        }

        if (value instanceof Slice) {
            return ((Slice) value).toStringUtf8();
        }

        return value;
    }

    private ConstantExpression evaluate(@Language("SQL") String expression)
    {
        RowExpression parsedExpression = sqlToRowExpression(expression);
        RowExpressionOptimizationResult optimizationResult = evaluate(parsedExpression);
        NativeSidecarFailureInfo failureInfo = optimizationResult.getExpressionFailureInfo();
        assertNotNull(failureInfo);
        assertTrue(failureInfo.getMessage() != null && failureInfo.getMessage().isEmpty());
        RowExpression result = optimizationResult.getOptimizedExpression();
        assertTrue(result instanceof ConstantExpression);
        return (ConstantExpression) result;
    }

    private RowExpression sqlToRowExpression(String expression)
    {
        Expression parsedExpression = FunctionAssertions.createExpression(expression, metadata, SYMBOL_TYPES);
        return translator.translate(parsedExpression, SYMBOL_TYPES);
    }

    private RowExpressionOptimizationResult evaluate(RowExpression expression)
    {
        List<RowExpressionOptimizationResult> results = rowExpressionInterpreter.optimize(TEST_SESSION.toConnectorSession(), ExpressionOptimizer.Level.EVALUATED, ImmutableList.of(expression));
        assertEquals(results.size(), 1);
        return results.get(0);
    }

    private NativeSidecarExpressionInterpreter getRowExpressionInterpreter(FunctionAndTypeManager functionAndTypeManager, NodeManager nodeManager)
    {
        Module module = binder -> {
            binder.bind(NodeManager.class).toInstance(nodeManager);
            binder.bind(TypeManager.class).toInstance(functionAndTypeManager);
            binder.install(new JsonModule());
            binder.install(new HandleJsonModule(functionAndTypeManager.getHandleResolver()));
            binder.bind(ConnectorManager.class).toProvider(() -> null).in(Scopes.SINGLETON);
            binder.install(new ThriftCodecModule());
            configBinder(binder).bindConfig(FeaturesConfig.class);

            jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
            newSetBinder(binder, Type.class);

            binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class).in(Scopes.SINGLETON);
            newSetBinder(binder, BlockEncoding.class);
            jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
            jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);
            jsonCodecBinder(binder).bindListJsonCodec(RowExpression.class);
            jsonCodecBinder(binder).bindListJsonCodec(RowExpressionOptimizationResult.class);

            httpClientBinder(binder).bindHttpClient("sidecar", ForSidecarInfo.class);

            binder.bind(NativeSidecarExpressionInterpreter.class).in(Scopes.SINGLETON);
        };
        Bootstrap app = new Bootstrap(ImmutableList.of(module));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();

        return injector.getInstance(NativeSidecarExpressionInterpreter.class);
    }
}
