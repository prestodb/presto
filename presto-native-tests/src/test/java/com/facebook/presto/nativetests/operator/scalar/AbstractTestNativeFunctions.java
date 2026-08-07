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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RowBlockBuilder;
import com.facebook.presto.common.block.SingleRowBlockWriter;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.IpAddressType;
import com.facebook.presto.common.type.IpPrefixType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SqlDecimal;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.sidecar.NativeSidecarFailureInfo;
import com.facebook.presto.sidecar.NativeSidecarPluginQueryRunner;
import com.facebook.presto.sidecar.expressions.NativeSidecarExpressionInterpreter;
import com.facebook.presto.sidecar.expressions.RowExpressionOptimizationResult;
import com.facebook.presto.sidecar.expressions.TestNativeExpressionInterpreter;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.operator.scalar.TestFunctions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.IpPrefixType.IPPREFIX;
import static com.facebook.presto.common.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.String.format;
import static java.lang.System.arraycopy;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestNativeFunctions
        implements TestFunctions
{
    private static final Logger log = Logger.get(AbstractTestNativeFunctions.class);
    public static final TypeProvider SYMBOL_TYPES = TypeProvider.viewOf(ImmutableMap.<String, Type>builder().build());

    private static volatile DistributedQueryRunner sharedQueryRunner;
    private static volatile NativeSidecarExpressionInterpreter sharedRowExpressionInterpreter;
    private static volatile Metadata sharedMetadata;
    private static volatile TestingRowExpressionTranslator sharedTranslator;

    private Metadata metadata;
    private TestingRowExpressionTranslator translator;
    private NativeSidecarExpressionInterpreter rowExpressionInterpreter;

    @BeforeClass
    public void init()
            throws Exception
    {
        // TestNG may instantiate subclasses in parallel threads; synchronized ensures
        // one-time initialization of shared static state, volatile ensures visibility.
        synchronized (AbstractTestNativeFunctions.class) {
            if (sharedQueryRunner == null) {
                sharedQueryRunner = NativeSidecarPluginQueryRunner.getQueryRunner();
                FunctionAndTypeManager functionAndTypeManager = sharedQueryRunner.getCoordinator().getFunctionAndTypeManager();
                sharedMetadata = MetadataManager.createTestMetadataManager(functionAndTypeManager);
                sharedTranslator = new TestingRowExpressionTranslator(sharedMetadata);
                sharedRowExpressionInterpreter = getRowExpressionInterpreter(functionAndTypeManager);
            }
        }
        metadata = sharedMetadata;
        translator = sharedTranslator;
        rowExpressionInterpreter = sharedRowExpressionInterpreter;
    }

    @AfterClass(alwaysRun = true)
    public void stopSidecar()
    {
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
    public void assertInvalidFunction(String projection, StandardErrorCode errorCode, String messagePattern)
    {
        RowExpression rowExpression = sqlToRowExpression(projection);
        RowExpressionOptimizationResult optimizationResult = evaluate(rowExpression);
        NativeSidecarFailureInfo failureInfo = optimizationResult.getExpressionFailureInfo();
        assertNotNull(failureInfo);
        assertNotNull(failureInfo.getErrorCode());
        assertEquals(failureInfo.getErrorCode().getCode(), errorCode.toErrorCode().getCode());
        assertNull(optimizationResult.getOptimizedExpression());
        assertNotNull(failureInfo.getMessage());
        assertTrue(failureInfo.getMessage().contains(messagePattern) || Pattern.compile(messagePattern).matcher(failureInfo.getMessage()).find(),
                format("Sidecar error message [%s] doesn't match [%s]", failureInfo.getMessage(), messagePattern));
    }

    @Override
    public void assertInvalidFunction(String projection, String messagePattern)
    {
        assertInvalidFunction(projection, INVALID_FUNCTION_ARGUMENT, messagePattern);
    }

    @Override
    public void assertNotSupported(String projection, String message)
    {
        assertInvalidFunction(projection, NOT_SUPPORTED, message);
    }

    @Override
    public void assertInvalidCast(@Language("SQL") String projection, String message)
    {
        assertInvalidFunction(projection, INVALID_CAST_ARGUMENT, message);
    }

    // IP_PREFIX() returns IPPREFIX which is a ROW type in Velox. The expression optimizer
    // serializes it as a ROW_CONSTRUCTOR SpecialFormExpression rather than a ConstantExpression.
    // This helper unwraps the ROW_CONSTRUCTOR and reconstructs the "ip/prefix" string for comparison.
    public void assertIpPrefixFunction(@Language("SQL") String projection, String expected)
    {
        RowExpression rowExpression = sqlToRowExpression(projection);
        RowExpressionOptimizationResult optimizationResult = evaluate(rowExpression);
        NativeSidecarFailureInfo failureInfo = optimizationResult.getExpressionFailureInfo();
        assertNotNull(failureInfo);
        assertTrue(failureInfo.getMessage() != null && failureInfo.getMessage().isEmpty(),
                format("Expected success but got sidecar failure: %s", failureInfo));
        RowExpression result = optimizationResult.getOptimizedExpression();
        assertTrue(result instanceof SpecialFormExpression,
                format("Expected SpecialFormExpression (ROW_CONSTRUCTOR) for IPPREFIX result but got %s", result.getClass().getSimpleName()));
        SpecialFormExpression rowConstructor = (SpecialFormExpression) result;
        assertEquals(rowConstructor.getForm(), SpecialFormExpression.Form.ROW_CONSTRUCTOR,
                format("Expected ROW_CONSTRUCTOR form but got %s", rowConstructor.getForm()));
        assertTrue(rowConstructor.getType() instanceof IpPrefixType,
                format("Expected IPPREFIX return type but got %s", rowConstructor.getType()));

        // arguments[0] is the IPADDRESS child (ConstantExpression with Slice value)
        // arguments[1] is the prefix length child (ConstantExpression with Long value)
        List<RowExpression> args = rowConstructor.getArguments();
        assertEquals(args.size(), 2);
        ConstantExpression ipArg = (ConstantExpression) args.get(0);
        ConstantExpression prefixArg = (ConstantExpression) args.get(1);

        Slice ipSlice = (Slice) ipArg.getValue();
        String actual = toNativeValue(IPPREFIX, new ConstantExpression(
                reconstructIpPrefixSlice(ipSlice, ((Long) prefixArg.getValue()).byteValue()),
                IPPREFIX)).toString();
        assertEquals(actual, expected,
                format("IP prefix mismatch for expression: %s", projection));
    }

    private static Slice reconstructIpPrefixSlice(Slice ipAddressSlice, byte prefixLength)
    {
        // IpPrefixType stores 17 bytes: 16 bytes of IPv6 address + 1 byte prefix length
        byte[] bytes = new byte[17];
        arraycopy(ipAddressSlice.getBytes(), 0, bytes, 0, 16);
        bytes[16] = prefixLength;
        return wrappedBuffer(bytes);
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
                if (type instanceof IpAddressType) {
                    return ipAddressStringToSlice((String) value);
                }
                if (type instanceof IpPrefixType) {
                    return ipPrefixStringToSlice((String) value);
                }
                return Slices.utf8Slice((String) value);
            }
        }
        return value;
    }

    private static Slice ipAddressStringToSlice(String address)
    {
        byte[] addr = InetAddresses.forString(address).getAddress();
        byte[] bytes;
        if (addr.length == 4) {
            bytes = new byte[16];
            bytes[10] = (byte) 0xff;
            bytes[11] = (byte) 0xff;
            arraycopy(addr, 0, bytes, 12, 4);
        }
        else {
            bytes = addr;
        }
        return wrappedBuffer(bytes);
    }

    private static Slice ipPrefixStringToSlice(String value)
    {
        String[] parts = value.split("/");
        byte[] address = InetAddresses.forString(parts[0]).getAddress();
        int subnetSize = Integer.parseInt(parts[1]);
        byte[] bytes = new byte[IPPREFIX.getFixedSize()];
        if (address.length == 4) {
            bytes[10] = (byte) 0xff;
            bytes[11] = (byte) 0xff;
            arraycopy(address, 0, bytes, 12, 4);
        }
        else {
            arraycopy(address, 0, bytes, 0, 16);
        }
        bytes[IPPREFIX.getFixedSize() - 1] = (byte) subnetSize;
        return wrappedBuffer(bytes);
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

        if (expectedType instanceof IpAddressType || expectedType instanceof IpPrefixType) {
            if (value instanceof Slice) {
                return expectedType.getObjectValue(TEST_SESSION.getSqlFunctionProperties(), blockFromSlice(expectedType, (Slice) value), 0);
            }
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

    private static Block blockFromSlice(Type type, Slice value)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        type.writeSlice(blockBuilder, value);
        return blockBuilder.build();
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

    private static NativeSidecarExpressionInterpreter getRowExpressionInterpreter(FunctionAndTypeManager functionAndTypeManager)
    {
        return TestNativeExpressionInterpreter.getRowExpressionInterpreter(
                functionAndTypeManager, sharedQueryRunner.getCoordinator().getPluginNodeManager());
    }
}
