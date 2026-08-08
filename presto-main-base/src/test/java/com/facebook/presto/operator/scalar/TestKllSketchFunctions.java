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

import com.facebook.presto.common.type.SqlVarbinary;
import com.google.common.base.Joiner;
import org.apache.datasketches.common.ArrayOfBooleansSerDe;
import org.apache.datasketches.common.ArrayOfDoublesSerDe;
import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.kll.KllItemsSketch;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;

public class TestKllSketchFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testDoubles()
    {
        KllItemsSketch<Double> sketch = KllItemsSketch.newHeapInstance(Double::compareTo, new ArrayOfDoublesSerDe());
        DoubleStream.iterate(0, i -> i + 1).limit(100).forEach(sketch::update);
        String sketchProjection = getSketchProjection(sketch, "double");
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.0 as DOUBLE)"), DOUBLE, 0.0);
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.5 as DOUBLE)"), DOUBLE, 49.0);
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.5 as DOUBLE)", false), DOUBLE, 50.0);
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(1.0 as DOUBLE)"), DOUBLE, 99.0);

        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "CAST(-1 as DOUBLE)"), DOUBLE, 0.0);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "CAST(49 as DOUBLE)"), DOUBLE, 0.5);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "CAST(50 as DOUBLE)", false), DOUBLE, 0.5);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "CAST(99 as DOUBLE)"), DOUBLE, 1.0);
    }

    @Test
    public void testInts()
    {
        KllItemsSketch<Long> sketch = KllItemsSketch.newHeapInstance(Long::compareTo, new ArrayOfLongsSerDe());
        LongStream.iterate(0, i -> i + 1).limit(100).forEach(sketch::update);
        String sketchProjection = getSketchProjection(sketch, "bigint");
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.0 as DOUBLE)"), BIGINT, 0L);
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.5 as DOUBLE)"), BIGINT, 49L);
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.5 as DOUBLE)", false), BIGINT, 50L);
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(1.0 as DOUBLE)"), BIGINT, 99L);

        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "CAST(-1 as BIGINT)"), DOUBLE, 0.0);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "CAST(49 as BIGINT)"), DOUBLE, 0.5);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "CAST(50 as BIGINT)", false), DOUBLE, 0.5);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "CAST(99 as BIGINT)"), DOUBLE, 1.0);
    }

    @Test
    public void testStrings()
    {
        KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(String::compareTo, new ArrayOfStringsSerDe());
        Arrays.stream("abcdefghijklmnopqrstuvwxyz".split("")).forEach(sketch::update);
        String sketchProjection = getSketchProjection(sketch, "varchar");
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.0 as DOUBLE)"), VARCHAR, "a");
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.5 as DOUBLE)"), VARCHAR, "m");
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.5 as DOUBLE)", false), VARCHAR, "n");
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(1.0 as DOUBLE)"), VARCHAR, "z");

        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "'1'"), DOUBLE, 0.0);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "'m'"), DOUBLE, 0.5);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "'n'", false), DOUBLE, 0.5);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "'z'"), DOUBLE, 1.0);
    }

    @Test
    public void testBooleans()
    {
        KllItemsSketch<Boolean> sketch = KllItemsSketch.newHeapInstance(Boolean::compareTo, new ArrayOfBooleansSerDe());
        LongStream.iterate(0, i -> i + 1).limit(100).mapToObj(i -> i % 3 == 0).forEach(sketch::update);
        String sketchProjection = getSketchProjection(sketch, "boolean");
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.5 as DOUBLE)"), BOOLEAN, false);
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.5 as DOUBLE)", false), BOOLEAN, false);

        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "false", false), DOUBLE, 0.0);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "true", false), DOUBLE, 0.66);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "false"), DOUBLE, 0.66);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "true"), DOUBLE, 1.0);
    }

    /**
     * Cross-engine golden-byte tests: verify that sketches serialized by the native (C++) engine
     * can be deserialized and queried correctly by the Java engine.
     *
     * <p>Golden hex bytes were produced by running the DISABLED C++ test suite with
     * {@code --gtest_also_run_disabled_tests} and capturing the output of:
     * <ul>
     *   <li>KllSketchCrossEngineTest.DISABLED_printNativeBigintGoldenBytes</li>
     *   <li>KllSketchCrossEngineTest.DISABLED_printNativeDoubleGoldenBytes</li>
     *   <li>KllSketchCrossEngineTest.DISABLED_printNativeVarcharGoldenBytes</li>
     *   <li>KllSketchCrossEngineTest.DISABLED_printNativeBooleanGoldenBytes</li>
     * </ul>
     *
     * <p>Each test inserts 100 items (0..99 for numeric, 'a'..'z' for varchar,
     * i%3==0 for boolean) using the C++ {@code kll_sketch<T>} at k=200, then serializes
     * using the format that the Java engine expects (standard serde for all types,
     * {@link org.apache.datasketches.common.BitPackedBooleanSerDe}-equivalent for boolean).
     *
     * <p>TODO: populate the hex constants below by running the C++ printer tests, then
     * remove the @Ignore annotations.
     */

    // BIGINT: native sketch bytes for values 0..99, k=200
    // Generated by: KllSketchCrossEngineTest.DISABLED_printNativeBigintGoldenBytes (C++)
    // CHECKSTYLE:OFF: LineLength
    private static final String NATIVE_BIGINT_GOLDEN_HEX = "05010f00c80008006400000000000000c8000100640000000000000000000000630000000000000063000000000000006200000000000000610000000000000060000000000000005f000000000000005e000000000000005d000000000000005c000000000000005b000000000000005a0000000000000059000000000000005800000000000000570000000000000056000000000000005500000000000000540000000000000053000000000000005200000000000000510000000000000050000000000000004f000000000000004e000000000000004d000000000000004c000000000000004b000000000000004a0000000000000049000000000000004800000000000000470000000000000046000000000000004500000000000000440000000000000043000000000000004200000000000000410000000000000040000000000000003f000000000000003e000000000000003d000000000000003c000000000000003b000000000000003a0000000000000039000000000000003800000000000000370000000000000036000000000000003500000000000000340000000000000033000000000000003200000000000000310000000000000030000000000000002f000000000000002e000000000000002d000000000000002c000000000000002b000000000000002a0000000000000029000000000000002800000000000000270000000000000026000000000000002500000000000000240000000000000023000000000000002200000000000000210000000000000020000000000000001f000000000000001e000000000000001d000000000000001c000000000000001b000000000000001a0000000000000019000000000000001800000000000000170000000000000016000000000000001500000000000000140000000000000013000000000000001200000000000000110000000000000010000000000000000f000000000000000e000000000000000d000000000000000c000000000000000b000000000000000a000000000000000900000000000000080000000000000007000000000000000600000000000000050000000000000004000000000000000300000000000000020000000000000001000000000000000000000000000000";

    // DOUBLE: native sketch bytes for values 0.0..99.0, k=200
    // Generated by: KllSketchCrossEngineTest.DISABLED_printNativeDoubleGoldenBytes (C++)
    private static final String NATIVE_DOUBLE_GOLDEN_HEX = "05010f00c80008006400000000000000c80001006400000000000000000000000000000000c058400000000000c058400000000000805840000000000040584000000000000058400000000000c057400000000000805740000000000040574000000000000057400000000000c056400000000000805640000000000040564000000000000056400000000000c055400000000000805540000000000040554000000000000055400000000000c054400000000000805440000000000040544000000000000054400000000000c053400000000000805340000000000040534000000000000053400000000000c052400000000000805240000000000040524000000000000052400000000000c051400000000000805140000000000040514000000000000051400000000000c050400000000000805040000000000040504000000000000050400000000000804f400000000000004f400000000000804e400000000000004e400000000000804d400000000000004d400000000000804c400000000000004c400000000000804b400000000000004b400000000000804a400000000000004a40000000000080494000000000000049400000000000804840000000000000484000000000008047400000000000004740000000000080464000000000000046400000000000804540000000000000454000000000008044400000000000004440000000000080434000000000000043400000000000804240000000000000424000000000008041400000000000004140000000000080404000000000000040400000000000003f400000000000003e400000000000003d400000000000003c400000000000003b400000000000003a4000000000000039400000000000003840000000000000374000000000000036400000000000003540000000000000344000000000000033400000000000003240000000000000314000000000000030400000000000002e400000000000002c400000000000002a40000000000000284000000000000026400000000000002440000000000000224000000000000020400000000000001c4000000000000018400000000000001440000000000000104000000000000008400000000000000040000000000000f03f0000000000000000";

    // VARCHAR: native sketch bytes for 'a'..'z', k=200
    // Generated by: KllSketchCrossEngineTest.DISABLED_printNativeVarcharGoldenBytes (C++)
    private static final String NATIVE_VARCHAR_GOLDEN_HEX = "05010f00c80008001a00000000000000c8000100ae0000000100000061010000007a010000007a0100000079010000007801000000770100000076010000007501000000740100000073010000007201000000710100000070010000006f010000006e010000006d010000006c010000006b010000006a010000006901000000680100000067010000006601000000650100000064010000006301000000620100000061";
    // CHECKSTYLE:ON: LineLength

    // BOOLEAN: native sketch bytes for i%3==0, i in 0..99, k=200 (bit-packed via serializeBoolSketch)
    // Generated by: KllSketchCrossEngineTest.DISABLED_printNativeBooleanGoldenBytes (C++)
    private static final String NATIVE_BOOLEAN_GOLDEN_HEX =
            "05010f00c80008006400000000000000c800010064000000000149922449922449922449922409";

    @Test
    public void testNativeToBigintRoundTrip()
    {
        String sketchProjection = getByteArrayProjection(fromHex(NATIVE_BIGINT_GOLDEN_HEX), "bigint");
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.0 as DOUBLE)"), BIGINT, 0L);
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(1.0 as DOUBLE)"), BIGINT, 99L);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "CAST(49 as BIGINT)"), DOUBLE, 0.5);
    }

    @Test
    public void testNativeToDoubleRoundTrip()
    {
        String sketchProjection = getByteArrayProjection(fromHex(NATIVE_DOUBLE_GOLDEN_HEX), "double");
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.0 as DOUBLE)"), DOUBLE, 0.0);
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(1.0 as DOUBLE)"), DOUBLE, 99.0);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "CAST(49.0 as DOUBLE)"), DOUBLE, 0.5);
    }

    @Test
    public void testNativeToVarcharRoundTrip()
    {
        String sketchProjection = getByteArrayProjection(fromHex(NATIVE_VARCHAR_GOLDEN_HEX), "varchar");
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.0 as DOUBLE)"), VARCHAR, "a");
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(1.0 as DOUBLE)"), VARCHAR, "z");
    }

    /**
     * Most critical cross-engine test: boolean sketches use bit-packing in Java
     * (ArrayOfBooleansSerDe) but the default C++ serde writes 1 byte per boolean.
     * The C++ engine produces and consumes the same bit-packed format as Java
     * via the transcoding layer in KllSketchTypeTraits.h (serializeBoolSketch /
     * deserializeBoolSketch).
     */
    @Test
    public void testNativeToBooleanRoundTrip()
    {
        String sketchProjection = getByteArrayProjection(fromHex(NATIVE_BOOLEAN_GOLDEN_HEX), "boolean");
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.0 as DOUBLE)"), BOOLEAN, false);
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(1.0 as DOUBLE)"), BOOLEAN, true);
    }

    private String getProjection(String functionName, String sketch, Object... args)
    {
        String otherArgs = Joiner.on(",").join(args);
        return String.format("%s(%s)", functionName, Joiner.on(",").join(sketch, otherArgs));
    }

    private String getSketchProjection(KllItemsSketch sketch, String type)
    {
        return getByteArrayProjection(sketch.toByteArray(), type);
    }

    private String getByteArrayProjection(byte[] arr, String type)
    {
        String sqlSerializedSketch = new SqlVarbinary(arr).toString().replaceAll("\\s+", " ");
        return String.format("CAST(X'%s' AS kllsketch(%s))", sqlSerializedSketch, type);
    }

    private static byte[] fromHex(String hex)
    {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }
}
