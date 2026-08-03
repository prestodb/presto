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

import org.apache.datasketches.common.ArrayOfBooleansSerDe;
import org.apache.datasketches.common.ArrayOfDoublesSerDe;
import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.kll.KllItemsSketch;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.stream.LongStream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Golden-byte generator and cross-engine validator for KLL sketch serialization.
 *
 * <h2>Purpose</h2>
 * <p>KLL sketches are serialized as VARBINARY and can cross the Java↔native boundary
 * (e.g. built by a Java worker and queried by a native worker, or persisted to Parquet
 * and read later).  The wire format must therefore be identical in both engines.
 *
 * <h2>Generating Java golden bytes (Java → Native direction)</h2>
 * <p>Run the {@code printGolden*Bytes} tests to stdout:
 * <pre>
 *   mvn -pl presto-main-base -Dtest=TestKllSketchGoldenBytes#printGoldenBigintBytes,\
 *       TestKllSketchGoldenBytes#printGoldenDoubleBytes,\
 *       TestKllSketchGoldenBytes#printGoldenVarcharBytes,\
 *       TestKllSketchGoldenBytes#printGoldenBooleanBytes test
 * </pre>
 * <p>Copy the printed hex strings into {@code kJava*GoldenHex} in
 * {@code KllSketchTest.cpp} and remove the {@code DISABLED_} prefix from the
 * {@code DISABLED_javaGoldenBytes*} tests.
 *
 * <h2>Verifying native golden bytes (Native → Java direction)</h2>
 * <p>Run the C++ printer tests with {@code --gtest_also_run_disabled_tests} and copy
 * the output into {@code NATIVE_*_GOLDEN_HEX} in {@link TestKllSketchFunctions}, then
 * set {@code enabled = true} on the {@code testNativeTo*RoundTrip} tests there.
 *
 * <h2>Canonical test inputs (must match C++ printers exactly)</h2>
 * <ul>
 *   <li>BIGINT / DOUBLE : 0..99 (100 items), k=200
 *   <li>VARCHAR         : 'a'..'z' (26 items), k=200
 *   <li>BOOLEAN         : i%3==0 for i in 0..99 (~34 true, ~66 false), k=200
 * </ul>
 */
public class TestKllSketchGoldenBytes
{
    // -------------------------------------------------------------------------
    // Java → Native: print hex so the C++ tests can embed them
    // -------------------------------------------------------------------------

    /** Prints the hex-encoded Java serialization of a BIGINT KLL sketch (values 0..99). */
    @Test
    public void printGoldenBigintBytes()
    {
        KllItemsSketch<Long> sketch = KllItemsSketch.newHeapInstance(200, Long::compareTo, new ArrayOfLongsSerDe());
        LongStream.range(0, 100).forEach(sketch::update);
        System.out.println("JAVA_BIGINT_GOLDEN_HEX: " + toHex(sketch.toByteArray()));
    }

    /** Prints the hex-encoded Java serialization of a DOUBLE KLL sketch (values 0.0..99.0). */
    @Test
    public void printGoldenDoubleBytes()
    {
        KllItemsSketch<Double> sketch = KllItemsSketch.newHeapInstance(200, Double::compareTo, new ArrayOfDoublesSerDe());
        for (double i = 0; i < 100; i++) {
            sketch.update(i);
        }
        System.out.println("JAVA_DOUBLE_GOLDEN_HEX: " + toHex(sketch.toByteArray()));
    }

    /** Prints the hex-encoded Java serialization of a VARCHAR KLL sketch ('a'..'z'). */
    @Test
    public void printGoldenVarcharBytes()
    {
        KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(200, String::compareTo, new ArrayOfStringsSerDe());
        Arrays.stream("abcdefghijklmnopqrstuvwxyz".split("")).forEach(sketch::update);
        System.out.println("JAVA_VARCHAR_GOLDEN_HEX: " + toHex(sketch.toByteArray()));
    }

    /**
     * Prints the hex-encoded Java serialization of a BOOLEAN KLL sketch.
     *
     * <p>Java uses {@link ArrayOfBooleansSerDe} which <em>bit-packs</em> items
     * (8 booleans per byte, LSB-first).  The native C++ engine must consume this
     * format via its transcoding layer ({@code deserializeBoolSketch} in
     * {@code KllSketchTypeTraits.h}), and must produce the same format when
     * serializing ({@code serializeBoolSketch}).
     */
    @Test
    public void printGoldenBooleanBytes()
    {
        KllItemsSketch<Boolean> sketch = KllItemsSketch.newHeapInstance(200, Boolean::compareTo, new ArrayOfBooleansSerDe());
        LongStream.range(0, 100).mapToObj(i -> i % 3 == 0).forEach(sketch::update);
        System.out.println("JAVA_BOOLEAN_GOLDEN_HEX: " + toHex(sketch.toByteArray()));
    }

    // -------------------------------------------------------------------------
    // Self-tests: verify the printed golden bytes are valid Java sketches
    // -------------------------------------------------------------------------

    /**
     * Sanity-check: the BIGINT golden bytes that we will embed in the C++ test
     * must be parseable by the Java engine and produce correct results.
     */
    @Test
    public void goldenBigintBytesAreValid()
    {
        KllItemsSketch<Long> sketch = KllItemsSketch.newHeapInstance(200, Long::compareTo, new ArrayOfLongsSerDe());
        LongStream.range(0, 100).forEach(sketch::update);
        byte[] bytes = sketch.toByteArray();
        assertNotNull(bytes);
        KllItemsSketch<Long> restored = KllItemsSketch.wrap(
                org.apache.datasketches.memory.Memory.wrap(bytes), Long::compareTo, new ArrayOfLongsSerDe());
        assertEquals(restored.getQuantile(0.0), Long.valueOf(0));
        assertEquals(restored.getQuantile(1.0), Long.valueOf(99));
        assertEquals(restored.getRank(49L), 0.5, 0.02);
    }

    /** Sanity-check: DOUBLE golden bytes roundtrip. */
    @Test
    public void goldenDoubleBytesAreValid()
    {
        KllItemsSketch<Double> sketch = KllItemsSketch.newHeapInstance(200, Double::compareTo, new ArrayOfDoublesSerDe());
        for (double i = 0; i < 100; i++) {
            sketch.update(i);
        }
        byte[] bytes = sketch.toByteArray();
        KllItemsSketch<Double> restored = KllItemsSketch.wrap(
                org.apache.datasketches.memory.Memory.wrap(bytes), Double::compareTo, new ArrayOfDoublesSerDe());
        assertEquals(restored.getQuantile(0.0), 0.0, 0.001);
        assertEquals(restored.getQuantile(1.0), 99.0, 0.001);
        assertEquals(restored.getRank(49.0), 0.5, 0.02);
    }

    /** Sanity-check: VARCHAR golden bytes roundtrip. */
    @Test
    public void goldenVarcharBytesAreValid()
    {
        KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(200, String::compareTo, new ArrayOfStringsSerDe());
        Arrays.stream("abcdefghijklmnopqrstuvwxyz".split("")).forEach(sketch::update);
        byte[] bytes = sketch.toByteArray();
        KllItemsSketch<String> restored = KllItemsSketch.wrap(
                org.apache.datasketches.memory.Memory.wrap(bytes), String::compareTo, new ArrayOfStringsSerDe());
        assertEquals(restored.getQuantile(0.0), "a");
        assertEquals(restored.getQuantile(1.0), "z");
        assertEquals(restored.getRank("m"), 0.5, 0.05);
    }

    /**
     * Sanity-check: BOOLEAN golden bytes roundtrip using
     * {@link ArrayOfBooleansSerDe} (bit-packed format).
     */
    @Test
    public void goldenBooleanBytesAreValid()
    {
        KllItemsSketch<Boolean> sketch = KllItemsSketch.newHeapInstance(200, Boolean::compareTo, new ArrayOfBooleansSerDe());
        LongStream.range(0, 100).mapToObj(i -> i % 3 == 0).forEach(sketch::update);
        byte[] bytes = sketch.toByteArray();
        KllItemsSketch<Boolean> restored = KllItemsSketch.wrap(
                org.apache.datasketches.memory.Memory.wrap(bytes), Boolean::compareTo, new ArrayOfBooleansSerDe());
        assertEquals(restored.getQuantile(0.0), Boolean.FALSE);
        assertEquals(restored.getQuantile(1.0), Boolean.TRUE);
        assertEquals(restored.getRank(false), 0.66, 0.05);
        assertEquals(restored.getRank(true), 1.0, 0.01);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static String toHex(byte[] bytes)
    {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }
        return sb.toString();
    }
}
