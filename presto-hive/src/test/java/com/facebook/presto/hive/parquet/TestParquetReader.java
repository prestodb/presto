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
package com.facebook.presto.hive.parquet;

import org.testng.annotations.Test;

@Test
public class TestParquetReader
        extends AbstractTestParquetReader
{
    public TestParquetReader()
    {
        super(ParquetTester.quickParquetTester());
    }

    // Override all test methods so mvn test command can run individual method in test
    // e.g, without this, ./mvnw -Dtest=TestParquetReader#testEmptyArrays  test -B -Dair.check.skip-all -Dmaven.javadoc.skip=true -DLogTestDurationListener.enabled=true   -pl :presto-hive
    // will run empty test
    @Override
    public void testArray()
            throws Exception
    {
        super.testArray();
    }

    @Override
    public void testEmptyArrays()
            throws Exception
    {
        super.testEmptyArrays();
    }

    @Override
    public void testNestedArrays()
            throws Exception
    {
        super.testNestedArrays();
    }

    @Override
    public void testSingleLevelSchemaNestedArrays()
            throws Exception
    {
        super.testSingleLevelSchemaNestedArrays();
    }

    @Override
    public void testArrayOfStructs()
            throws Exception
    {
        super.testArrayOfStructs();
    }

    @Override
    public void testCustomSchemaArrayOfStructs()
            throws Exception
    {
        super.testCustomSchemaArrayOfStructs();
    }

    @Override
    public void testSingleLevelSchemaArrayOfStructs()
            throws Exception
    {
        super.testSingleLevelSchemaArrayOfStructs();
    }

    @Override
    public void testArrayOfArrayOfStructOfArray()
            throws Exception
    {
        super.testArrayOfArrayOfStructOfArray();
    }

    @Override
    public void testSingleLevelSchemaArrayOfArrayOfStructOfArray()
            throws Exception
    {
        super.testSingleLevelSchemaArrayOfArrayOfStructOfArray();
    }

    @Override
    public void testArrayOfStructOfArray()
            throws Exception
    {
        super.testArrayOfStructOfArray();
    }

    @Override
    public void testSingleLevelSchemaArrayOfStructOfArray()
            throws Exception
    {
        super.testSingleLevelSchemaArrayOfStructOfArray();
    }

    @Override
    public void testMap()
            throws Exception
    {
        super.testMap();
    }

    @Override
    public void testNestedMaps()
            throws Exception
    {
        super.testNestedMaps();
    }

    @Override
    public void testArrayOfMaps()
            throws Exception
    {
        super.testArrayOfMaps();
    }

    @Override
    public void testSingleLevelSchemaArrayOfMaps()
            throws Exception
    {
        super.testSingleLevelSchemaArrayOfMaps();
    }

    @Override
    public void testArrayOfMapOfStruct()
            throws Exception
    {
        super.testArrayOfMapOfStruct();
    }

    @Override
    public void testSingleLevelArrayOfMapOfStruct()
            throws Exception
    {
        super.testSingleLevelArrayOfMapOfStruct();
    }

    @Override
    public void testSingleLevelArrayOfStructOfSingleElement()
            throws Exception
    {
        super.testSingleLevelArrayOfStructOfSingleElement();
    }

    @Override
    public void testSingleLevelArrayOfStructOfStructOfSingleElement()
            throws Exception
    {
        super.testSingleLevelArrayOfStructOfStructOfSingleElement();
    }

    @Override
    public void testArrayOfMapOfArray()
            throws Exception
    {
        super.testArrayOfMapOfArray();
    }

    @Override
    public void testSingleLevelArrayOfMapOfArray()
            throws Exception
    {
        super.testSingleLevelArrayOfMapOfArray();
    }

    @Override
    public void testMapOfArrayValues()
            throws Exception
    {
        super.testMapOfArrayValues();
    }

    @Override
    public void testMapOfArrayKeys()
            throws Exception
    {
        super.testMapOfArrayKeys();
    }

    @Override
    public void testMapOfSingleLevelArray()
            throws Exception
    {
        super.testMapOfSingleLevelArray();
    }

    @Override
    public void testMapOfStruct()
            throws Exception
    {
        super.testMapOfStruct();
    }

    @Override
    public void testMapWithNullValues()
            throws Exception
    {
        super.testMapWithNullValues();
    }

    @Override
    public void testStruct()
            throws Exception
    {
        super.testStruct();
    }

    @Override
    public void testNestedStructs()
            throws Exception
    {
        super.testNestedStructs();
    }

    @Override
    public void testComplexNestedStructs()
            throws Exception
    {
        super.testComplexNestedStructs();
    }

    @Override
    public void testStructOfMaps()
            throws Exception
    {
        super.testStructOfMaps();
    }

    @Override
    public void testStructOfNullableMapBetweenNonNullFields()
            throws Exception
    {
        super.testStructOfNullableMapBetweenNonNullFields();
    }

    @Override
    public void testStructOfNullableArrayBetweenNonNullFields()
            throws Exception
    {
        super.testStructOfNullableArrayBetweenNonNullFields();
    }

    @Override
    public void testStructOfArrayAndPrimitive()
            throws Exception
    {
        super.testStructOfArrayAndPrimitive();
    }

    @Override
    public void testStructOfSingleLevelArrayAndPrimitive()
            throws Exception
    {
        super.testStructOfSingleLevelArrayAndPrimitive();
    }

    @Override
    public void testStructOfPrimitiveAndArray()
            throws Exception
    {
        super.testStructOfPrimitiveAndArray();
    }

    @Override
    public void testStructOfPrimitiveAndSingleLevelArray()
            throws Exception
    {
        super.testStructOfPrimitiveAndSingleLevelArray();
    }

    @Override
    public void testStructOfTwoArrays()
            throws Exception
    {
        super.testStructOfTwoArrays();
    }

    @Override
    public void testStructOfTwoNestedArrays()
            throws Exception
    {
        super.testStructOfTwoNestedArrays();
    }

    @Override
    public void testStructOfTwoNestedSingleLevelSchemaArrays()
            throws Exception
    {
        super.testStructOfTwoNestedSingleLevelSchemaArrays();
    }

    @Override
    public void testBooleanSequence()
            throws Exception
    {
        super.testBooleanSequence();
    }

    @Override
    public void testSmallIntSequence()
            throws Exception
    {
        super.testSmallIntSequence();
    }

    @Override
    public void testLongSequence()
            throws Exception
    {
        super.testLongSequence();
    }

    @Override
    public void testLongSequenceWithHoles()
            throws Exception
    {
        super.testLongSequenceWithHoles();
    }

    @Override
    public void testLongDirect()
            throws Exception
    {
        super.testLongDirect();
    }

    @Override
    public void testLongDirect2()
            throws Exception
    {
        super.testLongDirect2();
    }

    @Override
    public void testLongShortRepeat()
            throws Exception
    {
        super.testLongShortRepeat();
    }

    @Override
    public void testLongPatchedBase()
            throws Exception
    {
        super.testLongPatchedBase();
    }

    @Override
    public void testDecimalBackedByINT32()
            throws Exception
    {
        super.testDecimalBackedByINT32();
    }

    @Override
    public void testTimestampMicrosBackedByINT64()
            throws Exception
    {
        super.testTimestampMicrosBackedByINT64();
    }

    @Override
    public void testTimestampMillisBackedByINT64()
            throws Exception
    {
        super.testTimestampMillisBackedByINT64();
    }

    @Override
    public void testDecimalBackedByINT64()
            throws Exception
    {
        super.testDecimalBackedByINT64();
    }

    @Override
    public void testDecimalBackedByFixedLenByteArray()
            throws Exception
    {
        super.testDecimalBackedByFixedLenByteArray();
    }

    @Override
    public void testSchemaWithRepeatedOptionalRequiredFields()
            throws Exception
    {
        super.testSchemaWithRepeatedOptionalRequiredFields();
    }

    @Override
    public void testSchemaWithOptionalOptionalRequiredFields()
            throws Exception
    {
        super.testSchemaWithOptionalOptionalRequiredFields();
    }

    @Override
    public void testSchemaWithOptionalRequiredOptionalFields()
            throws Exception
    {
        super.testSchemaWithOptionalRequiredOptionalFields();
    }

    @Override
    public void testSchemaWithRequiredRequiredOptionalFields()
            throws Exception
    {
        super.testSchemaWithRequiredRequiredOptionalFields();
    }

    @Override
    public void testSchemaWithRequiredOptionalOptionalFields()
            throws Exception
    {
        super.testSchemaWithRequiredOptionalOptionalFields();
    }

    @Override
    public void testSchemaWithRequiredOptionalRequiredFields()
            throws Exception
    {
        super.testSchemaWithRequiredOptionalRequiredFields();
    }

    @Override
    public void testSchemaWithRequiredStruct()
            throws Exception
    {
        super.testSchemaWithRequiredStruct();
    }

    @Override
    public void testSchemaWithRequiredOptionalRequired2Fields()
            throws Exception
    {
        super.testSchemaWithRequiredOptionalRequired2Fields();
    }

    @Override
    public void testOldAvroArray()
            throws Exception
    {
        super.testOldAvroArray();
    }

    @Override
    public void testNewAvroArray()
            throws Exception
    {
        super.testNewAvroArray();
    }

    @Override
    public void testArraySchemas()
            throws Exception
    {
        super.testArraySchemas();
    }

    @Override
    public void testMapSchemas()
            throws Exception
    {
        super.testMapSchemas();
    }

    @Override
    public void testLongStrideDictionary()
            throws Exception
    {
        super.testLongStrideDictionary();
    }

    @Override
    public void testFloatSequence()
            throws Exception
    {
        super.testFloatSequence();
    }

    @Override
    public void testFloatNaNInfinity()
            throws Exception
    {
        super.testFloatNaNInfinity();
    }

    @Override
    public void testDoubleSequence()
            throws Exception
    {
        super.testDoubleSequence();
    }

    @Override
    public void testDoubleNaNInfinity()
            throws Exception
    {
        super.testDoubleNaNInfinity();
    }

    @Override
    public void testStringUnicode()
            throws Exception
    {
        super.testStringUnicode();
    }

    @Override
    public void testStringDirectSequence()
            throws Exception
    {
        super.testStringDirectSequence();
    }

    @Override
    public void testStringDictionarySequence()
            throws Exception
    {
        super.testStringDictionarySequence();
    }

    @Override
    public void testStringStrideDictionary()
            throws Exception
    {
        super.testStringStrideDictionary();
    }

    @Override
    public void testEmptyStringSequence()
            throws Exception
    {
        super.testEmptyStringSequence();
    }

    @Override
    public void testBinaryDirectSequence()
            throws Exception
    {
        super.testBinaryDirectSequence();
    }

    @Override
    public void testBinaryDictionarySequence()
            throws Exception
    {
        super.testBinaryDictionarySequence();
    }

    @Override
    public void testEmptyBinarySequence()
            throws Exception
    {
        super.testEmptyBinarySequence();
    }

    @Override
    public void testStructMaxReadBytes()
            throws Exception
    {
        super.testStructMaxReadBytes();
    }

    @Override
    public void testNullableNullCount()
    {
        super.testNullableNullCount();
    }

    @Override
    public void testArrayMaxReadBytes()
            throws Exception
    {
        super.testArrayMaxReadBytes();
    }

    @Override
    public void testMapMaxReadBytes()
            throws Exception
    {
        super.testMapMaxReadBytes();
    }

    @Override
    public void testCaching()
            throws Exception
    {
        super.testCaching();
    }
}
