package com.facebook.presto.operator.scalar;

import org.testng.annotations.Test;

import static com.facebook.presto.operator.scalar.FunctionAssertions.assertFunction;
import static java.lang.String.format;

public class TestJsonFunctions
{
    @Test
    public void testJsonArrayLength()
    {
        assertFunction("JSON_ARRAY_LENGTH('[]')", 0);
        assertFunction("JSON_ARRAY_LENGTH('[1]')", 1);
        assertFunction("JSON_ARRAY_LENGTH('[1, \"foo\", null]')", 3);
        assertFunction("JSON_ARRAY_LENGTH('[2, 4, {\"a\": [8, 9]}, [], [5], 4]')", 6);
    }

    @Test
    public void testJsonArrayContainsBoolean()
    {
        assertFunction("JSON_ARRAY_CONTAINS('[]', true)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[true]', true)", true);
        assertFunction("JSON_ARRAY_CONTAINS('[false]', false)", true);
        assertFunction("JSON_ARRAY_CONTAINS('[true, false]', false)", true);
        assertFunction("JSON_ARRAY_CONTAINS('[false, true]', true)", true);
        assertFunction("JSON_ARRAY_CONTAINS('[1]', true)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[[true]]', true)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[1, \"foo\", null, \"true\"]', true)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[2, 4, {\"a\": [8, 9]}, [], [5], false]', false)", true);
    }

    @Test
    public void testJsonArrayContainsLong()
    {
        assertFunction("JSON_ARRAY_CONTAINS('[]', 1)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[3]', 3)", true);
        assertFunction("JSON_ARRAY_CONTAINS('[-4]', -4)", true);
        assertFunction("JSON_ARRAY_CONTAINS('[1.0]', 1)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[[2]]', 2)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[1, \"foo\", null, \"8\"]', 8)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[2, 4, {\"a\": [8, 9]}, [], [5], 6]', 6)", true);
        assertFunction("JSON_ARRAY_CONTAINS('[92233720368547758071]', -9)", false);
    }

    @Test
    public void testJsonArrayContainsDouble()
    {
        assertFunction("JSON_ARRAY_CONTAINS('[]', 1)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[1.5]', 1.5)", true);
        assertFunction("JSON_ARRAY_CONTAINS('[-9.5]', -9.5)", true);
        assertFunction("JSON_ARRAY_CONTAINS('[1]', 1.0)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[[2.5]]', 2.5)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[1, \"foo\", null, \"8.2\"]', 8.2)", false);
        assertFunction("JSON_ARRAY_CONTAINS('[2, 4, {\"a\": [8, 9]}, [], [5], 6.1]', 6.1)", true);
        assertFunction("JSON_ARRAY_CONTAINS('[9.6E400]', 4.2)", false);
    }

    @Test
    public void testJsonArrayContainsString()
    {

        assertFunction("JSON_ARRAY_CONTAINS('[]', 'x')", false);
        assertFunction("JSON_ARRAY_CONTAINS('[\"foo\"]', 'foo')", true);
        assertFunction("JSON_ARRAY_CONTAINS('[\"8\"]', '8')", true);
        assertFunction("JSON_ARRAY_CONTAINS('[1, \"foo\", null]', 'foo')", true);
        assertFunction("JSON_ARRAY_CONTAINS('[1, 5]', '5')", false);
        assertFunction("JSON_ARRAY_CONTAINS('[2, 4, {\"a\": [8, 9]}, [], [5], \"6\"]', '6')", true);
    }

    @Test
    public void testJsonArrayContainsInvalid()
    {
        for (String value : new String[] {"'x'", "2.5", "8", "true"}) {
            for (String array : new String[] {"", "123", "[", "[1,0,]", "[1,,0]"}) {
                assertFunction(format("JSON_ARRAY_CONTAINS('%s', %s)", array, value), null);
            }
        }
    }
}
