package com.facebook.presto.hive;

import com.google.common.base.Charsets;
import org.testng.annotations.Test;

import static com.facebook.presto.hive.HiveBooleanParser.parseHiveBoolean;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestHiveBooleanParser
{
    @Test
    public void testParse()
    {
        assertTrue(parseBoolean("true"));
        assertTrue(parseBoolean("TRUE"));
        assertTrue(parseBoolean("tRuE"));

        assertFalse(parseBoolean("false"));
        assertFalse(parseBoolean("FALSE"));
        assertFalse(parseBoolean("fAlSe"));

        assertNull(parseBoolean("true "));
        assertNull(parseBoolean(" true"));
        assertNull(parseBoolean("false "));
        assertNull(parseBoolean(" false"));
        assertNull(parseBoolean("t"));
        assertNull(parseBoolean("f"));
        assertNull(parseBoolean(""));
        assertNull(parseBoolean("blah"));
    }

    private Boolean parseBoolean(String s)
    {
        return parseHiveBoolean(s.getBytes(Charsets.US_ASCII), 0, s.length());
    }
}
