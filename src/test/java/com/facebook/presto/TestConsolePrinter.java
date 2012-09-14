/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.operator.ConsolePrinter;
import com.facebook.presto.operator.ConsolePrinter.DelimitedTuplePrinter;
import com.facebook.presto.operator.ConsolePrinter.RecordTuplePrinter;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.io.StringWriter;

import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Charsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestConsolePrinter
{

    private final Block valueBlock;

    public TestConsolePrinter()
    {
        BlockBuilder builder = new BlockBuilder(0, new TupleInfo(FIXED_INT_64, VARIABLE_BINARY));
        builder.append(0);
        builder.append("zero".getBytes(UTF_8));
        builder.append(1);
        builder.append("one".getBytes(UTF_8));
        builder.append(2);
        builder.append("two".getBytes(UTF_8));
        valueBlock = builder.build();
    }

    @Test
    public void testDelimitedTuplePrinter()
    {

        StringWriter writer = new StringWriter();
        ConsolePrinter consolePrinter = new ConsolePrinter(ImmutableList.of(valueBlock).iterator(), new DelimitedTuplePrinter(writer, ","));

        assertTrue(consolePrinter.hasNext());
        assertSame(consolePrinter.next(), valueBlock);

        assertEquals(writer.toString(), "0,zero\n1,one\n2,two\n");
    }

    @Test
    public void testRecordTuplePrinter()
    {

        StringWriter writer = new StringWriter();
        ConsolePrinter consolePrinter = new ConsolePrinter(ImmutableList.of(valueBlock).iterator(), new RecordTuplePrinter(writer));

        assertTrue(consolePrinter.hasNext());
        assertSame(consolePrinter.next(), valueBlock);

        assertEquals(writer.toString(), "0:\t0\n" +
                "1:\tzero\n" +
                "\n" +
                "0:\t1\n" +
                "1:\tone\n" +
                "\n" +
                "0:\t2\n" +
                "1:\ttwo\n" +
                "\n");
    }
}
