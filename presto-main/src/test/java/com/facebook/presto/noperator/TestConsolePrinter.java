/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.noperator;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.nblock.Block;
import com.facebook.presto.nblock.BlockBuilder;
import com.facebook.presto.noperator.ConsolePrinter.DelimitedTuplePrinter;
import com.facebook.presto.noperator.ConsolePrinter.RecordTuplePrinter;
import org.testng.annotations.Test;

import java.io.StringWriter;

import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.noperator.OperatorAssertions.createOperator;
import static com.google.common.base.Charsets.UTF_8;
import static org.testng.Assert.assertEquals;

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
        ConsolePrinter consolePrinter = new ConsolePrinter(createOperator(new Page(valueBlock)), new DelimitedTuplePrinter(writer, ","));

        OperatorAssertions.assertOperatorEquals(consolePrinter, createOperator(new Page(valueBlock)));

        assertEquals(writer.toString(), "0,zero\n1,one\n2,two\n");
    }

    @Test
    public void testDelimitedTuplePrinterMultipleChannels()
    {
        StringWriter writer = new StringWriter();
        ConsolePrinter consolePrinter = new ConsolePrinter(createOperator(new Page(valueBlock, valueBlock, valueBlock)), new DelimitedTuplePrinter(writer, ","));

        OperatorAssertions.assertOperatorEquals(consolePrinter, createOperator(new Page(valueBlock, valueBlock, valueBlock)));

        assertEquals(writer.toString(), "" +
                "0,zero,0,zero,0,zero\n" +
                "1,one,1,one,1,one\n" +
                "2,two,2,two,2,two\n");
    }

    @Test
    public void testRecordTuplePrinter()
    {

        StringWriter writer = new StringWriter();
        ConsolePrinter consolePrinter = new ConsolePrinter(createOperator(new Page(valueBlock)), new RecordTuplePrinter(writer));

        OperatorAssertions.assertOperatorEquals(consolePrinter, createOperator(new Page(valueBlock)));

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
    @Test
    public void testRecordTuplePrinterMultipleChannels()
    {

        StringWriter writer = new StringWriter();
        ConsolePrinter consolePrinter = new ConsolePrinter(createOperator(new Page(valueBlock, valueBlock, valueBlock)), new RecordTuplePrinter(writer));

        OperatorAssertions.assertOperatorEquals(consolePrinter, createOperator(new Page(valueBlock, valueBlock, valueBlock)));

        assertEquals(writer.toString(), "" +
                "0:\t0\n" +
                "1:\tzero\n" +
                "2:\t0\n" +
                "3:\tzero\n" +
                "4:\t0\n" +
                "5:\tzero\n" +
                "\n" +
                "0:\t1\n" +
                "1:\tone\n" +
                "2:\t1\n" +
                "3:\tone\n" +
                "4:\t1\n" +
                "5:\tone\n" +
                "\n" +
                "0:\t2\n" +
                "1:\ttwo\n" +
                "2:\t2\n" +
                "3:\ttwo\n" +
                "4:\t2\n" +
                "5:\ttwo\n" +
                "\n");
    }
}
