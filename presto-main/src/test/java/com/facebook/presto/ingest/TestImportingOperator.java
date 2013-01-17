/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.operator.BlockingOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.serde.BlocksFileWriter;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.io.NullOutputStream;
import com.google.common.io.OutputSupplier;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.OutputStream;

import static com.facebook.presto.operator.CancelTester.assertCancel;
import static com.facebook.presto.operator.CancelTester.createCancelableDataSource;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;

public class TestImportingOperator
{
    @Test
    public void testCancel()
            throws Exception
    {
        BlockingOperator blockingOperator = createCancelableDataSource(new TupleInfo(VARIABLE_BINARY), new TupleInfo(VARIABLE_BINARY));
        Operator operator = new ImportingOperator(blockingOperator, new BlocksFileWriter(BlocksFileEncoding.RAW, new OutputSupplier<OutputStream>() {
            @Override
            public OutputStream getOutput()
                    throws IOException
            {
                return new NullOutputStream();
            }
        }));
        assertCancel(operator, blockingOperator);
    }
}
