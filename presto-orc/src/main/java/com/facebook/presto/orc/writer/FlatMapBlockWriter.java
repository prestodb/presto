package com.facebook.presto.orc.writer;

import com.facebook.presto.common.block.ColumnarMap;

public interface FlatMapBlockWriter extends ColumnWriter
{
    public void writeColumnarMap(ColumnarMap columnarMap);
}
