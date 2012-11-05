package com.facebook.presto.serde;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Block;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import static org.codehaus.jackson.annotate.JsonSubTypes.Type;

// TODO: switch the JSON encoding to operate on data representations that can be translated to serdes
// We should start considering doing this when the serdes start to have more member fields
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property="serde")
@JsonSubTypes({
        @Type(value = UncompressedBlockSerde.class, name = "raw"),
        @Type(value = RunLengthEncodedBlockSerde.class, name = "rle")
})
public interface BlockSerde
{
    BlocksWriter createBlockWriter(SliceOutput sliceOutput);
    void writeBlock(SliceOutput sliceOutput, Block block);
    Block readBlock(SliceInput sliceInput, TupleInfo tupleInfo, long positionOffset);
}
