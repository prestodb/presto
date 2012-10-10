package com.facebook.presto.block;

import com.facebook.presto.block.dictionary.DictionarySerde;
import com.facebook.presto.block.rle.RunLengthEncodedSerde;
import com.facebook.presto.block.uncompressed.UncompressedSerde;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.codehaus.jackson.annotate.JsonSubTypes.Type;

// TODO: switch the JSON encoding to operate on data representations that can be translated to serdes
// We should start considering doing this when the serdes start to have more member fields
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property="serde")
@JsonSubTypes({
        @Type(value = RunLengthEncodedSerde.class, name = "rle"),
        @Type(value = DictionarySerde.class, name = "dic"),
        @Type(value = UncompressedSerde.class, name = "raw")
})
public interface TupleStreamSerde
{
    TupleStreamSerializer createSerializer();
    TupleStreamDeserializer createDeserializer();
}
