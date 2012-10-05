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

    public static enum Encoding
    {
        RAW("raw")
                {
                    @Override
                    public TupleStreamSerde createSerde()
                    {
                        return new UncompressedSerde();
                    }
                },
        RLE("rle")
                {
                    @Override
                    public TupleStreamSerde createSerde()
                    {
                        return new RunLengthEncodedSerde();
                    }
                },
        DICTIONARY_RAW("dicraw")
                {
                    @Override
                    public TupleStreamSerde createSerde()
                    {
                        return new DictionarySerde(RAW.createSerde());
                    }
                },
        DICTIONARY_RLE("dicrle")
                {
                    @Override
                    public TupleStreamSerde createSerde()
                    {
                        return new DictionarySerde(RLE.createSerde());
                    }
                };

        private static final Map<String, Encoding> NAME_MAP;
        static {
            ImmutableMap.Builder<String, Encoding> builder = ImmutableMap.builder();
            for (Encoding encoding : Encoding.values()) {
                builder.put(encoding.getName(), encoding);
            }
            NAME_MAP = builder.build();
        }

        // Name should be usable as a filename
        private final String name;

        private Encoding(String name)
        {
            this.name = checkNotNull(name, "name is null");
        }

        public String getName()
        {
            return name;
        }

        public abstract TupleStreamSerde createSerde();

        public static Encoding fromName(String name)
        {
            checkNotNull(name, "name is null");
            Encoding encoding = NAME_MAP.get(name);
            checkArgument(encoding != null, "Invalid type name: %s", name);
            return encoding;
        }
    }
}
