package com.facebook.presto.ingest;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.google.common.base.Splitter;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.io.LineReader;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;

public class DelimitedBlockExtractor
        implements BlockExtractor
{
    private final List<ColumnDefinition> columnDefinitions;
    private final Splitter columnSplitter;

    public DelimitedBlockExtractor(List<ColumnDefinition> columnDefinitions, Splitter columnSplitter)
    {
        checkNotNull(columnDefinitions, "columnDefinitions is null");
        checkArgument(!columnDefinitions.isEmpty(), "must provide at least one column definition");
        checkNotNull(columnSplitter, "columnSplitter is null");

        this.columnDefinitions = ImmutableList.copyOf(columnDefinitions);
        this.columnSplitter = columnSplitter;
    }

    @Override
    public Iterator<UncompressedBlock> extract(Readable source)
    {
        checkNotNull(source, "source is null");
        return extract(new LineReaderIterator(new LineReader(source)));
    }

    public Iterator<UncompressedBlock> extract(Iterator<String> sourceIterator)
    {
        checkNotNull(sourceIterator, "sourceIterator is null");
        return new BlockIterator(sourceIterator, columnDefinitions, columnSplitter);
    }

    private static class BlockIterator
            extends AbstractIterator<UncompressedBlock>
    {
        private final Iterator<String> lineIterator;
        private final List<ColumnDefinition> columnDefinitions;
        private final Splitter columnSplitter;
        private final TupleInfo tupleInfo;
        private int position = 0;

        private BlockIterator(Iterator<String> lineIterator, List<ColumnDefinition> columnDefinitions, Splitter columnSplitter)
        {
            this.lineIterator = lineIterator;
            this.columnDefinitions = columnDefinitions;
            this.columnSplitter = columnSplitter;

            ImmutableList.Builder<TupleInfo.Type> builder = ImmutableList.builder();
            for (ColumnDefinition request : columnDefinitions) {
                builder.add(request.getType());
            }
            tupleInfo = new TupleInfo(builder.build());
        }

        @Override
        protected UncompressedBlock computeNext()
        {
            if (!lineIterator.hasNext()) {
                return endOfData();
            }

            BlockBuilder blockBuilder = new BlockBuilder(position, tupleInfo);

            do {
                String line = lineIterator.next();
                List<String> values = ImmutableList.copyOf(columnSplitter.split(line));
                for (ColumnDefinition definition : columnDefinitions) {
                    checkElementIndex(definition.getColumnIndex(), values.size(), "columnIndex larger than split size");
                    String value = values.get(definition.getColumnIndex());
                    definition.getType().getStringValueConverter().convert(value, blockBuilder);
                }
            } while (!blockBuilder.isFull() && lineIterator.hasNext());

            UncompressedBlock block = blockBuilder.build();
            position += block.getCount();

            return block;
        }
    }

    public static class ColumnDefinition
    {
        private final int columnIndex;
        private final TupleInfo.Type type;

        public ColumnDefinition(int columnIndex, TupleInfo.Type type)
        {
            this.columnIndex = columnIndex;
            this.type = type;
        }

        public int getColumnIndex()
        {
            return columnIndex;
        }

        public TupleInfo.Type getType()
        {
            return type;
        }
    }
}
