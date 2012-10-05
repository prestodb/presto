package com.facebook.presto.ingest;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.AbstractYieldingIterator;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.google.common.base.Splitter;
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
    private final Splitter columnSplitter;
    private final List<ColumnDefinition> columnDefinitions;

    public DelimitedBlockExtractor(Splitter columnSplitter, List<ColumnDefinition> columnDefinitions)
    {
        checkNotNull(columnSplitter, "columnSplitter is null");
        checkNotNull(columnDefinitions, "columnDefinitions is null");
        checkArgument(!columnDefinitions.isEmpty(), "must provide at least one column definition");

        this.columnSplitter = columnSplitter;
        this.columnDefinitions = ImmutableList.copyOf(columnDefinitions);
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
        return new DelimitedYieldingIterator(sourceIterator, columnSplitter, columnDefinitions);
    }

    private static class DelimitedYieldingIterator
            extends AbstractYieldingIterator<UncompressedBlock>
    {
        private final Iterator<String> lineIterator;
        private final Splitter columnSplitter;
        private final List<ColumnDefinition> columnDefinitions;
        private final TupleInfo tupleInfo;
        private int position = 0;

        private DelimitedYieldingIterator(Iterator<String> lineIterator, Splitter columnSplitter, List<ColumnDefinition> columnDefinitions)
        {
            this.lineIterator = lineIterator;
            this.columnSplitter = columnSplitter;
            this.columnDefinitions = columnDefinitions;

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
            checkArgument(columnIndex >= 0, "columnIndex must be greater than or equal to zero");
            checkNotNull(type, "type is null");
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
