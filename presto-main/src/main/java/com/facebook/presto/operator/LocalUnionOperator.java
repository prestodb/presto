package com.facebook.presto.operator;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.weakref.jmx.com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.List;

public class LocalUnionOperator
        implements Operator
{
    private final List<Operator> sources;

    public LocalUnionOperator(List<Operator> sources)
    {
        Preconditions.checkNotNull(sources, "sources is null");
        Preconditions.checkArgument(!sources.isEmpty(), "must have at least one source");
        this.sources = ImmutableList.copyOf(sources);
    }

    @Override
    public int getChannelCount()
    {
        return sources.get(0).getChannelCount();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return sources.get(0).getTupleInfos();
    }

    @Override
    public PageIterator iterator(OperatorStats operatorStats)
    {
        ImmutableList.Builder<PageIterator> builder = ImmutableList.builder();
        for (Operator source : sources) {
            builder.add(source.iterator(operatorStats));
        }
        return new LocalUnionOperatorIterator(sources.get(0).getTupleInfos(), builder.build());
    }

    private static class LocalUnionOperatorIterator
            extends AbstractPageIterator
    {
        private final List<PageIterator> sources;
        private final Iterator<Page> iterator;

        protected LocalUnionOperatorIterator(Iterable<TupleInfo> tupleInfos, List<PageIterator> sources)
        {
            super(tupleInfos);
            this.sources = sources;
            iterator = Iterators.concat(sources.iterator());
        }

        @Override
        protected void doClose()
        {
            for (PageIterator source : sources) {
                source.close();
            }
        }

        @Override
        protected Page computeNext()
        {
            if (!iterator.hasNext()) {
                return endOfData();
            }
            return iterator.next();
        }
    }
}
