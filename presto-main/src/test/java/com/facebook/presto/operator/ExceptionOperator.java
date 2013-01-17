/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * The the page iterator from this operator will throw an exception if not closed before max pages are returned.
 */
public class ExceptionOperator
        implements Operator
{
    private final List<TupleInfo> tupleInfos;
    private final Page page;
    private final int maxPages;

    public ExceptionOperator(TupleInfo... tupleInfos)
    {
        this(null, 0, tupleInfos);
    }

    public ExceptionOperator(Page page, int maxPages, TupleInfo... tupleInfos)
    {
        this.tupleInfos = ImmutableList.copyOf(tupleInfos);
        this.page = page;
        this.maxPages = maxPages;
    }

    @Override
    public int getChannelCount()
    {
        return tupleInfos.size();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public PageIterator iterator(OperatorStats operatorStats)
    {
        return new ExceptionPageIterator(tupleInfos, page, maxPages);
    }

    private final class ExceptionPageIterator
            extends AbstractPageIterator
    {
        private final Page page;
        private final int maxPages;
        private int pageCount;

        public ExceptionPageIterator(List<TupleInfo> tupleInfos, Page page, int maxPages)
        {
            super(tupleInfos);
            this.page = page;
            this.maxPages = maxPages;
        }

        @Override
        protected Page computeNext()
        {
            if (pageCount >= maxPages) {
                throw new IllegalStateException("next should never be called");
            }
            pageCount++;
            return page;
        }

        @Override
        protected void doClose()
        {
        }
    }
}
