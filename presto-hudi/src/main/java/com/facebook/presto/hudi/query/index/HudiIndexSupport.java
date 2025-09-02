package com.facebook.presto.hudi.query.index;

import com.facebook.presto.common.predicate.TupleDomain;
import org.apache.hudi.common.model.FileSlice;

public interface HudiIndexSupport
{
    boolean canApply(TupleDomain<String> tupleDomain);

    default boolean shouldSkipFileSlice(FileSlice slice)
    {
        return false;
    }
}
