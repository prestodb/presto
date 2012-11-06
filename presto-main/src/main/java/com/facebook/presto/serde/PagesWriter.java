package com.facebook.presto.serde;

import com.facebook.presto.operator.Page;

public interface PagesWriter
{
    /**
     * Appends the specified page to this serialization
     */
    PagesWriter append(Page page);
}
