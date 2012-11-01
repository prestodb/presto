package com.facebook.presto.serde;

import com.facebook.presto.noperator.Page;

public interface PagesWriter
{
    /**
     * Appends the specified page to this serialization
     */
    PagesWriter append(Page page);

    /**
     * Must be called after all blocks have been appended to complete the serialization
     */
    void finish();
}
