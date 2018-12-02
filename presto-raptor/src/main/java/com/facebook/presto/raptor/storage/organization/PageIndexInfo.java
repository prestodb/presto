package com.facebook.presto.raptor.storage.organization;

import com.facebook.presto.spi.Page;

public class PageIndexInfo
{
    private Page currentPage;
    private int currentPosition;

    public PageIndexInfo(Page currentPage, int currentPosition)
    {
        this.currentPage = currentPage;
        this.currentPosition = currentPosition;
    }

    public Page getCurrentPage() { return currentPage; }

    public int getCurrentPosition() { return currentPosition; }
}
