/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.noperator;

import static com.facebook.presto.nblock.BlockAssertions.assertBlockEquals;
import static org.testng.Assert.assertEquals;

public final class PageAssertions
{
    private PageAssertions()
    {
    }

    public static void assertPageEquals(Page actualPage, Page expectedPage)
    {
        assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
        for (int i = 0; i < actualPage.getChannelCount(); i++) {
            assertBlockEquals(actualPage.getBlock(i), expectedPage.getBlock(i));
        }
    }
}
