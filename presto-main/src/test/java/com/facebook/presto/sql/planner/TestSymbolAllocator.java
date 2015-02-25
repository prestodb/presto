package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.type.BigintType;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Set;

import static org.testng.Assert.assertEquals;

public class TestSymbolAllocator
{
    @Test
    public void testUnique()
            throws Exception
    {
        SymbolAllocator allocator = new SymbolAllocator();
        Set<Symbol> symbols = ImmutableSet.<Symbol>builder()
                .add(allocator.newSymbol("foo_1_0", BigintType.BIGINT))
                .add(allocator.newSymbol("foo", BigintType.BIGINT))
                .add(allocator.newSymbol("foo", BigintType.BIGINT))
                .add(allocator.newSymbol("foo", BigintType.BIGINT))
                .build();

        assertEquals(symbols.size(), 4);
    }
}
