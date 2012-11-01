package com.facebook.presto.serde;

import com.facebook.presto.Range;
import com.facebook.presto.nblock.Blocks;
import com.facebook.presto.slice.SliceInput;
import com.google.common.io.InputSupplier;

public interface BlockDeserializer
{
    /**
     * Extract the blocks that has been serialized to the Slice.
     * In most cases, this will provide a view on top of the specified Slice and assumes
     * that the contents of the underlying Slice will not be changing.
     */
    Blocks deserializeBlocks(Range totalRange, InputSupplier<SliceInput> sliceInputSupplier);
}
