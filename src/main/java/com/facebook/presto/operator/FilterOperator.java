package com.facebook.presto.operator;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.MaskedBlock;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;

public class FilterOperator
        implements BlockStream, Iterable<Block>
{
    private final TupleInfo tupleInfo;
    private final Iterable<? extends Block> source;
    private final BlockStream positions;

    public FilterOperator(TupleInfo tupleInfo, Iterable<? extends Block> source, BlockStream positions)
    {
        this.tupleInfo = tupleInfo;
        this.source = source;
        this.positions = positions;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    public Iterator<Block> iterator()
    {
        return new AbstractIterator<Block>()
        {
            Iterator<? extends Block> valueBlocks = source.iterator();
            Cursor positionsCursor = positions.cursor();
            {
                positionsCursor.advanceNextPosition();
            }
            @Override
            protected Block computeNext()
            {
                while (valueBlocks.hasNext()) {
                    Block currentValueBlock = valueBlocks.next();

                    // advance current position cursor to value block
                    if (positionsCursor.getPosition() < currentValueBlock.getRange().getStart() &&
                            !positionsCursor.advanceToPosition(currentValueBlock.getRange().getStart())) {
                        // no more positions
                        endOfData();
                        return null;
                    }

                    // if the position cursor is past the current block end, continue with the next block
                    if (positionsCursor.getPosition() > currentValueBlock.getRange().getEnd()) {
                        continue;
                    }

                    // get all positions that overlap with the value block
                    ImmutableList.Builder<Long> positionsForCurrentBlock = ImmutableList.builder();
                    do {
                        positionsForCurrentBlock.add(positionsCursor.getPosition());
                    } while (positionsCursor.advanceNextPosition() && positionsCursor.getPosition() <= currentValueBlock.getRange().getEnd());

                    // if the value block and the position blocks have and positions in common, output a block
                    List<Long> validPositions = getValidPositions(currentValueBlock, positionsForCurrentBlock.build());
                    if (!validPositions.isEmpty()) {
                        return new MaskedBlock(currentValueBlock, validPositions);
                    }
                }
                endOfData();
                return null;
            }

            private List<Long> getValidPositions(Block currentValueBlock, List<Long> positionsForCurrentBlock)
            {
                ImmutableList.Builder<Long> validPositions = ImmutableList.builder();

                BlockCursor valueCursor = currentValueBlock.blockCursor();
                valueCursor.advanceNextPosition();

                for (Long nextPosition : positionsForCurrentBlock) {
                    if (nextPosition > valueCursor.getRange().getEnd()) {
                        break;
                    }
                    if (nextPosition > valueCursor.getPosition()) {
                        valueCursor.advanceToPosition(nextPosition);
                    }
                    if (valueCursor.getPosition() == nextPosition) {
                        validPositions.add(nextPosition);
                    }
                }
                return validPositions.build();
            }
        };
    }

    @Override
    public Cursor cursor()
    {
        return new GenericCursor(getTupleInfo(), iterator());
    }
}
