package com.facebook.presto.operators;

import com.facebook.presto.BlockStream;
import com.facebook.presto.Cursor;
import com.facebook.presto.MaskedValueBlock;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.ValueBlock;
import com.facebook.presto.block.cursor.BlockCursor;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;

public class DataScan3
        implements BlockStream<ValueBlock>
{
    private final BlockStream<? extends ValueBlock> source;
    private final BlockStream<? extends ValueBlock> positions;

    public DataScan3(BlockStream<? extends ValueBlock> source, BlockStream<? extends ValueBlock> positions)
    {
        this.source = source;
        this.positions = positions;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return source.getTupleInfo();
    }

    @Override
    public Iterator<ValueBlock> iterator()
    {
        return new AbstractIterator<ValueBlock>()
        {
            Iterator<? extends ValueBlock> valueBlocks = source.iterator();
            Iterator<? extends ValueBlock> positionBlocks = positions.iterator();
            ValueBlock currentPositionBlock = positionBlocks.next();

            @Override
            protected ValueBlock computeNext()
            {
                while (valueBlocks.hasNext()) {
                    ValueBlock currentValueBlock = valueBlocks.next();

                    // advance current position block to value block
                    while (currentPositionBlock.getRange().getEnd() < currentValueBlock.getRange().getStart()) {
                        if (!positionBlocks.hasNext()) {
                            endOfData();
                            return null;
                        }
                        currentPositionBlock = positionBlocks.next();
                    }

                    // get all position blocks that overlap with the value block
                    ImmutableList.Builder<ValueBlock> positionsForCurrentBlock = ImmutableList.builder();
                    while (positionBlocks.hasNext() && currentPositionBlock.getRange().getEnd() < currentValueBlock.getRange().getEnd()) {
                        positionsForCurrentBlock.add(currentPositionBlock);
                        currentPositionBlock = positionBlocks.next();
                    }

                    // if current position block overlaps with value block, add it
                    if (currentPositionBlock.getRange().overlaps(currentValueBlock.getRange()))  {
                        positionsForCurrentBlock.add(currentPositionBlock);
                    }

                    // if the value block and the position blocks have and positions in common, output a block
                    List<Long> validPositions = getValidPositions(currentValueBlock, positionsForCurrentBlock.build());
                    if (!validPositions.isEmpty()) {
                        return new MaskedValueBlock(currentValueBlock, validPositions);
                    }
                }
                endOfData();
                return null;
            }

            private List<Long> getValidPositions(ValueBlock currentValueBlock, List<ValueBlock> positionsForCurrentBlock)
            {
                ImmutableList.Builder<Long> validPositions = ImmutableList.builder();

                BlockCursor valueCursor = currentValueBlock.blockCursor();
                valueCursor.advanceNextValue();

                for (ValueBlock positionBlock : positionsForCurrentBlock) {
                    BlockCursor positionCursor = positionBlock.blockCursor();
                    while (positionCursor.hasNextPosition()) {
                        positionCursor.advanceNextPosition();
                        long nextPosition = positionCursor.getPosition();
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
                }
                return validPositions.build();
            }
        };
    }

    @Override
    public Cursor cursor()
    {
        return new ValueCursor(getTupleInfo(), iterator());
    }
}
