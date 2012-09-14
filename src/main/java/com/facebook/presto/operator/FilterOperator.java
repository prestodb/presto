package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.MaskedBlock;
import com.facebook.presto.block.TupleStream;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;

public class FilterOperator
        implements TupleStream, Iterable<TupleStream>
{
    private final TupleInfo tupleInfo;
    private final Iterable<? extends TupleStream> source;
    private final TupleStream positions;

    public FilterOperator(TupleInfo tupleInfo, Iterable<? extends TupleStream> source, TupleStream positions)
    {
        this.tupleInfo = tupleInfo;
        this.source = source;
        this.positions = positions;
    }

    @Override
    public Range getRange()
    {
        return Range.ALL;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    public Iterator<TupleStream> iterator()
    {
        return new AbstractIterator<TupleStream>()
        {
            Iterator<? extends TupleStream> valueBlocks = source.iterator();
            Cursor positionsCursor = positions.cursor();
            {
                positionsCursor.advanceNextPosition();
            }
            @Override
            protected TupleStream computeNext()
            {
                while (valueBlocks.hasNext()) {
                    TupleStream currentValueBlock = valueBlocks.next();

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

            private List<Long> getValidPositions(TupleStream currentValueBlock, List<Long> positionsForCurrentBlock)
            {
                ImmutableList.Builder<Long> validPositions = ImmutableList.builder();

                Cursor valueCursor = currentValueBlock.cursor();
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
