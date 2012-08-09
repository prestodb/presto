package com.facebook.presto;

import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

public class DataScan3
        extends AbstractIterator<ValueBlock>
{
    private final Iterator<? extends ValueBlock> source;
    private final Iterator<? extends PositionBlock> positions;

    private ValueBlock valueBlock;
    private PositionBlock positionBlock;

    public DataScan3(Iterator<? extends ValueBlock> source, Iterator<? extends PositionBlock> positions)
    {
        this.source = source;
        this.positions = positions;
    }

    @Override
    protected ValueBlock computeNext()
    {
        while (advance()) {
            if (valueBlock.getRange().isConnected(positionBlock.getRange())) {
                ValueBlock result = valueBlock.filter(positionBlock);
                if (!result.isEmpty()) {
                    return result;
                }
            }
        }

        endOfData();
        return null;
    }

    private boolean advance()
    {
        if (valueBlock != null && positionBlock != null) {
            if (valueBlock.getRange().upperEndpoint() < positionBlock.getRange().upperEndpoint()) {
                valueBlock = null;
            }
            else {
                positionBlock = null;
            }
        }

        if (valueBlock == null && source.hasNext()) {
            valueBlock = source.next();
        }

        if (positionBlock == null && positions.hasNext()) {
            positionBlock = positions.next();
        }

        return valueBlock != null && positionBlock != null;
    }

}
