package com.facebook.presto;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;



import java.util.Iterator;

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class GroupBy
        extends AbstractIterator<RunLengthEncodedBlock>
{
    private final Iterator<ValueBlock> groupBySource;

    private PeekingIterator<Pair> currentGroupByBlock;

    public GroupBy(Iterator<ValueBlock> keySource)
    {
        this.groupBySource = keySource;
    }

    @Override
    protected RunLengthEncodedBlock computeNext()
    {
        // if no more data, return null
        if (!advanceGroupByBlock()) {
            endOfData();
            return null;
        }

        // form a group from the current position, until the value changes
        Pair entry = currentGroupByBlock.next();
        Tuple groupByKey = entry.getValue();
        long startPosition = entry.getPosition();

        while (true) {
            // skip entries until the current key changes or we've consumed this block
            while (currentGroupByBlock.hasNext() && currentGroupByBlock.peek().getValue().equals(groupByKey)) {
                entry = currentGroupByBlock.next();
            }

            // stop if there is more data in the current block since the next entry will be for a new group
            if (currentGroupByBlock.hasNext()) {
                break;
            }

            // stop if we are at the end of the stream
            if (!groupBySource.hasNext()) {
                break;
            }

            // process the next block
            currentGroupByBlock = groupBySource.next().pairIterator();
        }

        long endPosition = entry.getPosition();
        Range range = Range.create(startPosition, endPosition);

        RunLengthEncodedBlock group = new RunLengthEncodedBlock(groupByKey, range);
        return group;
    }

    private boolean advanceGroupByBlock()
    {
        // does current block iterator have more data?
        if (currentGroupByBlock != null && currentGroupByBlock.hasNext()) {
            return true;
        }

        // are there more blocks?
        if (!groupBySource.hasNext()) {
            return false;
        }

        // advance to next block and open an iterator
        currentGroupByBlock = groupBySource.next().pairIterator();
        return true;
    }
}
