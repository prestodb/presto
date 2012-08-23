package com.facebook.presto;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Iterator;

public class ForwardingSeekableIterator<T extends Block>
    implements SeekableIterator<T>
{
    private final PeekingIterator<T> iterator;
    private T last;
    private T seekedTo;

    public ForwardingSeekableIterator(Iterator<T> iterator)
    {
        this.iterator = Iterators.peekingIterator(iterator);
    }

    @Override
    public T peek()
    {
        if (seekedTo != null) {
            return seekedTo;
        }

        return iterator.peek();
    }

    @Override
    public T next()
    {
        if (seekedTo != null) {
            last = seekedTo;
            seekedTo = null;
            return last;
        }

        last = iterator.next();
        return last;
    }

    @Override
    public boolean hasNext()
    {
        if (seekedTo != null) {
            return true;
        }

        return iterator.hasNext();
    }

    @Override
    public boolean seekTo(long position)
    {
        if (last != null) {
            Preconditions.checkArgument(position >= last.getRange().getStart(), "Cannot seek to position %s before current range [%s, %s]", position, last.getRange().getStart(), last.getRange().getEnd());
            if (last.getRange().contains(position)) {
                seekedTo = last;
                return true;
            }
        }

        while (iterator.hasNext()) {
            last = iterator.next();

            if (last.getRange().contains(position)) {
                seekedTo = last;
                return true;
            }
            else if (last.getRange().getStart() > position) {
                seekedTo = last;
                return false;
            }
        }

        seekedTo = null;
        last = null;
        return false;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

}
