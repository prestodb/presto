/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * QueryState contains the current state of the query and output buffer.
 */
@ThreadSafe
public class QueryState
{
    private static enum State
    {
        RUNNING,
        FINISHED,
        CANCELED,
        FAILED
    }

    @GuardedBy("blockBuffer")
    private final ArrayDeque<UncompressedBlock> blockBuffer;

    @GuardedBy("blockBuffer")
    private State state = State.RUNNING;

    @GuardedBy("blockBuffer")
    private final List<Throwable> causes = new ArrayList<>();

    @GuardedBy("blockBuffer")
    private int sourceCount;

    private final Semaphore notFull;
    private final Semaphore notEmpty;

    public QueryState(int sourceCount, int blockBufferMax)
    {
        Preconditions.checkArgument(sourceCount > 0, "sourceCount must be at least 1");
        Preconditions.checkArgument(blockBufferMax > 0, "blockBufferMax must be at least 1");

        this.sourceCount = sourceCount;
        this.blockBuffer = new ArrayDeque<>(blockBufferMax);
        this.notFull = new Semaphore(blockBufferMax);
        this.notEmpty = new Semaphore(0);
    }

    public boolean isDone()
    {
        synchronized (blockBuffer) {
            return state != State.RUNNING;
        }
    }

    public boolean isFailed()
    {
        synchronized (blockBuffer) {
            return state == State.FAILED;
        }
    }

    public boolean isCanceled()
    {
        synchronized (blockBuffer) {
            return state == State.CANCELED;
        }
    }

    /**
     * Marks a source as finished and drop all buffered blocks.  Once all sources are finished, no more blocks can be added to the buffer.
     */
    public void cancel()
    {
        synchronized (blockBuffer) {
            if (state != State.RUNNING) {
                return;
            }

            state = State.CANCELED;
            sourceCount = 0;
            blockBuffer.clear();
            notEmpty.release();

            // Just to be safe release most notFull permits, so threads blocked in addNextBlock are released
            // the 1000 is to avoid overflow problems due to programing bugs
            notFull.release(Integer.MAX_VALUE - notFull.availablePermits() - 1000);
        }
    }

    /**
     * Marks a source as finished.  Once all sources are finished, no more blocks can be added to the buffer.
     */
    public void sourceFinished()
    {
        synchronized (blockBuffer) {
            if (state != State.RUNNING) {
                return;
            }
            sourceCount--;
            if (sourceCount == 0) {
                if (blockBuffer.isEmpty()) {
                    state = State.FINISHED;
                }
                notEmpty.release();

                // Just to be safe release most notFull permits, so threads blocked in addNextBlock are released
                // the 1000 is to avoid overflow problems due to programing bugs
                notFull.release(Integer.MAX_VALUE - notFull.availablePermits() - 1000);
            }
        }
    }

    /**
     * Marks the query as failed and finished.  Once the query if failed, no more blocks can be added to the buffer.
     */
    public void queryFailed(Throwable cause)
    {
        synchronized (blockBuffer) {
            // if query is already done, nothing can be done here
            if (state == State.CANCELED || state == State.FINISHED) {
                causes.add(cause);
                return;
            }
            state = State.FAILED;
            causes.add(cause);
            sourceCount = 0;
            blockBuffer.clear();
            notEmpty.release();

            // Just to be safe release most notFull permits, so threads blocked in addNextBlock are released
            // the 1000 is to avoid overflow problems due to programing bugs
            notFull.release(Integer.MAX_VALUE - notFull.availablePermits() - 1000);
        }
    }

    /**
     * Add a block to the buffer.  The buffers space is limited, so the caller will be blocked until
     * space is available in the buffer.
     *
     * @throws InterruptedException the thread is interrupted while waiting for buffer space to be freed
     * @throws IllegalStateException if the query is finished
     */
    public void addBlock(UncompressedBlock block)
            throws InterruptedException
    {
        notFull.acquire();
        synchronized (blockBuffer) {
            // don't throw an exception if the query was canceled or failed as the caller may not be aware of this
            if (state == State.CANCELED || state == State.FAILED) {
                // release additional threads blocked in the code above
                notFull.release();
                return;
            }

            // if all sources are finished throw an exception
            if (sourceCount == 0) {
                // release additional threads blocked in the code above
                notFull.release();
                throw new IllegalStateException("All sources are finished");
            }
            blockBuffer.addLast(block);
            notEmpty.release();
        }
    }

    /**
     * Gets the next blocks from the buffer.  The caller will block until at least one block is available.
     *
     * @throws InterruptedException the thread is interrupted while waiting for blocks to be buffered
     */
    public List<UncompressedBlock> getNextBlocks(int maxBlockCount)
            throws InterruptedException
    {
        Preconditions.checkArgument(maxBlockCount > 0, "blockBufferMax must be at least 1");

        // block until first block is available
        notEmpty.acquire();
        synchronized (blockBuffer) {
            // verify state
            if (state == State.CANCELED || state == State.FINISHED) {
                // allow thread blocked in getNextBlocks (above) through
                // todo do we want a cancel exception
                notEmpty.release();
                return ImmutableList.of();
            }
            if (state == State.FAILED) {
                // allow thread blocked in getNextBlocks (above) through
                notEmpty.release();
                throw new FailedQueryException(causes);
            }

            // acquire all available blocks up to the limit
            ImmutableList.Builder<UncompressedBlock> nextBlocks = ImmutableList.builder();
            int count = 0;
            // use a do while because we have "reserved" a block in the above acquire call
            do {
                if (blockBuffer.isEmpty()) {
                    // there is one extra permit when all sources are finished
                    Preconditions.checkState(sourceCount == 0, "All sources to be finished");

                    // allow thread blocked in getNextBlocks (above) through
                    notEmpty.release();
                    break;
                }
                else {
                    nextBlocks.add(blockBuffer.removeFirst());
                }
                count++;
                // tryAcquire can fail even if more blocks are available, because the blocks may have been "reserved" by the above acquire call
            } while (count < maxBlockCount && notEmpty.tryAcquire());

            // allow blocks to be replaced
            notFull.release(count);

            // check for end condition
            List<UncompressedBlock> blocks = nextBlocks.build();
            if (sourceCount == 0 && blockBuffer.isEmpty()) {
                state = State.FINISHED;
            }

            return blocks;
        }
    }
}
