/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive.acid;

import com.google.common.annotations.VisibleForTesting;

import java.util.Arrays;

/**
 * An implementation of {@link org.apache.hadoop.hive.common.ValidTxnList} for use by readers.
 * This class will view a transaction as valid only if it is committed.  Both open and aborted
 * transactions will be seen as invalid.
 */
@SuppressWarnings("all")
public class ValidReadTxnList implements ValidTxnList {

    protected long[] exceptions;
    //default value means there are no open txn in the snapshot
    private long minOpenTxn = Long.MAX_VALUE;
    protected long highWatermark;

    public ValidReadTxnList() {
        this(new long[0], Long.MAX_VALUE, Long.MAX_VALUE);
    }

    /**
     * Used if there are no open transactions in the snapshot
     */
    public ValidReadTxnList(long[] exceptions, long highWatermark) {
        this(exceptions, highWatermark, Long.MAX_VALUE);
    }
    public ValidReadTxnList(long[] exceptions, long highWatermark, long minOpenTxn) {
        if (exceptions.length == 0) {
            this.exceptions = exceptions;
        } else {
            this.exceptions = exceptions.clone();
            Arrays.sort(this.exceptions);
            this.minOpenTxn = minOpenTxn;
            if(this.exceptions[0] <= 0) {
                //should never happen of course
                throw new IllegalArgumentException("Invalid txnid: " + this.exceptions[0] + " found");
            }
        }
        this.highWatermark = highWatermark;
    }

    public ValidReadTxnList(String value) {
        readFromString(value);
    }

    @Override
    public boolean isTxnValid(long txnid) {
        if (highWatermark < txnid) {
            return false;
        }
        return Arrays.binarySearch(exceptions, txnid) < 0;
    }

    /**
     * We cannot use a base file if its range contains an open txn.
     * @param txnid from base_xxxx
     */
    @Override
    public boolean isValidBase(long txnid) {
        return minOpenTxn > txnid && txnid <= highWatermark;
    }
    @Override
    public RangeResponse isTxnRangeValid(long minTxnId, long maxTxnId) {
        // check the easy cases first
        if (highWatermark < minTxnId) {
            return RangeResponse.NONE;
        } else if (exceptions.length > 0 && exceptions[0] > maxTxnId) {
            return RangeResponse.ALL;
        }

        // since the exceptions and the range in question overlap, count the
        // exceptions in the range
        long count = Math.max(0, maxTxnId - highWatermark);
        for(long txn: exceptions) {
            if (minTxnId <= txn && txn <= maxTxnId) {
                count += 1;
            }
        }

        if (count == 0) {
            return RangeResponse.ALL;
        } else if (count == (maxTxnId - minTxnId + 1)) {
            return RangeResponse.NONE;
        } else {
            return RangeResponse.SOME;
        }
    }

    @Override
    public String toString() {
        return writeToString();
    }

    @Override
    public String writeToString() {
        StringBuilder buf = new StringBuilder();
        buf.append(highWatermark);
        buf.append(':');
        buf.append(minOpenTxn);
        if (exceptions.length == 0) {
            buf.append(':');
        } else {
            for(long except: exceptions) {
                buf.append(':');
                buf.append(except);
            }
        }
        return buf.toString();
    }

    @Override
    public void readFromString(String src) {
        if (src == null || src.length() == 0) {
            highWatermark = Long.MAX_VALUE;
            exceptions = new long[0];
        } else {
            String[] values = src.split(":");
            highWatermark = Long.parseLong(values[0]);
            minOpenTxn = Long.parseLong(values[1]);
            exceptions = new long[values.length - 2];
            for(int i = 2; i < values.length; ++i) {
                exceptions[i-2] = Long.parseLong(values[i]);
            }
        }
    }

    @Override
    public long getHighWatermark() {
        return highWatermark;
    }

    @Override
    public long[] getInvalidTransactions() {
        return exceptions;
    }
    @VisibleForTesting
    public long getMinOpenTxn() {
        return minOpenTxn;
    }
}
