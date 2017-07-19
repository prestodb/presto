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
package com.facebook.presto.spi;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xuwan on 17-7-17.
 */
public class ColHistogram
{
    private List<Bucket> buckets = new ArrayList<Bucket>();
    private long rowcount;
    private double minVal;
    private double maxVal;
    private int bucketNum;

    public void addBucket(double begin, double end, int diff, int count)
    {
        buckets.add(new Bucket(begin, end, diff, count));
    }

    public List<Bucket> getBuckets()
    {
        return buckets;
    }

    public void setBuckets(List<Bucket> buckets)
    {
        this.buckets = buckets;
    }

    public String toString()
    {
        return "HIstogram:minVal:" + minVal + ", maxVal:" + maxVal + ", rowcount:" + rowcount + ", bucketNum:" + bucketNum;
    }

    public long getRowcount()
    {
        return rowcount;
    }

    public void setRowcount(long rowcount)
    {
        this.rowcount = rowcount;
    }

    public double getMinVal()
    {
        return minVal;
    }

    public void setMinVal(double minVal)
    {
        this.minVal = minVal;
    }

    public double getMaxVal()
    {
        return maxVal;
    }

    public void setMaxVal(double maxVal)
    {
        this.maxVal = maxVal;
    }

    public int getBucketNum()
    {
        return bucketNum;
    }

    public void setBucketNum(int bucketNum)
    {
        this.bucketNum = bucketNum;
    }
    public class Bucket
    {
        double begin;
        double end;
        int diff;
        int count;

        public Bucket(double begin, double end, int diff, int count)
        {
            this.begin = begin;
            this.end = end;
            this.diff = diff;
            this.count = count;
        }

        public double getBegin()
        {
            return begin;
        }

        public void setBegin(double begin)
        {
            this.begin = begin;
        }

        public double getEnd()
        {
            return end;
        }

        public void setEnd(double end)
        {
            this.end = end;
        }

        public int getDiff()
        {
            return diff;
        }

        public void setDiff(int diff)
        {
            this.diff = diff;
        }

        public int getCount()
        {
            return count;
        }

        public void setCount(int count)
        {
            this.count = count;
        }

        public String toString()
        {
            return "Bucket:begin:" + begin + ",end:" + end + ",diff:" + diff + ",count:" + count;
        }
    }
}
