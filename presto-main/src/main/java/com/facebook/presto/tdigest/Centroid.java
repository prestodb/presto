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

/*
 * Licensed to Ted Dunning under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.tdigest;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

/**
 * A single centroid which represents a number of data points.
 */
@NotThreadSafe
public class Centroid
        implements Comparable<Centroid>, Serializable
{
    private static final AtomicInteger uniqueCount = new AtomicInteger(1);

    private double centroid;
    private double count;

    // The ID is transient because it must be unique within a given JVM. A new
    // ID should be generated from uniqueCount when a Centroid is deserialized.
    private transient int id;

    public Centroid(double x)
    {
        start(x, 1, uniqueCount.getAndIncrement());
    }

    public Centroid(double x, double w)
    {
        start(x, w, uniqueCount.getAndIncrement());
    }

    public Centroid(double x, double w, int id)
    {
        start(x, w, id);
    }

    private void start(double x, double w, int id)
    {
        this.id = id;
        add(x, w);
    }

    public void add(double x, double w)
    {
        count += w;
        centroid += w * (x - centroid) / count;
    }

    public double getMean()
    {
        return centroid;
    }

    public double getWeight()
    {
        return count;
    }

    public int getId()
    {
        return id;
    }

    @Override
    public String toString()
    {
        return "Centroid{" +
                "mean=" + centroid +
                ", count=" + count +
                '}';
    }

    @Override
    public int hashCode()
    {
        return id;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Centroid centroid = (Centroid) o;
        return this.centroid == centroid.getMean() && this.count == centroid.getWeight();
    }

    @Override
    public int compareTo(Centroid o)
    {
        requireNonNull(o);
        int r = Double.compare(centroid, o.centroid);
        if (r == 0) {
            r = id - o.id;
        }
        return r;
    }
}
