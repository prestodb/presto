package com.facebook.presto.hive.clustering;

import javafx.util.Pair;
import java.util.List;

/*
 Use histogram to store distribution for a column.
 The value of histogram should be evenly distributed
 within the data range in terms of percentile.
 Example:
   (100, 1), (200, 3), (300, 5) Means:
   Value 1 is 25th percentile; there are 100 rows with values <= 1;
   Value 3 is 50th percentile; there are 200 rows with values <= 3;
   Value 5 is 75th percentile; there are 300 rows with values <= 5;
*/
public final class Distribution<T>
{
    private List<Pair<Integer, T>> distribution;

    public Distribution(List<Pair<Integer, T>> distribution)
    {
        this.distribution = distribution;
    }


}
