package com.facebook.presto.hive.clustering;

import java.util.List;
import java.util.Map;

public final class DistributionExtractor
{
    private DistributionExtractor() {}

    Map<String, Distribution> distributions;

    // Extract distributions from table properties.
    //
    public void extractDistributions()
    {
        distributions = null;
    }

}
