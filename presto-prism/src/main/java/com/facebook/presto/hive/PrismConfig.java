package com.facebook.presto.hive;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PrismConfig
{
    private static final Splitter REGION_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

    private Duration cacheTtl = new Duration(1, TimeUnit.HOURS);
    private String prismSmcTier = "prism.nssr";
    private List<String> allowedRegions;

    @NotNull
    public Duration getCacheTtl()
    {
        return cacheTtl;
    }

    @Config("prism.cache-ttl")
    public PrismConfig setCacheTtl(Duration cacheTtl)
    {
        this.cacheTtl = cacheTtl;
        return this;
    }

    @NotNull
    public String getPrismSmcTier()
    {
        return prismSmcTier;
    }

    @Config("prism.smc-tier")
    public PrismConfig setPrismSmcTier(String prismSmcTier)
    {
        this.prismSmcTier = prismSmcTier;
        return this;
    }

    @NotNull
    public List<String> getAllowedRegions()
    {
        return allowedRegions;
    }

    @Config("prism.allowed-regions")
    public PrismConfig setAllowedRegions(String allowedRegionsList)
    {
        this.allowedRegions = (allowedRegionsList == null) ? null : ImmutableList.copyOf(REGION_SPLITTER.split(allowedRegionsList));
        return this;
    }
}
