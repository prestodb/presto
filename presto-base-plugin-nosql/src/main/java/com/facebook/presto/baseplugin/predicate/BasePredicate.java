package com.facebook.presto.baseplugin.predicate;

import com.facebook.presto.baseplugin.BaseColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.weakref.jmx.internal.guava.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Created by amehta on 6/20/16.
 */
public class BasePredicate {
    private final BaseColumnHandle baseColumnHandle;
    private final BaseComparisonOperator baseComparisonOperator;
    private final List<Object> values;

    @JsonCreator
    public BasePredicate(
            @JsonProperty("baseColumnHandle") BaseColumnHandle baseColumnHandle,
            @JsonProperty("baseComparisonOperator") BaseComparisonOperator baseComparisonOperator,
            @JsonProperty("values") List<Object> values
    ) {
        this.baseColumnHandle = baseColumnHandle;
        this.baseComparisonOperator = baseComparisonOperator;
        this.values = values;
    }

    public static BasePredicate fromColumnDomain(TupleDomain.ColumnDomain<ColumnHandle> columnDomain)
    {
        Builder builder = new Builder()
                .setBaseColumnHandle((BaseColumnHandle) columnDomain.getColumn())
                .setBaseComparisonOperator(BaseComparisonOperator.NE);
        List<Range> ranges = columnDomain.getDomain().getValues().getRanges().getOrderedRanges();
        if (ranges.size() == 1) { //EQ,GT,LT,LTE,GTE,BETWEEN
            Range range = ranges.get(0);
            if (range.getLow().getBound() == Marker.Bound.ABOVE) {
                if (range.getHigh().getBound() == Marker.Bound.BELOW) {
                    if (range.getLow().getValueBlock().isPresent()) {
                        if (!range.getHigh().getValueBlock().isPresent()) {//GT
                            builder.setBaseComparisonOperator(BaseComparisonOperator.GT);
                            builder.addValues(range.getLow().getValue());
                        }
                    } else if (range.getHigh().getValueBlock().isPresent()){//LT
                        builder.setBaseComparisonOperator(BaseComparisonOperator.LT);
                        builder.addValues(range.getHigh().getValue());
                    }
                } else if (range.getHigh().getValueBlock().isPresent()){//high.bound = EXACTLY, LTE
                    builder.setBaseComparisonOperator(BaseComparisonOperator.LTE);
                    builder.addValues(range.getHigh().getValue());
                }
            } else { //low.bound = EXACTLY
                if (range.getHigh().getBound() == Marker.Bound.EXACTLY){ //EQ
                    if(range.isSingleValue()) {
                        builder.setBaseComparisonOperator(BaseComparisonOperator.EQUAL);
                        builder.addValues(range.getSingleValue());
                    } else if(range.getHigh().getValueBlock().isPresent() && range.getLow().getValueBlock().isPresent()){
                        builder.setBaseComparisonOperator(BaseComparisonOperator.BETWEEN);
                        builder.addValues(range.getLow().getValue(), range.getHigh().getValue());
                    }
                } else if(range.getLow().getValueBlock().isPresent()){//GTE
                    builder.setBaseComparisonOperator(BaseComparisonOperator.GTE);
                    builder.addValues(range.getLow().getValue());
                }
            }
        } else { //IN
            builder.setBaseComparisonOperator(BaseComparisonOperator.IN);
            ranges.forEach(x -> builder.addValues(x.getSingleValue()));
        }
        return builder.build();
    }

    public static String predicatesToString(List<BasePredicate> predicates){
        return predicates.stream().map(p -> p.toString()+"|").sorted().collect(Collectors.joining());
    }

    @JsonProperty
    public BaseColumnHandle getBaseColumnHandle() {
        return baseColumnHandle;
    }

    @JsonProperty
    public BaseComparisonOperator getBaseComparisonOperator() {
        return baseComparisonOperator;
    }

    @JsonProperty
    public List<Object> getValues() {
        return values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof BasePredicate){
            BasePredicate basePredicate = (BasePredicate) o;
            return new EqualsBuilder()
                    .append(baseColumnHandle, basePredicate.baseColumnHandle)
                    .append(baseComparisonOperator, basePredicate.baseComparisonOperator)
                    .append(values, basePredicate.values)
                    .isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(baseColumnHandle)
                .append(baseComparisonOperator)
                .append(values)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "BasePredicate{"+getBaseColumnHandle().getColumnName()+"."+getBaseComparisonOperator()+"."+getValues()+"}";
    }

    public static class Builder {
        private BaseColumnHandle baseColumnHandle;
        private BaseComparisonOperator baseComparisonOperator;
        private ImmutableList.Builder<Object> values;

        public Builder() {
            values = ImmutableList.builder();
        }

        public Builder setBaseColumnHandle(BaseColumnHandle baseColumnHandle) {
            this.baseColumnHandle = requireNonNull(baseColumnHandle, "baseColumnHandle is null");
            return this;
        }

        public Builder setBaseComparisonOperator(BaseComparisonOperator baseComparisonOperator) {
            this.baseComparisonOperator = requireNonNull(baseComparisonOperator, "baseComparisonOperator is null");
            return this;
        }

        public Builder addValues(Object... values){
            this.values.add(values);
            return this;
        }

        public BasePredicate build() {
            return new BasePredicate(baseColumnHandle, baseComparisonOperator, values.build());
        }
    }
}
