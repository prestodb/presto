package com.facebook.presto.hudi.util;

import com.facebook.presto.common.predicate.AllOrNoneValueSet;
import com.facebook.presto.common.predicate.DiscreteValues;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.EquatableValueSet;
import com.facebook.presto.common.predicate.EquatableValueSet.ValueEntry;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.Ranges;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

import static com.facebook.presto.common.predicate.TupleDomain.toLinkedMap;
import static com.facebook.presto.common.predicate.TupleDomain.withColumnDomains;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class TupleDomainUtils
{
    // Utility classes should not have a public or default constructor.
    private TupleDomainUtils() {}

    /**
     * Get all columns that are referenced in the provided tupleDomain predicates.
     */
    public static List<String> getReferencedColumns(TupleDomain<String> tupleDomain)
    {
        if (tupleDomain.getDomains().isEmpty()) {
            return List.of();
        }
        return tupleDomain.getDomains().get().keySet().stream().toList();
    }

    /**
     * Check if all of the provided source fields are referenced in the tupleDomain predicates.
     */
    public static boolean areAllFieldsReferenced(TupleDomain<String> tupleDomain, List<String> sourceFields)
    {
        Set<String> referenceColSet = new HashSet<>(TupleDomainUtils.getReferencedColumns(tupleDomain));
        Set<String> sourceFieldSet = new HashSet<>(sourceFields);

        return referenceColSet.containsAll(sourceFieldSet);
    }

    /**
     * Check if at least one of the provided source field is referenced in the tupleDomain predicates.
     */
    public static boolean areSomeFieldsReferenced(TupleDomain<String> tupleDomain, List<String> sourceFields)
    {
        Set<String> referenceColSet = new HashSet<>(TupleDomainUtils.getReferencedColumns(tupleDomain));
        for (String sourceField : sourceFields) {
            if (referenceColSet.contains(sourceField)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check all columns referencing sourceFields are either IN or EQUAL predicates.
     */
    public static boolean areDomainsInOrEqualOnly(TupleDomain<String> tupleDomain, List<String> sourceFields)
    {
        // If no recordKeys or no recordKeyDomains, return empty list
        if (sourceFields == null || sourceFields.isEmpty() || tupleDomain.isAll() || tupleDomain.isNone()) {
            return false;
        }

        Optional<Map<String, Domain>> domainsOpt = tupleDomain.getDomains();
        // Not really necessary, as tupleDomain.isNone() already checks for this
        if (domainsOpt.isEmpty()) {
            return false;
        }

        boolean areReferencedInOrEqual = true;
        for (String sourceField : sourceFields) {
            Domain domain = domainsOpt.get().get(sourceField);
            // For cases where sourceField does not exist in tupleDomain
            if (domain == null) {
                return false;
            }
            areReferencedInOrEqual &= (domain.isSingleValue() || isDiscreteSet(domain.getValues()));
        }
        return areReferencedInOrEqual;
    }

    /**
     * Checks if a specific Domain represents ONLY an 'IS NULL' constraint.
     * This means null is allowed, and no other non-null values are allowed.
     * Important: Not handling `= NULL` predicates as colA `= NULL` does not evaluate to TRUE or FALSE, it evaluates to UNKNOWN, which is treated as false.
     *
     * @param domain The Domain to check.
     * @return true if the domain represents 'IS NULL', false otherwise.
     */
    private static boolean isOnlyNullConstraint(Domain domain)
    {
        // Null must be allowed, and the ValueSet must allow *no* non-null values.
        return domain.isNullAllowed() && domain.getValues().isNone();
    }

    /**
     * Checks if a specific Domain represents ONLY an 'IS NOT NULL' constraint.
     * This means null is not allowed, and all non-null values are allowed (no other range/value restrictions).
     * Important: Not handling `!= NULL` or `<> NULL` predicates as this does not evaluate to TRUE or FALSE, it evaluates to UNKNOWN, which is treated as false.
     *
     * @param domain The Domain to check.
     * @return true if the domain represents 'IS NOT NULL', false otherwise.
     */
    private static boolean isOnlyNotNullConstraint(Domain domain)
    {
        // Null must *NOT* be allowed, and the ValueSet must allow *ALL* possible non-null values.
        return !domain.isNullAllowed() && domain.getValues().isAll();
    }

    /**
     * Overloaded function to test if a Domain contains null checks or not.
     *
     * @param domain The Domain to check.
     * @return true if the domain represents 'IS NOT NULL' or 'IS NULL', false otherwise.
     */
    public static boolean hasSimpleNullCheck(Domain domain)
    {
        return isOnlyNullConstraint(domain) || isOnlyNotNullConstraint(domain);
    }

    /**
     * Checks if a TupleDomain contains at least one column Domain that represents
     * exclusively an 'IS NULL' or 'IS NOT NULL' constraint.
     *
     * @param tupleDomain The TupleDomain to inspect.
     * @return true if a simple null check constraint exists, false otherwise.
     */
    public static boolean hasSimpleNullCheck(TupleDomain<String> tupleDomain)
    {
        // A 'None' TupleDomain implies contradiction, not a simple null check
        if (tupleDomain.isNone()) {
            return false;
        }
        Optional<Map<String, Domain>> domains = tupleDomain.getDomains();
        // An 'All' TupleDomain has no constraints
        if (domains.isEmpty()) {
            return false;
        }

        // Iterate through the domains for each column in the TupleDomain
        for (Domain domain : domains.get().values()) {
            if (hasSimpleNullCheck(domain)) {
                // Found a domain that is purely an IS NULL or IS NOT NULL check
                return true;
            }
        }
        // No domain matched the simple null check patterns
        return false;
    }

    public static <T> TupleDomain<T> filter(TupleDomain<T> tupleDomain, BiPredicate<T, Domain> predicate)
    {
        requireNonNull(predicate, "predicate is null");
        return transformDomains(tupleDomain, (key, domain) -> {
            if (!predicate.test(key, domain)) {
                return Domain.all(domain.getType());
            }
            return domain;
        });
    }

    public static <T> TupleDomain<T> transformDomains(TupleDomain<T> tupleDomain, BiFunction<T, Domain, Domain> transformation)
    {
        requireNonNull(transformation, "transformation is null");
        if (tupleDomain.isNone() || tupleDomain.isAll()) {
            return tupleDomain;
        }

        return withColumnDomains(tupleDomain.getDomains().get().entrySet().stream()
                .collect(toLinkedMap(
                        Map.Entry::getKey,
                        entry -> {
                            Domain newDomain = transformation.apply(entry.getKey(), entry.getValue());
                            return requireNonNull(newDomain, "newDomain is null");
                        })));
    }

    public static boolean isDiscreteSet(ValueSet valueSet)
    {
        if (valueSet instanceof AllOrNoneValueSet)
        {
            return false;
        } else if (valueSet instanceof EquatableValueSet equatableValueSet)
        {
            DiscreteValues discreteValues = equatableValueSet.getDiscreteValues();
            return discreteValues.isWhiteList() && !equatableValueSet.getDiscreteValues().getValues().isEmpty();
        } else if (valueSet instanceof SortedRangeSet sortedRangeSet)
        {
            Ranges ranges = sortedRangeSet.getRanges();
            List<Range> orderedRanges = ranges.getOrderedRanges();
            for (int i = 0; i < ranges.getRangeCount(); i++) {
                if (!orderedRanges.get(i).isSingleValue()) {
                    return false;
                }
            }
            return !sortedRangeSet.isNone();
        }
        return false;
    }

    public static List<Object> getDiscreteSet(ValueSet valueSet)
    {
        if (valueSet instanceof AllOrNoneValueSet)
        {
            throw new UnsupportedOperationException();
        } else if (valueSet instanceof EquatableValueSet equatableValueSet)
        {
            if (!isDiscreteSet(valueSet)) {
                throw new IllegalStateException("EquatableValueSet is not a discrete set");
            }
            return equatableValueSet.getEntries().stream()
                    .map(ValueEntry::getValue)
                    .toList();
        } else if (valueSet instanceof SortedRangeSet sortedRangeSet)
        {
            List<Object> values = new ArrayList<>(sortedRangeSet.getRangeCount());
            Ranges ranges = sortedRangeSet.getRanges();
            List<Range> orderedRanges = ranges.getOrderedRanges();
            for (int rangeIndex = 0; rangeIndex < sortedRangeSet.getRangeCount(); rangeIndex++) {
                Range range = orderedRanges.get(rangeIndex);
                try
                {
                    values.add(range.getSingleValue());
                } catch (IllegalStateException e)
                {
                    throw new IllegalStateException("SortedRangeSet is not a discrete set", e);
                }
            }
            return unmodifiableList(values);
        }

        throw new IllegalStateException("Unsupported value set type: " + valueSet.getClass().getName());
    }
}