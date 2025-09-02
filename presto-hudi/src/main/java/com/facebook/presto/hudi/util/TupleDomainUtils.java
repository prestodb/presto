package com.facebook.presto.hudi.util;

import com.facebook.presto.common.predicate.TupleDomain;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
}