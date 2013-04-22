package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.Query;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

public class AnalysisContext
{
    private final AnalysisContext parent;
    private final Map<String, Query> namedQueries = new HashMap<>();

    public AnalysisContext(AnalysisContext parent)
    {
        this.parent = parent;
    }

    public AnalysisContext()
    {
        parent = null;
    }

    public void addNamedQuery(String name, Query query)
    {
        Preconditions.checkState(!namedQueries.containsKey(name), "Named query already registered: %s", name);
        namedQueries.put(name, query);
    }

    public Query getNamedQuery(String name)
    {
        Query result = namedQueries.get(name);

        if (result == null && parent != null) {
            return parent.getNamedQuery(name);
        }

        return result;
    }

    public boolean isNamedQueryDeclared(String name)
    {
        return namedQueries.containsKey(name);
    }
}
