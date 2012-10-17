package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Preconditions;

import java.util.HashSet;
import java.util.Set;

public class NameGenerator
{
    private int relationCount = 0;
    private int fieldCount = 0;

    private final Set<String> existingAliases;
    private final Set<QualifiedName> existingRelations;

    public NameGenerator(Set<String> existingAliases, Set<QualifiedName> existingRelations)
    {
        Preconditions.checkNotNull(existingAliases, "existingAliases is null");
        Preconditions.checkNotNull(existingRelations, "existingRelations is null");

        this.existingAliases = new HashSet<>(existingAliases);
        this.existingRelations = new HashSet<>(existingRelations);
    }

    public String newRelationAlias()
    {
        String name = generateRelationAlias();
        while (existingRelations.contains(QualifiedName.of(name))) {
            name = generateRelationAlias();
        }

        existingRelations.add(QualifiedName.of(name));

        return name;
    }

    public String newFieldAlias()
    {
        String name = generateFieldAlias();
        while (existingAliases.contains(name)) {
            name = generateFieldAlias();
        }

        existingAliases.add(name);
        return name;
    }

    private String generateRelationAlias()
    {
        String result = "_R" + relationCount;
        ++relationCount;
        return result;
    }

    private String generateFieldAlias()
    {
        String result = "_a" + fieldCount;
        ++fieldCount;
        return result;
    }
}
