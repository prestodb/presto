package com.facebook.presto.sql.parser;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.antlr.runtime.tree.Tree;

import java.util.List;

public final class TreePrinter
{
    private TreePrinter() {}

    public static String treeToString(Tree tree)
    {
        return treeToString(tree, 1);
    }

    private static String treeToString(Tree tree, int depth)
    {
        if (tree.getChildCount() == 0) {
            return quotedString(tree.toString());
        }
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        sb.append(tree.toString());
        for (Tree t : children(tree)) {
            if (hasSubtree(t) && (leafCount(tree) > 2)) {
                sb.append("\n");
                sb.append(Strings.repeat("   ", depth));
            }
            else {
                sb.append(" ");
            }
            sb.append(treeToString(t, depth + 1));
        }
        sb.append(")");
        return sb.toString();
    }

    private static String quotedString(String s)
    {
        return s.contains(" ") ? ('"' + s + '"') : s;
    }

    private static boolean hasSubtree(Tree tree)
    {
        for (Tree t : children(tree)) {
            if (t.getChildCount() > 0) {
                return true;
            }
        }
        return false;
    }

    private static int leafCount(Tree tree)
    {
        if (tree.getChildCount() == 0) {
            return 1;
        }

        int n = 0;
        for (Tree t : children(tree)) {
            n += leafCount(t);
        }
        return n;
    }

    private static List<Tree> children(Tree tree)
    {
        ImmutableList.Builder<Tree> list = ImmutableList.builder();
        for (int i = 0; i < tree.getChildCount(); i++) {
            list.add(tree.getChild(i));
        }
        return list.build();
    }
}
