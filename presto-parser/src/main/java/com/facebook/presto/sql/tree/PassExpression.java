package com.facebook.presto.sql.tree;

import java.util.Objects;

public class PassExpression
        extends Literal {
    private final String value;

    public PassExpression(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitPassExpression(this, context);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getValue());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        PassExpression o = (PassExpression) obj;
        return Objects.equals(value, o.value);
    }
}
