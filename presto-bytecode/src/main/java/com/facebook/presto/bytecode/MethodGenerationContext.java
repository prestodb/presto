/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.bytecode;

import com.facebook.presto.bytecode.debug.LocalVariableNode;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class MethodGenerationContext
{
    private final MethodVisitor methodVisitor;

    private final Set<Scope> allEnteredScopes = new LinkedHashSet<>();
    private final Deque<ScopeContext> scopes = new ArrayDeque<>();

    private final Map<Variable, Integer> variableSlots = new HashMap<>();
    private int nextSlot;

    private int currentLineNumber = -1;

    public MethodGenerationContext(MethodVisitor methodVisitor)
    {
        this.methodVisitor = requireNonNull(methodVisitor, "methodVisitor is null");
    }

    public void enterScope(Scope scope)
    {
        requireNonNull(scope, "scope is null");

        checkArgument(!allEnteredScopes.contains(scope), "scope has already been entered");
        allEnteredScopes.add(scope);

        ScopeContext scopeContext = new ScopeContext(scope);
        scopes.addLast(scopeContext);

        for (Variable variable : scopeContext.getVariables()) {
            checkArgument(!"this".equals(variable.getName()) || nextSlot == 0, "The 'this' variable must be in slot 0");
            variableSlots.put(variable, nextSlot);
            nextSlot += Type.getType(variable.getType().getType()).getSize();
        }

        scopeContext.getStartLabel().accept(methodVisitor, this);
    }

    public void exitScope(Scope scope)
    {
        checkArgument(allEnteredScopes.contains(scope), "scope has not been entered");
        checkArgument(!scopes.isEmpty() && scope == scopes.peekLast().getScope(), "Scope is not top of the stack");

        ScopeContext scopeContext = scopes.removeLast();

        scopeContext.getEndLabel().accept(methodVisitor, this);

        for (Variable variable : scopeContext.getVariables()) {
            new LocalVariableNode(variable, scopeContext.getStartLabel(), scopeContext.getEndLabel()).accept(methodVisitor, this);
        }

        variableSlots.keySet().removeAll(scopeContext.getVariables());
    }

    public int getVariableSlot(Variable variable)
    {
        Integer slot = variableSlots.get(variable);
        checkArgument(slot != null, "Variable '%s' has not been assigned a slot", variable);
        return slot;
    }

    public boolean updateLineNumber(int lineNumber)
    {
        if (lineNumber == currentLineNumber) {
            return false;
        }

        currentLineNumber = lineNumber;
        return true;
    }

    private final class ScopeContext
    {
        private final Scope scope;
        private final ImmutableList<Variable> variables;

        private final LabelNode startLabel = new LabelNode("VariableStart");
        private final LabelNode endLabel = new LabelNode("VariableEnd");

        public ScopeContext(Scope scope)
        {
            this.scope = scope;
            this.variables = ImmutableList.copyOf(scope.getVariables());
        }

        public Scope getScope()
        {
            return scope;
        }

        public ImmutableList<Variable> getVariables()
        {
            return variables;
        }

        public LabelNode getStartLabel()
        {
            return startLabel;
        }

        public LabelNode getEndLabel()
        {
            return endLabel;
        }
    }
}
