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

import com.facebook.presto.bytecode.control.CaseStatement;
import com.facebook.presto.bytecode.control.DoWhileLoop;
import com.facebook.presto.bytecode.control.ForLoop;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.control.SwitchStatement;
import com.facebook.presto.bytecode.control.TryCatch;
import com.facebook.presto.bytecode.control.WhileLoop;
import com.facebook.presto.bytecode.debug.LineNumberNode;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.instruction.Constant.BooleanConstant;
import com.facebook.presto.bytecode.instruction.Constant.BoxedBooleanConstant;
import com.facebook.presto.bytecode.instruction.Constant.BoxedDoubleConstant;
import com.facebook.presto.bytecode.instruction.Constant.BoxedFloatConstant;
import com.facebook.presto.bytecode.instruction.Constant.BoxedIntegerConstant;
import com.facebook.presto.bytecode.instruction.Constant.BoxedLongConstant;
import com.facebook.presto.bytecode.instruction.Constant.ClassConstant;
import com.facebook.presto.bytecode.instruction.Constant.DoubleConstant;
import com.facebook.presto.bytecode.instruction.Constant.FloatConstant;
import com.facebook.presto.bytecode.instruction.Constant.IntConstant;
import com.facebook.presto.bytecode.instruction.Constant.LongConstant;
import com.facebook.presto.bytecode.instruction.Constant.StringConstant;
import com.facebook.presto.bytecode.instruction.InstructionNode;
import com.facebook.presto.bytecode.instruction.InvokeInstruction;
import com.facebook.presto.bytecode.instruction.InvokeInstruction.InvokeDynamicInstruction;
import com.facebook.presto.bytecode.instruction.JumpInstruction;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.bytecode.instruction.VariableInstruction.IncrementVariableInstruction;
import com.facebook.presto.bytecode.instruction.VariableInstruction.LoadVariableInstruction;
import com.facebook.presto.bytecode.instruction.VariableInstruction.StoreVariableInstruction;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.facebook.presto.bytecode.Access.INTERFACE;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static java.lang.String.format;

public class DumpBytecodeVisitor
        extends BytecodeVisitor<Void>
{
    private final PrintWriter out;
    private int indentLevel;

    public DumpBytecodeVisitor(Writer out)
    {
        this.out = new PrintWriter(out);
    }

    @Override
    public Void visitClass(ClassDefinition classDefinition)
    {
        // print annotations first
        for (AnnotationDefinition annotationDefinition : classDefinition.getAnnotations()) {
            visitAnnotation(classDefinition, annotationDefinition);
        }

        // print class declaration
        Line classDeclaration = line().addAll(classDefinition.getAccess());
        if (!classDefinition.getAccess().contains(INTERFACE)) {
            classDeclaration.add("class");
        }
        classDeclaration.add(classDefinition.getType().getJavaClassName());
        if (!classDefinition.getSuperClass().equals(type(Object.class))) {
            classDeclaration.add("extends").add(classDefinition.getSuperClass().getJavaClassName());
        }
        if (!classDefinition.getInterfaces().isEmpty()) {
            classDeclaration.add("implements");
            for (ParameterizedType interfaceType : classDefinition.getInterfaces()) {
                classDeclaration.add(interfaceType.getJavaClassName());
            }
        }
        classDeclaration.print();

        // print class body
        printLine("{");
        indentLevel++;

        // print fields
        for (FieldDefinition fieldDefinition : classDefinition.getFields()) {
            visitField(classDefinition, fieldDefinition);
        }

        // print methods
        for (MethodDefinition methodDefinition : classDefinition.getMethods()) {
            visitMethod(classDefinition, methodDefinition);
        }

        // print class initializer
        visitMethod(classDefinition, classDefinition.getClassInitializer());

        indentLevel--;
        printLine("}");
        printLine();
        return null;
    }

    @Override
    public Void visitAnnotation(Object parent, AnnotationDefinition annotationDefinition)
    {
        printLine("@%s", annotationDefinition.getType().getJavaClassName(), annotationDefinition.getValues());
        return null;
    }

    @Override
    public Void visitField(ClassDefinition classDefinition, FieldDefinition fieldDefinition)
    {
        // print annotations first
        for (AnnotationDefinition annotationDefinition : fieldDefinition.getAnnotations()) {
            visitAnnotation(fieldDefinition, annotationDefinition);
        }

        // print field declaration
        line().addAll(fieldDefinition.getAccess()).add(fieldDefinition.getType().getJavaClassName()).add(fieldDefinition.getName()).add(";").print();

        printLine();
        return null;
    }

    @Override
    public Void visitMethod(ClassDefinition classDefinition, MethodDefinition methodDefinition)
    {
        if (methodDefinition.getComment() != null) {
            printLine("// %s", methodDefinition.getComment());
        }
        // print annotations first
        for (AnnotationDefinition annotationDefinition : methodDefinition.getAnnotations()) {
            visitAnnotation(methodDefinition, annotationDefinition);
        }

        // print method declaration
        printLine(methodDefinition.toSourceString());

        // print body
        methodDefinition.getBody().accept(null, this);

        printLine();
        return null;
    }

    @Override
    public Void visitComment(BytecodeNode parent, Comment node)
    {
        printLine();
        printLine("// %s", node.getComment());
        return null;
    }

    @Override
    public Void visitBlock(BytecodeNode parent, BytecodeBlock block)
    {
        // only indent if we have a block description or more than one child node
        boolean indented;
        if (block.getDescription() != null) {
            line().add(block.getDescription()).add("{").print();
            indentLevel++;
            indented = true;
        }
        else if (block.getChildNodes().size() > 1) {
            printLine("{");
            indentLevel++;
            indented = true;
        }
        else {
            indented = false;
        }

        visitBlockContents(block);
        if (indented) {
            indentLevel--;
            printLine("}");
        }

        return null;
    }

    private void visitBlockContents(BytecodeBlock block)
    {
        for (BytecodeNode node : block.getChildNodes()) {
            if (node instanceof BytecodeBlock) {
                BytecodeBlock childBlock = (BytecodeBlock) node;
                if (childBlock.getDescription() != null) {
                    visitBlock(block, childBlock);
                }
                else {
                    visitBlockContents(childBlock);
                }
            }
            else {
                node.accept(node, this);
            }
        }
    }

    @Override
    public Void visitBytecodeExpression(BytecodeNode parent, BytecodeExpression expression)
    {
        printLine(expression.toString());
        return null;
    }

    @Override
    public Void visitNode(BytecodeNode parent, BytecodeNode node)
    {
        printLine(node.toString());
        super.visitNode(parent, node);
        return null;
    }

    @Override
    public Void visitLabel(BytecodeNode parent, LabelNode labelNode)
    {
        printLine("%s:", labelNode.getName());
        return null;
    }

    @Override
    public Void visitJumpInstruction(BytecodeNode parent, JumpInstruction jumpInstruction)
    {
        printLine("%s %s", jumpInstruction.getOpCode(), jumpInstruction.getLabel().getName());
        return null;
    }

    //
    // Variable
    //

    @Override
    public Void visitLoadVariable(BytecodeNode parent, LoadVariableInstruction loadVariableInstruction)
    {
        Variable variable = loadVariableInstruction.getVariable();
        printLine("load %s", variable.getName());
        return null;
    }

    @Override
    public Void visitStoreVariable(BytecodeNode parent, StoreVariableInstruction storeVariableInstruction)
    {
        Variable variable = storeVariableInstruction.getVariable();
        printLine("store %s)", variable.getName());
        return null;
    }

    @Override
    public Void visitIncrementVariable(BytecodeNode parent, IncrementVariableInstruction incrementVariableInstruction)
    {
        Variable variable = incrementVariableInstruction.getVariable();
        byte increment = incrementVariableInstruction.getIncrement();
        printLine("increment %s %s", variable.getName(), increment);
        return null;
    }

    //
    // Invoke
    //

    @Override
    public Void visitInvoke(BytecodeNode parent, InvokeInstruction invokeInstruction)
    {
        printLine("invoke %s.%s%s",
                invokeInstruction.getTarget().getJavaClassName(),
                invokeInstruction.getName(),
                invokeInstruction.getMethodDescription());
        return null;
    }

    @Override
    public Void visitInvokeDynamic(BytecodeNode parent, InvokeDynamicInstruction invokeDynamicInstruction)
    {
        printLine("invokeDynamic %s%s %s",
                invokeDynamicInstruction.getName(),
                invokeDynamicInstruction.getMethodDescription(),
                invokeDynamicInstruction.getBootstrapArguments());
        return null;
    }

    //
    // Control Flow
    //

    @Override
    public Void visitTryCatch(BytecodeNode parent, TryCatch tryCatch)
    {
        if (tryCatch.getComment() != null) {
            printLine();
            printLine("// %s", tryCatch.getComment());
        }

        printLine("try {");
        indentLevel++;
        tryCatch.getTryNode().accept(tryCatch, this);
        indentLevel--;
        printLine("}");

        printLine("catch (%s) {", tryCatch.getExceptionName());
        indentLevel++;
        tryCatch.getCatchNode().accept(tryCatch, this);
        indentLevel--;
        printLine("}");

        return null;
    }

    @Override
    public Void visitIf(BytecodeNode parent, IfStatement ifStatement)
    {
        if (ifStatement.getComment() != null) {
            printLine();
            printLine("// %s", ifStatement.getComment());
        }
        printLine("if {");
        indentLevel++;
        visitNestedNode("condition", ifStatement.condition(), ifStatement);
        if (!ifStatement.ifTrue().isEmpty()) {
            visitNestedNode("ifTrue", ifStatement.ifTrue(), ifStatement);
        }
        if (!ifStatement.ifFalse().isEmpty()) {
            visitNestedNode("ifFalse", ifStatement.ifFalse(), ifStatement);
        }
        indentLevel--;
        printLine("}");
        return null;
    }

    @Override
    public Void visitFor(BytecodeNode parent, ForLoop forLoop)
    {
        if (forLoop.getComment() != null) {
            printLine();
            printLine("// %s", forLoop.getComment());
        }
        printLine("for {");
        indentLevel++;
        visitNestedNode("initialize", forLoop.initialize(), forLoop);
        visitNestedNode("condition", forLoop.condition(), forLoop);
        visitNestedNode("update", forLoop.update(), forLoop);
        visitNestedNode("body", forLoop.body(), forLoop);
        indentLevel--;
        printLine("}");
        return null;
    }

    @Override
    public Void visitWhile(BytecodeNode parent, WhileLoop whileLoop)
    {
        if (whileLoop.getComment() != null) {
            printLine();
            printLine("// %s", whileLoop.getComment());
        }
        printLine("while {");
        indentLevel++;
        visitNestedNode("condition", whileLoop.condition(), whileLoop);
        visitNestedNode("body", whileLoop.body(), whileLoop);
        indentLevel--;
        printLine("}");
        return null;
    }

    @Override
    public Void visitDoWhile(BytecodeNode parent, DoWhileLoop doWhileLoop)
    {
        if (doWhileLoop.getComment() != null) {
            printLine();
            printLine("// %s", doWhileLoop.getComment());
        }
        printLine("while {");
        indentLevel++;
        visitNestedNode("body", doWhileLoop.body(), doWhileLoop);
        visitNestedNode("condition", doWhileLoop.condition(), doWhileLoop);
        indentLevel--;
        printLine("}");
        return null;
    }

    @Override
    public Void visitSwitch(BytecodeNode parent, SwitchStatement switchStatement)
    {
        if (switchStatement.getComment() != null) {
            printLine();
            printLine("// %s", switchStatement.getComment());
        }
        printLine("switch {");
        indentLevel++;
        visitNestedNode("expression", switchStatement.expression(), switchStatement);
        for (CaseStatement caseStatement : switchStatement.cases()) {
            visitNestedNode(format("case %s:", caseStatement.getKey()), caseStatement.getBody(), switchStatement);
        }
        if (switchStatement.getDefaultBody() != null) {
            visitNestedNode("default:", switchStatement.getDefaultBody(), switchStatement);
        }
        indentLevel--;
        printLine("}");
        return null;
    }

    //
    // Instructions
    //

    @Override
    public Void visitInstruction(BytecodeNode parent, InstructionNode node)
    {
        return super.visitInstruction(parent, node);
    }

    //
    // Constants
    //

    @Override
    public Void visitBoxedBooleanConstant(BytecodeNode parent, BoxedBooleanConstant boxedBooleanConstant)
    {
        printLine("load constant %s", boxedBooleanConstant.getValue());
        return null;
    }

    @Override
    public Void visitBooleanConstant(BytecodeNode parent, BooleanConstant booleanConstant)
    {
        printLine("load constant %s", booleanConstant.getValue());
        return null;
    }

    @Override
    public Void visitIntConstant(BytecodeNode parent, IntConstant intConstant)
    {
        printLine("load constant %s", intConstant.getValue());
        return null;
    }

    @Override
    public Void visitBoxedIntegerConstant(BytecodeNode parent, BoxedIntegerConstant boxedIntegerConstant)
    {
        printLine("load constant new Integer(%s)", boxedIntegerConstant.getValue());
        return null;
    }

    @Override
    public Void visitFloatConstant(BytecodeNode parent, FloatConstant floatConstant)
    {
        printLine("load constant %sf", floatConstant.getValue());
        return null;
    }

    @Override
    public Void visitBoxedFloatConstant(BytecodeNode parent, BoxedFloatConstant boxedFloatConstant)
    {
        printLine("load constant new Float(%sf)", boxedFloatConstant.getValue());
        return null;
    }

    @Override
    public Void visitLongConstant(BytecodeNode parent, LongConstant longConstant)
    {
        printLine("load constant %sL", longConstant.getValue());
        return null;
    }

    @Override
    public Void visitBoxedLongConstant(BytecodeNode parent, BoxedLongConstant boxedLongConstant)
    {
        printLine("load constant new Long(%sL)", boxedLongConstant.getValue());
        return null;
    }

    @Override
    public Void visitDoubleConstant(BytecodeNode parent, DoubleConstant doubleConstant)
    {
        printLine("load constant %s", doubleConstant.getValue());
        return null;
    }

    @Override
    public Void visitBoxedDoubleConstant(BytecodeNode parent, BoxedDoubleConstant boxedDoubleConstant)
    {
        printLine("load constant new Double(%s)", boxedDoubleConstant.getValue());
        return null;
    }

    @Override
    public Void visitStringConstant(BytecodeNode parent, StringConstant stringConstant)
    {
        printLine("load constant \"%s\"", stringConstant.getValue());
        return null;
    }

    @Override
    public Void visitClassConstant(BytecodeNode parent, ClassConstant classConstant)
    {
        printLine("load constant %s.class", classConstant.getValue().getJavaClassName());
        return null;
    }

    //
    // Line Number
    //

    @Override
    public Void visitLineNumber(BytecodeNode parent, LineNumberNode lineNumberNode)
    {
        lineNumber = lineNumberNode.getLineNumber();
        printLine("LINE %s", lineNumber);
        return null;
    }

    //
    // Print
    //

    private int lineNumber = -1;

    public void printLine()
    {
        out.println(indent(indentLevel));
    }

    public void printLine(String line)
    {
        out.println(format("%s%s", indent(indentLevel), line));
    }

    public void printLine(String format, Object... args)
    {
        String line = format(format, args);
        out.println(format("%s%s", indent(indentLevel), line));
    }

    public void printWords(String... words)
    {
        String line = Joiner.on(" ").join(words);
        out.println(format("%s%s", indent(indentLevel), line));
    }

    private String indent(int level)
    {
        return Strings.repeat("    ", level);
    }

    private void visitNestedNode(String description, BytecodeNode node, BytecodeNode parent)
    {
        printLine(description + " {");
        indentLevel++;
        node.accept(parent, this);
        indentLevel--;
        printLine("}");
    }

    private Line line()
    {
        return new Line();
    }

    private Line line(String separator)
    {
        return new Line(separator);
    }

    private class Line
    {
        private final String separator;
        private final List<Object> parts = new ArrayList<>();

        private Line()
        {
            separator = " ";
        }

        private Line(String separator)
        {
            this.separator = separator;
        }

        public Line add(Object element)
        {
            parts.add(element);
            return this;
        }

        public Line addAll(Collection<?> c)
        {
            parts.addAll(c);
            return this;
        }

        public void print()
        {
            printLine(Joiner.on(separator).join(parts));
        }

        @Override
        public String toString()
        {
            return Joiner.on(separator).join(parts);
        }
    }
}
