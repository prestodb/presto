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
package com.facebook.presto.byteCode;

import com.facebook.presto.byteCode.control.CaseStatement;
import com.facebook.presto.byteCode.control.DoWhileLoop;
import com.facebook.presto.byteCode.control.ForLoop;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.control.LookupSwitch;
import com.facebook.presto.byteCode.control.WhileLoop;
import com.facebook.presto.byteCode.debug.LineNumberNode;
import com.facebook.presto.byteCode.instruction.Constant.BoxedBooleanConstant;
import com.facebook.presto.byteCode.instruction.Constant.BoxedDoubleConstant;
import com.facebook.presto.byteCode.instruction.Constant.BoxedFloatConstant;
import com.facebook.presto.byteCode.instruction.Constant.BoxedIntegerConstant;
import com.facebook.presto.byteCode.instruction.Constant.BoxedLongConstant;
import com.facebook.presto.byteCode.instruction.Constant.ClassConstant;
import com.facebook.presto.byteCode.instruction.Constant.DoubleConstant;
import com.facebook.presto.byteCode.instruction.Constant.FloatConstant;
import com.facebook.presto.byteCode.instruction.Constant.IntConstant;
import com.facebook.presto.byteCode.instruction.Constant.LongConstant;
import com.facebook.presto.byteCode.instruction.Constant.StringConstant;
import com.facebook.presto.byteCode.instruction.InstructionNode;
import com.facebook.presto.byteCode.instruction.InvokeInstruction;
import com.facebook.presto.byteCode.instruction.InvokeInstruction.InvokeDynamicInstruction;
import com.facebook.presto.byteCode.instruction.JumpInstruction;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.byteCode.instruction.VariableInstruction.IncrementVariableInstruction;
import com.facebook.presto.byteCode.instruction.VariableInstruction.LoadVariableInstruction;
import com.facebook.presto.byteCode.instruction.VariableInstruction.StoreVariableInstruction;
import com.google.common.base.Joiner;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.facebook.presto.byteCode.ParameterizedType.type;

public class DumpByteCodeVisitor
        extends ByteCodeVisitor<Void>
{
    private final PrintStream out;
    private int indentLevel;

    public DumpByteCodeVisitor(PrintStream out)
    {
        this.out = out;
    }

    @Override
    public Void visitClass(ClassDefinition classDefinition)
    {
        // print annotations first
        for (AnnotationDefinition annotationDefinition : classDefinition.getAnnotations()) {
            visitAnnotation(classDefinition, annotationDefinition);
        }

        // print class declaration
        Line classDeclaration = line().addAll(classDefinition.getAccess()).add("class").add(classDefinition.getType().getJavaClassName());
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
        Line methodDeclaration = line().addAll(methodDefinition.getAccess()).add(methodDefinition.getReturnType().getJavaClassName());
        if (!methodDefinition.getParameters().isEmpty()) {
            Line parameters = line(", ");
            for (NamedParameterDefinition parameterDefinition : methodDefinition.getParameters()) {
                parameters.add(line().add(parameterDefinition.getType().getJavaClassName()).add(parameterDefinition.getName()));
            }
            methodDeclaration.add(methodDefinition.getName() + "(" + parameters + ")");
        }
        else {
            methodDeclaration.add(methodDefinition.getName() + "()");
        }
        methodDeclaration.print();

        // print body
        methodDefinition.getBody().accept(null, this);

        printLine();
        return null;
    }

    @Override
    public Void visitComment(ByteCodeNode parent, Comment node)
    {
        printLine();
        printLine("// %s", node.getComment());
        return null;
    }

    @Override
    public Void visitBlock(ByteCodeNode parent, Block block)
    {
        if (block.getDescription() != null) {
            line().add(block.getDescription()).add("{").print();
        }
        else {
            printLine("{");
        }
        indentLevel++;
        visitBlockContents(block);
        indentLevel--;
        printLine("}");
        return null;
    }

    private void visitBlockContents(Block block)
    {
        for (ByteCodeNode node : block.getChildNodes()) {
            if (node instanceof Block) {
                Block childBlock = (Block) node;
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
    public Void visitNode(ByteCodeNode parent, ByteCodeNode node)
    {
        printLine(node.toString());
        super.visitNode(parent, node);
        return null;
    }

    @Override
    public Void visitLabel(ByteCodeNode parent, LabelNode labelNode)
    {
        printLine("%s:", labelNode.getName());
        return null;
    }

    @Override
    public Void visitJumpInstruction(ByteCodeNode parent, JumpInstruction jumpInstruction)
    {
        printLine("%s %s", jumpInstruction.getOpCode(), jumpInstruction.getLabel().getName());
        return null;
    }

    //
    // Variable
    //

    @Override
    public Void visitLoadVariable(ByteCodeNode parent, LoadVariableInstruction loadVariableInstruction)
    {
        LocalVariableDefinition variable = loadVariableInstruction.getVariable();
        printLine("load %s(#%d)", variable.getName(), variable.getSlot());
        return null;
    }

    @Override
    public Void visitStoreVariable(ByteCodeNode parent, StoreVariableInstruction storeVariableInstruction)
    {
        LocalVariableDefinition variable = storeVariableInstruction.getVariable();
        printLine("store %s(#%d)", variable.getName(), variable.getSlot());
        return null;
    }

    @Override
    public Void visitIncrementVariable(ByteCodeNode parent, IncrementVariableInstruction incrementVariableInstruction)
    {
        LocalVariableDefinition variable = incrementVariableInstruction.getVariable();
        byte increment = incrementVariableInstruction.getIncrement();
        printLine("increment %s(#%d) %s", variable.getName(), variable.getSlot(), increment);
        return null;
    }

    //
    // Invoke
    //

    @Override
    public Void visitInvoke(ByteCodeNode parent, InvokeInstruction invokeInstruction)
    {
        printLine("invoke %s.%s%s",
                invokeInstruction.getTarget().getJavaClassName(),
                invokeInstruction.getName(),
                invokeInstruction.getMethodDescription());
        return null;
    }

    @Override
    public Void visitInvokeDynamic(ByteCodeNode parent, InvokeDynamicInstruction invokeDynamicInstruction)
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
    public Void visitIf(ByteCodeNode parent, IfStatement ifStatement)
    {
        if (ifStatement.getComment() != null) {
            printLine();
            printLine("// %s", ifStatement.getComment());
        }
        printLine("if {");
        indentLevel++;
        ifStatement.getCondition().accept(ifStatement, this);
        ifStatement.getIfTrue().accept(ifStatement, this);
        if (ifStatement.getIfFalse() != null) {
            ifStatement.getIfFalse().accept(ifStatement, this);
        }
        indentLevel--;
        printLine("}");
        return null;
    }

    @Override
    public Void visitFor(ByteCodeNode parent, ForLoop forLoop)
    {
        if (forLoop.getComment() != null) {
            printLine();
            printLine("// %s", forLoop.getComment());
        }
        printLine("for {");
        indentLevel++;
        forLoop.getInitialize().accept(forLoop, this);
        forLoop.getCondition().accept(forLoop, this);
        forLoop.getUpdate().accept(forLoop, this);
        forLoop.getBody().accept(forLoop, this);
        indentLevel--;
        printLine("}");
        return null;
    }

    @Override
    public Void visitWhile(ByteCodeNode parent, WhileLoop whileLoop)
    {
        if (whileLoop.getComment() != null) {
            printLine();
            printLine("// %s", whileLoop.getComment());
        }
        printLine("while {");
        indentLevel++;
        whileLoop.getCondition().accept(whileLoop, this);
        whileLoop.getBody().accept(whileLoop, this);
        indentLevel--;
        printLine("}");
        return null;
    }

    @Override
    public Void visitDoWhile(ByteCodeNode parent, DoWhileLoop doWhileLoop)
    {
        if (doWhileLoop.getComment() != null) {
            printLine();
            printLine("// %s", doWhileLoop.getComment());
        }
        return super.visitDoWhile(parent, doWhileLoop);
    }

    @Override
    public Void visitLookupSwitch(ByteCodeNode parent, LookupSwitch lookupSwitch)
    {
        if (lookupSwitch.getComment() != null) {
            printLine();
            printLine("// %s", lookupSwitch.getComment());
        }
        printLine("switch {");
        indentLevel++;
        for (CaseStatement caseStatement : lookupSwitch.getCases()) {
            printLine("case %s: goto %s", caseStatement.getKey(), caseStatement.getLabel().getName());
        }
        printLine("default: goto %s", lookupSwitch.getDefaultCase().getName());
        indentLevel--;
        printLine("}");
        return null;
    }

    //
    // Instructions
    //

    @Override
    public Void visitInstruction(ByteCodeNode parent, InstructionNode node)
    {
        return super.visitInstruction(parent, node);
    }

    //
    // Constants
    //

    @Override
    public Void visitBoxedBooleanConstant(ByteCodeNode parent, BoxedBooleanConstant boxedBooleanConstant)
    {
        printLine("load constant %s", boxedBooleanConstant.getValue());
        return null;
    }

    @Override
    public Void visitIntConstant(ByteCodeNode parent, IntConstant intConstant)
    {
        printLine("load constant %s", intConstant.getValue());
        return null;
    }

    @Override
    public Void visitBoxedIntegerConstant(ByteCodeNode parent, BoxedIntegerConstant boxedIntegerConstant)
    {
        printLine("load constant new Integer(%s)", boxedIntegerConstant.getValue());
        return null;
    }

    @Override
    public Void visitFloatConstant(ByteCodeNode parent, FloatConstant floatConstant)
    {
        printLine("load constant %sf", floatConstant.getValue());
        return null;
    }

    @Override
    public Void visitBoxedFloatConstant(ByteCodeNode parent, BoxedFloatConstant boxedFloatConstant)
    {
        printLine("load constant new Float(%sf)", boxedFloatConstant.getValue());
        return null;
    }

    @Override
    public Void visitLongConstant(ByteCodeNode parent, LongConstant longConstant)
    {
        printLine("load constant %sL", longConstant.getValue());
        return null;
    }

    @Override
    public Void visitBoxedLongConstant(ByteCodeNode parent, BoxedLongConstant boxedLongConstant)
    {
        printLine("load constant new Long(%sL)", boxedLongConstant.getValue());
        return null;
    }

    @Override
    public Void visitDoubleConstant(ByteCodeNode parent, DoubleConstant doubleConstant)
    {
        printLine("load constant %s", doubleConstant.getValue());
        return null;
    }

    @Override
    public Void visitBoxedDoubleConstant(ByteCodeNode parent, BoxedDoubleConstant boxedDoubleConstant)
    {
        printLine("load constant new Double(%s)", boxedDoubleConstant.getValue());
        return null;
    }

    @Override
    public Void visitStringConstant(ByteCodeNode parent, StringConstant stringConstant)
    {
        printLine("load constant \"%s\"", stringConstant.getValue());
        return null;
    }

    @Override
    public Void visitClassConstant(ByteCodeNode parent, ClassConstant classConstant)
    {
        printLine("load constant %s.class", classConstant.getValue().getJavaClassName());
        return null;
    }

    //
    // Line Number
    //

    @Override
    public Void visitLineNumber(ByteCodeNode parent, LineNumberNode lineNumberNode)
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
        out.println(String.format("%s%s", indent(indentLevel), line));
    }

    public void printLine(String format, Object... args)
    {
        String line = String.format(format, args);
        out.println(String.format("%s%s", indent(indentLevel), line));
    }

    public void printWords(String... words)
    {
        String line = Joiner.on(" ").join(words);
        out.println(String.format("%s%s", indent(indentLevel), line));
    }

    private String indent(int level)
    {
        StringBuilder builder = new StringBuilder();

//        if (lineNumber >= 0) {
//            builder.append(String.format("%4s", lineNumber + ": "));
//        } else {
//            builder.append("    ");
//        }

        for (int i = 0; i < level; i++) {
            builder.append("    ");
        }
        return builder.toString();
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
