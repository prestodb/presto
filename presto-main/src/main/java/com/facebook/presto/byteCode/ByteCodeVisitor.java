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

import com.facebook.presto.byteCode.control.DoWhileLoop;
import com.facebook.presto.byteCode.control.FlowControl;
import com.facebook.presto.byteCode.control.ForLoop;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.control.LookupSwitch;
import com.facebook.presto.byteCode.control.WhileLoop;
import com.facebook.presto.byteCode.debug.DebugNode;
import com.facebook.presto.byteCode.debug.LineNumberNode;
import com.facebook.presto.byteCode.debug.LocalVariableNode;
import com.facebook.presto.byteCode.instruction.Constant;
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
import com.facebook.presto.byteCode.instruction.FieldInstruction;
import com.facebook.presto.byteCode.instruction.FieldInstruction.GetFieldInstruction;
import com.facebook.presto.byteCode.instruction.FieldInstruction.PutFieldInstruction;
import com.facebook.presto.byteCode.instruction.InstructionNode;
import com.facebook.presto.byteCode.instruction.InvokeInstruction;
import com.facebook.presto.byteCode.instruction.InvokeInstruction.InvokeDynamicInstruction;
import com.facebook.presto.byteCode.instruction.JumpInstruction;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.byteCode.instruction.VariableInstruction;
import com.facebook.presto.byteCode.instruction.VariableInstruction.IncrementVariableInstruction;
import com.facebook.presto.byteCode.instruction.VariableInstruction.LoadVariableInstruction;
import com.facebook.presto.byteCode.instruction.VariableInstruction.StoreVariableInstruction;

public class ByteCodeVisitor<T>
{
    public T visitClass(ClassDefinition classDefinition)
    {
        for (AnnotationDefinition annotationDefinition : classDefinition.getAnnotations()) {
            visitAnnotation(classDefinition, annotationDefinition);
        }
        for (FieldDefinition fieldDefinition : classDefinition.getFields()) {
            visitField(classDefinition, fieldDefinition);
        }
        for (MethodDefinition methodDefinition : classDefinition.getMethods()) {
            visitMethod(classDefinition, methodDefinition);
        }
        return null;
    }

    public T visitAnnotation(Object parent, AnnotationDefinition annotationDefinition)
    {
        return null;
    }

    public T visitField(ClassDefinition classDefinition, FieldDefinition fieldDefinition)
    {
        for (AnnotationDefinition annotationDefinition : fieldDefinition.getAnnotations()) {
            visitAnnotation(fieldDefinition, annotationDefinition);
        }
        return null;
    }

    public T visitMethod(ClassDefinition classDefinition, MethodDefinition methodDefinition)
    {
        for (AnnotationDefinition annotationDefinition : methodDefinition.getAnnotations()) {
            visitAnnotation(methodDefinition, annotationDefinition);
        }
        methodDefinition.getBody().accept(null, this);
        return null;
    }

    public T visitNode(ByteCodeNode parent, ByteCodeNode node)
    {
        for (ByteCodeNode byteCodeNode : node.getChildNodes()) {
            byteCodeNode.accept(node, this);
        }
        return null;
    }

    //
    // Comment
    //

    public T visitComment(ByteCodeNode parent, Comment node)
    {
        return visitNode(parent, node);
    }

    //
    // Block
    //

    public T visitBlock(ByteCodeNode parent, Block block)
    {
        return visitNode(parent, block);
    }

    //
    // Flow Control
    //

    public T visitFlowControl(ByteCodeNode parent, FlowControl flowControl)
    {
        return visitNode(parent, flowControl);
    }

    public T visitIf(ByteCodeNode parent, IfStatement ifStatement)
    {
        return visitFlowControl(parent, ifStatement);
    }

    public T visitFor(ByteCodeNode parent, ForLoop forLoop)
    {
        return visitFlowControl(parent, forLoop);
    }

    public T visitWhile(ByteCodeNode parent, WhileLoop whileLoop)
    {
        return visitFlowControl(parent, whileLoop);
    }

    public T visitDoWhile(ByteCodeNode parent, DoWhileLoop doWhileLoop)
    {
        return visitFlowControl(parent, doWhileLoop);
    }

    public T visitLookupSwitch(ByteCodeNode parent, LookupSwitch lookupSwitch)
    {
        return visitFlowControl(parent, lookupSwitch);
    }

    //
    // Instructions
    //

    public T visitInstruction(ByteCodeNode parent, InstructionNode node)
    {
        return visitNode(parent, node);
    }

    public T visitLabel(ByteCodeNode parent, LabelNode labelNode)
    {
        return visitInstruction(parent, labelNode);
    }

    public T visitJumpInstruction(ByteCodeNode parent, JumpInstruction jumpInstruction)
    {
        return visitInstruction(parent, jumpInstruction);
    }

    //
    // Constants
    //

    public T visitConstant(ByteCodeNode parent, Constant constant)
    {
        return visitInstruction(parent, constant);
    }

    public T visitBoxedBooleanConstant(ByteCodeNode parent, BoxedBooleanConstant boxedBooleanConstant)
    {
        return visitConstant(parent, boxedBooleanConstant);
    }

    public T visitIntConstant(ByteCodeNode parent, IntConstant intConstant)
    {
        return visitConstant(parent, intConstant);
    }

    public T visitBoxedIntegerConstant(ByteCodeNode parent, BoxedIntegerConstant boxedIntegerConstant)
    {
        return visitConstant(parent, boxedIntegerConstant);
    }

    public T visitFloatConstant(ByteCodeNode parent, FloatConstant floatConstant)
    {
        return visitConstant(parent, floatConstant);
    }

    public T visitBoxedFloatConstant(ByteCodeNode parent, BoxedFloatConstant boxedFloatConstant)
    {
        return visitConstant(parent, boxedFloatConstant);
    }

    public T visitLongConstant(ByteCodeNode parent, LongConstant longConstant)
    {
        return visitConstant(parent, longConstant);
    }

    public T visitBoxedLongConstant(ByteCodeNode parent, BoxedLongConstant boxedLongConstant)
    {
        return visitConstant(parent, boxedLongConstant);
    }

    public T visitDoubleConstant(ByteCodeNode parent, DoubleConstant doubleConstant)
    {
        return visitConstant(parent, doubleConstant);
    }

    public T visitBoxedDoubleConstant(ByteCodeNode parent, BoxedDoubleConstant boxedDoubleConstant)
    {
        return visitConstant(parent, boxedDoubleConstant);
    }

    public T visitStringConstant(ByteCodeNode parent, StringConstant stringConstant)
    {
        return visitConstant(parent, stringConstant);
    }

    public T visitClassConstant(ByteCodeNode parent, ClassConstant classConstant)
    {
        return visitConstant(parent, classConstant);
    }

    //
    // Local Variable Instructions
    //

    public T visitVariableInstruction(ByteCodeNode parent, VariableInstruction variableInstruction)
    {
        return visitInstruction(parent, variableInstruction);
    }

    public T visitLoadVariable(ByteCodeNode parent, LoadVariableInstruction loadVariableInstruction)
    {
        return visitVariableInstruction(parent, loadVariableInstruction);
    }

    public T visitStoreVariable(ByteCodeNode parent, StoreVariableInstruction storeVariableInstruction)
    {
        return visitVariableInstruction(parent, storeVariableInstruction);
    }

    public T visitIncrementVariable(ByteCodeNode parent, IncrementVariableInstruction incrementVariableInstruction)
    {
        return visitVariableInstruction(parent, incrementVariableInstruction);
    }

    //
    // Field Instructions
    //

    public T visitFieldInstruction(ByteCodeNode parent, FieldInstruction fieldInstruction)
    {
        return visitInstruction(parent, fieldInstruction);
    }

    public T visitGetField(ByteCodeNode parent, GetFieldInstruction getFieldInstruction)
    {
        return visitFieldInstruction(parent, getFieldInstruction);
    }

    public T visitPutField(ByteCodeNode parent, PutFieldInstruction putFieldInstruction)
    {
        return visitFieldInstruction(parent, putFieldInstruction);
    }

    //
    // Invoke
    //

    public T visitInvoke(ByteCodeNode parent, InvokeInstruction invokeInstruction)
    {
        return visitInstruction(parent, invokeInstruction);
    }

    public T visitInvokeDynamic(ByteCodeNode parent, InvokeDynamicInstruction invokeDynamicInstruction)
    {
        return visitInvoke(parent, invokeDynamicInstruction);
    }

    //
    // Debug
    //

    public T visitDebug(ByteCodeNode parent, DebugNode debugNode)
    {
        return visitNode(parent, debugNode);
    }

    public T visitLineNumber(ByteCodeNode parent, LineNumberNode lineNumberNode)
    {
        return visitDebug(parent, lineNumberNode);
    }

    public T visitLocalVariable(ByteCodeNode parent, LocalVariableNode localVariableNode)
    {
        return visitDebug(parent, localVariableNode);
    }
}
