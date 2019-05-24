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

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;

import static org.objectweb.asm.Opcodes.ASM5;

class AddFakeLineNumberClassVisitor
        extends ClassVisitor
{
    int methodCount;

    public AddFakeLineNumberClassVisitor(ClassVisitor cv)
    {
        super(ASM5, cv);
        super.visitSource("FakeSource.java", null);
    }

    @Override
    public void visitSource(String source, String debug)
    {
        super.visitSource(source, debug);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions)
    {
        MethodVisitor methodVisitor = cv.visitMethod(access, name, desc, signature, exceptions);
        methodCount++;
        return new AddFakeLineNumberMethodVisitor(methodVisitor, 1000 * methodCount);
    }

    private static class AddFakeLineNumberMethodVisitor
            extends MethodVisitor
    {
        private int count;

        public AddFakeLineNumberMethodVisitor(MethodVisitor mv, int startLineNumber)
        {
            super(ASM5, mv);
            this.count = startLineNumber;
        }

        private void addFakeLineNumber()
        {
            Label label = new Label();
            mv.visitLabel(label);
            mv.visitLineNumber(++count, label);
        }

        @Override
        public void visitInsn(int opcode)
        {
            addFakeLineNumber();
            super.visitInsn(opcode);
        }

        @Override
        public void visitIntInsn(int opcode, int operand)
        {
            addFakeLineNumber();
            super.visitIntInsn(opcode, operand);
        }

        @Override
        public void visitVarInsn(int opcode, int var)
        {
            addFakeLineNumber();
            super.visitVarInsn(opcode, var);
        }

        @Override
        public void visitTypeInsn(int opcode, String type)
        {
            addFakeLineNumber();
            super.visitTypeInsn(opcode, type);
        }

        @Override
        public void visitFieldInsn(int opcode, String owner, String name, String desc)
        {
            addFakeLineNumber();
            super.visitFieldInsn(opcode, owner, name, desc);
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf)
        {
            addFakeLineNumber();
            super.visitMethodInsn(opcode, owner, name, desc, itf);
        }

        @Override
        public void visitInvokeDynamicInsn(String name, String desc, Handle bsm, Object... bsmArgs)
        {
            addFakeLineNumber();
            super.visitInvokeDynamicInsn(name, desc, bsm, bsmArgs);
        }

        @Override
        public void visitJumpInsn(int opcode, Label label)
        {
            addFakeLineNumber();
            super.visitJumpInsn(opcode, label);
        }

        @Override
        public void visitLdcInsn(Object cst)
        {
            addFakeLineNumber();
            super.visitLdcInsn(cst);
        }

        @Override
        public void visitIincInsn(int var, int increment)
        {
            addFakeLineNumber();
            super.visitIincInsn(var, increment);
        }

        @Override
        public void visitTableSwitchInsn(int min, int max, Label dflt, Label... labels)
        {
            addFakeLineNumber();
            super.visitTableSwitchInsn(min, max, dflt, labels);
        }

        @Override
        public void visitLookupSwitchInsn(Label dflt, int[] keys, Label[] labels)
        {
            addFakeLineNumber();
            super.visitLookupSwitchInsn(dflt, keys, labels);
        }

        @Override
        public void visitMultiANewArrayInsn(String desc, int dims)
        {
            addFakeLineNumber();
            super.visitMultiANewArrayInsn(desc, dims);
        }
    }
}
