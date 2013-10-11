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
package com.facebook.presto.sql.gen;

import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.instruction.Constant;
import com.facebook.presto.sql.planner.LikeUtils;
import com.facebook.presto.sql.planner.LikeUtils.LikePatternCache;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.joni.Regex;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.List;

import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;

public class LikeFunctionBinder
        implements FunctionBinder
{
    private static final MethodHandle constantPattern;
    private static final MethodHandle dynamicPattern;

    static {
        try {
            constantPattern = lookup().findStatic(LikeUtils.class, "regexMatches", methodType(boolean.class, Regex.class, Slice.class));
            dynamicPattern = lookup().findStatic(LikeUtils.class, "dynamicLike", methodType(boolean.class, LikePatternCache.class, Slice.class, Slice.class, Slice.class));
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    public FunctionBinding bindFunction(long bindingId, String name, ByteCodeNode getSessionByteCode, List<TypedByteCodeNode> arguments)
    {
        TypedByteCodeNode valueNode = arguments.get(0);
        TypedByteCodeNode patternNode = arguments.get(1);
        TypedByteCodeNode escapeNode = null;
        if (arguments.size() == 3) {
            escapeNode = arguments.get(2);
        }

        MethodHandle methodHandle;
        if (patternNode.getNode() instanceof Constant && (escapeNode == null || escapeNode.getNode() instanceof Constant)) {
            Slice pattern = (Slice) ((Constant) patternNode.getNode()).getValue();
            Slice escapeSlice = null;
            if (escapeNode != null) {
                escapeSlice = (Slice) ((Constant) escapeNode.getNode()).getValue();
            }

            Regex regex = LikeUtils.likeToPattern(pattern, escapeSlice);

            methodHandle = constantPattern.bindTo(regex);
            arguments = ImmutableList.of(valueNode);
        }
        else {
            methodHandle = dynamicPattern.bindTo(new LikePatternCache(100));
            if (escapeNode == null) {
                methodHandle = MethodHandles.insertArguments(methodHandle, 2, (Object) null);
            }
        }

        CallSite callSite = new ConstantCallSite(methodHandle);
        return new FunctionBinding(bindingId, name, callSite, arguments, false);
    }
}
