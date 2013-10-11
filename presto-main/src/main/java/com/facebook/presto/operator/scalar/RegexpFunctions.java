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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.instruction.Constant;
import com.facebook.presto.operator.Description;
import com.facebook.presto.sql.gen.DefaultFunctionBinder;
import com.facebook.presto.sql.gen.FunctionBinder;
import com.facebook.presto.sql.gen.FunctionBinding;
import com.facebook.presto.sql.gen.TypedByteCodeNode;
import com.facebook.presto.util.ThreadLocalCache;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class RegexpFunctions
{
    private static final PatternCache CACHE = new PatternCache(100);

    private RegexpFunctions()
    {
    }

    @Description("returns substrings matching a regular expression")
    @ScalarFunction(functionBinder = RegexFunctionBinder.class)
    public static boolean regexpLike(Slice source, Slice pattern)
    {
        return regexpLike(CACHE, source, pattern);
    }

    public static boolean regexpLike(PatternCache patternCache, Slice source, Slice pattern)
    {
        return regexpLike(source, patternCache.get(pattern));
    }

    public static boolean regexpLike(Slice source, Pattern pattern)
    {
        return pattern.matcher(source.toString(UTF_8)).find();
    }

    @Description("removes substrings matching a regular expression")
    @ScalarFunction(functionBinder = RegexFunctionBinder.class)
    public static Slice regexpReplace(Slice source, Slice pattern)
    {
        return regexpReplace(source, pattern, Slices.EMPTY_SLICE);
    }

    @Description("replaces substrings matching a regular expression by given string")
    @ScalarFunction(functionBinder = RegexFunctionBinder.class)
    public static Slice regexpReplace(Slice source, Slice pattern, Slice replacement)
    {
        return regexpReplace(CACHE, source, pattern, replacement);
    }

    public static Slice regexpReplace(PatternCache patternCache, Slice source, Slice pattern, Slice replacement)
    {
        return regexpReplace(source, patternCache.get(pattern), replacement);
    }

    public static Slice regexpReplace(Slice source, Pattern pattern, Slice replacement)
    {
        Matcher matcher = pattern.matcher(source.toString(UTF_8));
        String replaced = matcher.replaceAll(replacement.toString(UTF_8));
        return Slices.copiedBuffer(replaced, UTF_8);
    }

    @Nullable
    @Description("string extracted using the given pattern")
    @ScalarFunction(functionBinder = RegexFunctionBinder.class)
    public static Slice regexpExtract(Slice source, Slice pattern)
    {
        return regexpExtract(source, pattern, 0);
    }

    @Nullable
    @Description("returns regex group of extracted string with a pattern")
    @ScalarFunction(functionBinder = RegexFunctionBinder.class)
    public static Slice regexpExtract(Slice source, Slice pattern, long group)
    {
        return regexpExtract(CACHE, source, pattern, group);
    }

    @Nullable
    public static Slice regexpExtract(PatternCache patternCache, Slice source, Slice pattern, long group)
    {
        return regexpExtract(source, patternCache.get(pattern), group);
    }

    @Nullable
    public static Slice regexpExtract(Slice source, Pattern pattern, long group)
    {
        Matcher matcher = pattern.matcher(source.toString(UTF_8));
        if ((group < 0) || (group > matcher.groupCount())) {
            throw new IllegalArgumentException("invalid group count");
        }
        if (!matcher.find()) {
            return null;
        }
        String extracted = matcher.group(Ints.checkedCast(group));
        return Slices.copiedBuffer(extracted, UTF_8);
    }

    public static class RegexFunctionBinder
            implements FunctionBinder
    {
        private static final MethodHandle constantRegexpLike;
        private static final MethodHandle dynamicRegexpLike;
        private static final MethodHandle constantRegexpReplace;
        private static final MethodHandle dynamicRegexpReplace;
        private static final MethodHandle constantRegexpExtract;
        private static final MethodHandle dynamicRegexpExtract;

        static {
            try {
                constantRegexpLike = lookup().findStatic(RegexpFunctions.class, "regexpLike", methodType(boolean.class, Slice.class, Pattern.class));
                dynamicRegexpLike = lookup().findStatic(RegexpFunctions.class, "regexpLike", methodType(boolean.class, PatternCache.class, Slice.class, Slice.class));
                constantRegexpReplace = lookup().findStatic(RegexpFunctions.class, "regexpReplace", methodType(Slice.class, Slice.class, Pattern.class, Slice.class));
                dynamicRegexpReplace = lookup().findStatic(RegexpFunctions.class, "regexpReplace", methodType(Slice.class, PatternCache.class, Slice.class, Slice.class, Slice.class));
                constantRegexpExtract = lookup().findStatic(RegexpFunctions.class, "regexpExtract", methodType(Slice.class, Slice.class, Pattern.class, long.class));
                dynamicRegexpExtract = lookup().findStatic(RegexpFunctions.class, "regexpExtract", methodType(Slice.class, PatternCache.class, Slice.class, Slice.class, long.class));
            }
            catch (ReflectiveOperationException e) {
                throw Throwables.propagate(e);
            }
        }

        public FunctionBinding bindFunction(long bindingId, String name, ByteCodeNode getSessionByteCode, List<TypedByteCodeNode> arguments)
        {
            MethodHandle methodHandle;
            boolean nullable = false;
            TypedByteCodeNode patternNode = arguments.get(1);
            if (patternNode.getNode() instanceof Constant) {
                switch (name) {
                    case "regexp_like":
                        methodHandle = constantRegexpLike;
                        break;
                    case "regexp_replace":
                        methodHandle = constantRegexpReplace;
                        if (arguments.size() == 2) {
                            methodHandle = MethodHandles.insertArguments(methodHandle, 2, Slices.EMPTY_SLICE);
                        }
                        break;
                    case "regexp_extract":
                        methodHandle = constantRegexpExtract;
                        nullable = true;
                        if (arguments.size() == 2) {
                            methodHandle = MethodHandles.insertArguments(methodHandle, 2, 0L);
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported method " + name);
                }

                Slice patternSlice = (Slice) ((Constant) patternNode.getNode()).getValue();

                Pattern pattern = Pattern.compile(patternSlice.toString(UTF_8));

                methodHandle = MethodHandles.insertArguments(methodHandle, 1, pattern);

                // remove the pattern argument
                arguments = new ArrayList<>(arguments);
                arguments.remove(1);
                arguments = ImmutableList.copyOf(arguments);
            }
            else {
                switch (name) {
                    case "regexp_like":
                        methodHandle = dynamicRegexpLike;
                        break;
                    case "regexp_replace":
                        methodHandle = dynamicRegexpReplace;
                        if (arguments.size() == 2) {
                            methodHandle = MethodHandles.insertArguments(methodHandle, 3, Slices.EMPTY_SLICE);
                        }
                        break;
                    case "regexp_extract":
                        methodHandle = dynamicRegexpExtract;
                        nullable = true;
                        if (arguments.size() == 2) {
                            methodHandle = MethodHandles.insertArguments(methodHandle, 3, 0L);
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported method " + name);
                }

                methodHandle = methodHandle.bindTo(new PatternCache(100));
            }

            return DefaultFunctionBinder.bindConstantArguments(bindingId, name, getSessionByteCode, arguments, methodHandle, nullable);
        }
    }

    public static class PatternCache
            extends ThreadLocalCache<Slice, Pattern>
    {
        public PatternCache(int maxSizePerThread)
        {
            super(maxSizePerThread);
        }

        @Nonnull
        @Override
        protected Pattern load(Slice patternSlice)
        {
            return Pattern.compile(patternSlice.toString(UTF_8));
        }
    }
}
