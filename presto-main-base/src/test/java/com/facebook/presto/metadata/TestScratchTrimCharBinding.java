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
package com.facebook.presto.metadata;

import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;

public class TestScratchTrimCharBinding
{
    private final FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();

    @Test
    public void testCandidateOrderAndResolution()
    {
        for (String name : ImmutableList.of("trim", "ltrim", "rtrim")) {
            System.out.println("=== registration order: " + name + " ===");
            List<SqlFunction> candidates = candidates(name);
            for (SqlFunction candidate : candidates) {
                System.out.println("  " + candidate.getSignature());
            }

            // end-to-end: what the analyzer does for name(CAST(x AS varchar))
            try {
                FunctionHandle handle = functionAndTypeManager.lookupFunction(name, fromTypes(VARCHAR));
                functionAndTypeManager.getFunctionMetadata(handle);
                System.out.println("  -> analysis OK for " + name + "(varchar)");
            }
            catch (RuntimeException e) {
                System.out.println("  -> ANALYSIS FAILED for " + name + "(varchar): " + e);
            }

            // same lookup, but with the char overloads examined first, which simulates a different
            // registration order of the very same candidate set
            List<SqlFunction> charFirst = new ArrayList<>();
            for (SqlFunction candidate : candidates) {
                if (candidate.getSignature().getArgumentTypes().get(0).getBase().equals("char")) {
                    charFirst.add(candidate);
                }
            }
            charFirst.addAll(candidates);
            Signature resolved = new Signature(
                    candidates.get(0).getSignature().getName(),
                    candidates.get(0).getSignature().getKind(),
                    VARCHAR.getTypeSignature(),
                    VARCHAR.getTypeSignature());
            try {
                functionAndTypeManager.getSpecializedFunctionKey(resolved, charFirst);
                System.out.println("  -> char-candidate-first lookup OK");
            }
            catch (RuntimeException e) {
                System.out.println("  -> char-candidate-first lookup FAILED: " + e);
            }
        }
    }

    @Test
    public void testWhichImplementationServesCharCallsites()
    {
        // With the char overloads returning varchar(x), a char(x) callsite and a varchar(x) callsite
        // have the same return type, so check that a char argument still selects the char
        // implementation rather than the varchar one.
        for (String name : ImmutableList.of("trim", "ltrim", "rtrim")) {
            for (List<com.facebook.presto.common.type.Type> arguments : ImmutableList.of(
                    ImmutableList.<com.facebook.presto.common.type.Type>of(createCharType(10)),
                    ImmutableList.<com.facebook.presto.common.type.Type>of(createCharType(10), VARCHAR))) {
                FunctionHandle handle = functionAndTypeManager.lookupFunction(name, fromTypes(arguments));
                Signature resolved = ((BuiltInFunctionHandle) handle).getSignature();
                SqlFunction implementation = functionAndTypeManager
                        .getSpecializedFunctionKey(resolved, candidates(name))
                        .getFunction();
                System.out.println(name + arguments + " resolved to " + resolved
                        + "  implemented by " + implementation.getSignature());
            }
        }
    }

    @Test
    public void testBindCharCandidateAgainstUnboundedVarchar()
    {
        Signature afterPr = trimSignature("varchar(x)");
        System.out.println("binding " + afterPr + " against args=(varchar), return=varchar");
        try {
            System.out.println("  result: " + new SignatureBinder(functionAndTypeManager, afterPr, false)
                    .bindVariables(fromTypes(VARCHAR), VARCHAR));
        }
        catch (RuntimeException e) {
            System.out.println("  THREW: " + e);
        }

        Signature beforePr = trimSignature("char(x)");
        System.out.println("binding " + beforePr + " against args=(varchar), return=varchar");
        try {
            System.out.println("  result: " + new SignatureBinder(functionAndTypeManager, beforePr, false)
                    .bindVariables(fromTypes(VARCHAR), VARCHAR));
        }
        catch (RuntimeException e) {
            System.out.println("  THREW: " + e);
        }
    }

    private List<SqlFunction> candidates(String name)
    {
        Collection<SqlFunction> all = functionAndTypeManager.listBuiltInFunctions();
        List<SqlFunction> candidates = new ArrayList<>();
        for (SqlFunction candidate : all) {
            if (candidate.getSignature().getName().getObjectName().equals(name)) {
                candidates.add(candidate);
            }
        }
        return candidates;
    }

    private static Signature trimSignature(String returnType)
    {
        return new Signature(
                com.facebook.presto.common.QualifiedObjectName.valueOf("presto.default.trim"),
                com.facebook.presto.spi.function.FunctionKind.SCALAR,
                TypeSignature.parseTypeSignature(returnType, ImmutableSet.of("x")),
                ImmutableList.of(TypeSignature.parseTypeSignature("char(x)", ImmutableSet.of("x"))));
    }
}
