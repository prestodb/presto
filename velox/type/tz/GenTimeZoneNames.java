/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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
package velox.type.tz;

import java.text.DateFormatSymbols;
import java.util.Locale;

public class GenTimeZoneNames {
  public static void main(String[] args) {
    System.out.println("/*");
    System.out.println(" * Copyright (c) Facebook, Inc. and its affiliates.");
    System.out.println(" *");
    System.out.println(" * Licensed under the Apache License, Version 2.0 (the \"License\");");
    System.out.println(" * you may not use this file except in compliance with the License.");
    System.out.println(" * You may obtain a copy of the License at");
    System.out.println(" *");
    System.out.println(" *     http://www.apache.org/licenses/LICENSE-2.0");
    System.out.println(" *");
    System.out.println(" * Unless required by applicable law or agreed to in writing, software");
    System.out.println(" * distributed under the License is distributed on an \"AS IS\" BASIS,");
    System.out.println(" * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.");
    System.out.println(" * See the License for the specific language governing permissions and");
    System.out.println(" * limitations under the License.");
    System.out.println(" */");
    System.out.println("");
    System.out.println("// This file is generated. Do not modify it manually. To re-generate it, run:");
    System.out.println("//");
    System.out.println("//  javac velox/type/tz/GenTimeZoneNames.java");
    System.out.println("//  java velox/type/tz/GenTimeZoneNames > velox/type/tz/TimeZoneNames.cpp");
    System.out.println("//");
    System.out.println("// Use the version of Java with which you're running Presto to generate");
    System.out.println("// compatible names. This version was generated using openjdk version ");
    System.out.println("// \"17.0.2\"");
    System.out.println("// @generated");
    System.out.println("");
    System.out.println("#include \"velox/type/tz/TimeZoneNames.h\"");
    System.out.println("");
    System.out.println("#include <folly/Portability.h>");
    System.out.println("");
    System.out.println("// This file just contains a single function that returns an, admittedly");
    System.out.println("// large, hard coded map.");
    System.out.println("FOLLY_CLANG_DISABLE_WARNING(\"-Wignored-optimization-argument\");");
    System.out.println("");
    System.out.println("namespace facebook::velox::tz {");
    System.out.println("");
    System.out.println("const std::unordered_map<std::string, TimeZoneNames>& getTimeZoneNames() {");
    System.out.println("  static auto* timeZoneNames = new std::unordered_map<");
    System.out.println("      std::string,");
    System.out.println("      TimeZoneNames>([] {");
    System.out.println("    // Work around clang compiler bug causing multi-hour compilation");
    System.out.println("    // with -fsanitize=fuzzer");
    System.out.println("    // https://github.com/llvm/llvm-project/issues/75666");
    System.out.println("    return std::unordered_map<std::string, TimeZoneNames>{");

    String[][] zoneStringsEn = DateFormatSymbols.getInstance(Locale.ENGLISH).getZoneStrings();
    for (String[] zoneStrings : zoneStringsEn) {
      System.out.println(String.format("        {\"%s\", {\"%s\", \"%s\", \"%s\", \"%s\"}},", zoneStrings[0], zoneStrings[1], zoneStrings[2], zoneStrings[3], zoneStrings[4]));
    }

    System.out.println("    };");
    System.out.println("  }());");
    System.out.println("  return *timeZoneNames;");
    System.out.println("}");
    System.out.println("");
    System.out.println("} // namespace facebook::velox::tz");
  }
}
