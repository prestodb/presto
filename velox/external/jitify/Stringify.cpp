/*
 * Copyright (c) 2017-2020, NVIDIA CORPORATION. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * * Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 *   notice, this list of conditions and the following disclaimer in the
 *   documentation and/or other materials provided with the distribution.
 * * Neither the name of NVIDIA CORPORATION nor the names of its
 *   contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
 * OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
  Stringify is a simple utility to convert text files to C string literals.
 */

/* Modified for use in Velox Wave */

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

// Replaces non-alphanumeric characters with '_' and
//   prepends '_' if the string begins with a digit.
std::string sanitize_varname(std::string const& s) {
  std::string r = s;
  if (std::isdigit(r[0])) {
    r = '_' + r;
  }
  for (std::string::iterator it = r.begin(); it != r.end(); ++it) {
    if (!std::isalnum(*it)) {
      *it = '_';
    }
  }
  return r;
}
// Replaces " with \"
std::string sanitize_string_literal(std::string const& s) {
  std::stringstream ss;
  for (std::string::const_iterator it = s.begin(); it != s.end(); ++it) {
    if (*it == '"' || *it == '\\') {
      ss << '\\';
    }
    ss << *it;
  }
  return ss.str();
}

int main(int argc, char* argv[]) {
  if (argc <= 1 || argv[1][0] == '-') {
    std::cout << "Stringify - Converts text files to C string literals"
              << std::endl;
    std::cout << "Usage: " << argv[0] << " infile [varname] > outfile"
              << std::endl;
    return -1;
  }
  char* filename = argv[1];
  std::string varname = (argc > 2) ? argv[2] : sanitize_varname(filename);
  std::ifstream istream(filename);
  std::ostream& ostream = std::cout;
  std::string line;
  // Note: This puts "filename\n" at the beginning of the string, which is
  //         what jitify expects.
  ostream << "const char* " << varname << " = " << std::endl
          << "\"" << filename << "\\n\"" << std::endl;
  while (std::getline(istream, line)) {
    ostream << "\"" << sanitize_string_literal(line) << "\\n\"" << std::endl;
  }
  ostream << ";" << std::endl
          << "bool " << varname << "_reg = registerHeader(" << varname << ");"
          << std::endl;
  return 0;
}
