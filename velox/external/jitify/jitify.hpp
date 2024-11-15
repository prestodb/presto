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
  -----------
  Jitify 0.9
  -----------
  A C++ library for easy integration of CUDA runtime compilation into
  existing codes.

  --------------

  Modified for use in Velox Wave by exposing the complete list of sources from a Program.

  --------------
  How to compile
  --------------
  Compiler dependencies: <jitify.hpp>, -std=c++11
  Linker dependencies:   dl cuda nvrtc

  --------------------------------------
  Embedding source files into executable
  --------------------------------------
  g++  ... -ldl -rdynamic -DJITIFY_ENABLE_EMBEDDED_FILES=1
  -Wl,-b,binary,my_kernel.cu,include/my_header.cuh,-b,default nvcc ... -ldl
  -Xcompiler "-rdynamic
  -Wl\,-b\,binary\,my_kernel.cu\,include/my_header.cuh\,-b\,default"
  JITIFY_INCLUDE_EMBEDDED_FILE(my_kernel_cu);
  JITIFY_INCLUDE_EMBEDDED_FILE(include_my_header_cuh);

  ----
  TODO
  ----
  Extract valid compile options and pass the rest to cuModuleLoadDataEx
  See if can have stringified headers automatically looked-up
    by having stringify add them to a (static) global map.
    The global map can be updated by creating a static class instance
      whose constructor performs the registration.
    Can then remove all headers from JitCache constructor in example code
  See other TODOs in code
*/

/*! \file jitify.hpp
 *  \brief The Jitify library header
 */

/*! \mainpage Jitify - A C++ library that simplifies the use of NVRTC
 *  \p Use class jitify::JitCache to manage and launch JIT-compiled CUDA
 *    kernels.
 *
 *  \p Use namespace jitify::reflection to reflect types and values into
 *    code-strings.
 *
 *  \p Use JITIFY_INCLUDE_EMBEDDED_FILE() to declare files that have been
 *  embedded into the executable using the GCC linker.
 *
 *  \p Use jitify::parallel_for and JITIFY_LAMBDA() to generate and launch
 *  simple kernels.
 */

#pragma once

#ifndef JITIFY_THREAD_SAFE
#define JITIFY_THREAD_SAFE 1
#endif

#if JITIFY_ENABLE_EMBEDDED_FILES
#include <dlfcn.h>
#endif
#include <stdint.h>
#include <algorithm>
#include <cctype>
#include <cstring>  // For strtok_r etc.
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#if JITIFY_THREAD_SAFE
#include <mutex>
#endif

#include <cuda.h>
#include <cuda_runtime_api.h>  // For dim3, cudaStream_t
#if CUDA_VERSION >= 8000
#define NVRTC_GET_TYPE_NAME 1
#endif
#include <nvrtc.h>

// For use by get_current_executable_path().
#ifdef __linux__
#include <linux/limits.h>  // For PATH_MAX

#include <cstdlib>  // For realpath
#define JITIFY_PATH_MAX PATH_MAX
#elif defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#define JITIFY_PATH_MAX MAX_PATH
#else
#error "Unsupported platform"
#endif

#ifdef _MSC_VER       // MSVC compiler
#include <dbghelp.h>  // For UnDecorateSymbolName
#else
#include <cxxabi.h>  // For abi::__cxa_demangle
#endif

#if defined(_WIN32) || defined(_WIN64)
// WAR for strtok_r being called strtok_s on Windows
#pragma push_macro("strtok_r")
#undef strtok_r
#define strtok_r strtok_s
// WAR for min and max possibly being macros defined by windows.h
#pragma push_macro("min")
#pragma push_macro("max")
#undef min
#undef max
#endif

#ifndef JITIFY_PRINT_LOG
#define JITIFY_PRINT_LOG 1
#endif

#if JITIFY_PRINT_ALL
#define JITIFY_PRINT_INSTANTIATION 1
#define JITIFY_PRINT_SOURCE 1
#define JITIFY_PRINT_LOG 1
#define JITIFY_PRINT_PTX 1
#define JITIFY_PRINT_LINKER_LOG 1
#define JITIFY_PRINT_LAUNCH 1
#define JITIFY_PRINT_HEADER_PATHS 1
#endif

#if JITIFY_ENABLE_EMBEDDED_FILES
#define JITIFY_FORCE_UNDEFINED_SYMBOL(x) void* x##_forced = (void*)&x
/*! Include a source file that has been embedded into the executable using the
 *    GCC linker.
 * \param name The name of the source file (<b>not</b> as a string), which must
 * be sanitized by replacing non-alpha-numeric characters with underscores.
 * E.g., \code{.cpp}JITIFY_INCLUDE_EMBEDDED_FILE(my_header_h)\endcode will
 * include the embedded file "my_header.h".
 * \note Files declared with this macro can be referenced using
 * their original (unsanitized) filenames when creating a \p
 * jitify::Program instance.
 */
#define JITIFY_INCLUDE_EMBEDDED_FILE(name)                                \
  extern "C" uint8_t _jitify_binary_##name##_start[] asm("_binary_" #name \
                                                         "_start");       \
  extern "C" uint8_t _jitify_binary_##name##_end[] asm("_binary_" #name   \
                                                       "_end");           \
  JITIFY_FORCE_UNDEFINED_SYMBOL(_jitify_binary_##name##_start);           \
  JITIFY_FORCE_UNDEFINED_SYMBOL(_jitify_binary_##name##_end)
#endif  // JITIFY_ENABLE_EMBEDDED_FILES

/*! Jitify library namespace
 */
namespace jitify {

/*! Source-file load callback.
 *
 *  \param filename The name of the requested source file.
 *  \param tmp_stream A temporary stream that can be used to hold source code.
 *  \return A pointer to an input stream containing the source code, or NULL
 *  to defer loading of the file to Jitify's file-loading mechanisms.
 */
typedef std::istream* (*file_callback_type)(std::string filename,
                                            std::iostream& tmp_stream);

// Exclude from Doxygen
//! \cond

class JitCache;

// Simple cache using LRU discard policy
template <typename KeyType, typename ValueType>
class ObjectCache {
 public:
  typedef KeyType key_type;
  typedef ValueType value_type;

 private:
  typedef std::map<key_type, value_type> object_map;
  typedef std::deque<key_type> key_rank;
  typedef typename key_rank::iterator rank_iterator;
  object_map _objects;
  key_rank _ranked_keys;
  size_t _capacity;

  inline void discard_old(size_t n = 0) {
    if (n > _capacity) {
      throw std::runtime_error("Insufficient capacity in cache");
    }
    while (_objects.size() > _capacity - n) {
      key_type discard_key = _ranked_keys.back();
      _ranked_keys.pop_back();
      _objects.erase(discard_key);
    }
  }

 public:
  inline ObjectCache(size_t capacity = 8) : _capacity(capacity) {}
  inline void resize(size_t capacity) {
    _capacity = capacity;
    this->discard_old();
  }
  inline bool contains(const key_type& k) const {
    return (bool)_objects.count(k);
  }
  inline void touch(const key_type& k) {
    if (!this->contains(k)) {
      throw std::runtime_error("Key not found in cache");
    }
    rank_iterator rank = std::find(_ranked_keys.begin(), _ranked_keys.end(), k);
    if (rank != _ranked_keys.begin()) {
      // Move key to front of ranks
      _ranked_keys.erase(rank);
      _ranked_keys.push_front(k);
    }
  }
  inline value_type& get(const key_type& k) {
    if (!this->contains(k)) {
      throw std::runtime_error("Key not found in cache");
    }
    this->touch(k);
    return _objects[k];
  }
  inline value_type& insert(const key_type& k,
                            const value_type& v = value_type()) {
    this->discard_old(1);
    _ranked_keys.push_front(k);
    return _objects.insert(std::make_pair(k, v)).first->second;
  }
  template <typename... Args>
  inline value_type& emplace(const key_type& k, Args&&... args) {
    this->discard_old(1);
    // Note: Use of piecewise_construct allows non-movable non-copyable types
    auto iter = _objects
                    .emplace(std::piecewise_construct, std::forward_as_tuple(k),
                             std::forward_as_tuple(args...))
                    .first;
    _ranked_keys.push_front(iter->first);
    return iter->second;
  }
};

namespace detail {

// Convenience wrapper for std::vector that provides handy constructors
template <typename T>
class vector : public std::vector<T> {
  typedef std::vector<T> super_type;

 public:
  vector() : super_type() {}
  vector(size_t n) : super_type(n) {}  // Note: Not explicit, allows =0
  vector(std::vector<T> const& vals) : super_type(vals) {}
  template <int N>
  vector(T const (&vals)[N]) : super_type(vals, vals + N) {}
  vector(std::vector<T>&& vals) : super_type(vals) {}
  vector(std::initializer_list<T> vals) : super_type(vals) {}
};

// Helper functions for parsing/manipulating source code

inline std::string replace_characters(std::string str,
                                      std::string const& oldchars,
                                      char newchar) {
  size_t i = str.find_first_of(oldchars);
  while (i != std::string::npos) {
    str[i] = newchar;
    i = str.find_first_of(oldchars, i + 1);
  }
  return str;
}
inline std::string sanitize_filename(std::string name) {
  return replace_characters(name, "/\\.-: ?%*|\"<>", '_');
}

#if JITIFY_ENABLE_EMBEDDED_FILES
class EmbeddedData {
  void* _app;
  EmbeddedData(EmbeddedData const&);
  EmbeddedData& operator=(EmbeddedData const&);

 public:
  EmbeddedData() {
    _app = dlopen(NULL, RTLD_LAZY);
    if (!_app) {
      throw std::runtime_error(std::string("dlopen failed: ") + dlerror());
    }
    dlerror();  // Clear any existing error
  }
  ~EmbeddedData() {
    if (_app) {
      dlclose(_app);
    }
  }
  const uint8_t* operator[](std::string key) const {
    key = sanitize_filename(key);
    key = "_binary_" + key;
    uint8_t const* data = (uint8_t const*)dlsym(_app, key.c_str());
    if (!data) {
      throw std::runtime_error(std::string("dlsym failed: ") + dlerror());
    }
    return data;
  }
  const uint8_t* begin(std::string key) const {
    return (*this)[key + "_start"];
  }
  const uint8_t* end(std::string key) const { return (*this)[key + "_end"]; }
};
#endif  // JITIFY_ENABLE_EMBEDDED_FILES

inline bool is_tokenchar(char c) {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
         (c >= '0' && c <= '9') || c == '_';
}
inline std::string replace_token(std::string src, std::string token,
                                 std::string replacement) {
  size_t i = src.find(token);
  while (i != std::string::npos) {
    if (i == 0 || i == src.size() - token.size() ||
        (!is_tokenchar(src[i - 1]) && !is_tokenchar(src[i + token.size()]))) {
      src.replace(i, token.size(), replacement);
      i += replacement.size();
    } else {
      i += token.size();
    }
    i = src.find(token, i);
  }
  return src;
}
inline std::string path_base(std::string p) {
  // "/usr/local/myfile.dat" -> "/usr/local"
  // "foo/bar"  -> "foo"
  // "foo/bar/" -> "foo/bar"
#if defined _WIN32 || defined _WIN64
  const char* sep = "\\/";
#else
  char sep = '/';
#endif
  size_t i = p.find_last_of(sep);
  if (i != std::string::npos) {
    return p.substr(0, i);
  } else {
    return "";
  }
}
inline std::string path_join(std::string p1, std::string p2) {
#ifdef _WIN32
  char sep = '\\';
#else
  char sep = '/';
#endif
  if (p1.size() && p2.size() && p2[0] == sep) {
    throw std::invalid_argument("Cannot join to absolute path");
  }
  if (p1.size() && p1[p1.size() - 1] != sep) {
    p1 += sep;
  }
  return p1 + p2;
}
// Elides "/." and "/.." tokens from path.
inline std::string path_simplify(const std::string& path) {
  std::vector<std::string> dirs;
  std::string cur_dir;
  bool after_slash = false;
  for (int i = 0; i < (int)path.size(); ++i) {
    if (path[i] == '/') {
      if (after_slash) continue;  // Ignore repeat slashes
      after_slash = true;
      if (cur_dir == ".." && !dirs.empty() && dirs.back() != "..") {
        if (dirs.size() == 1 && dirs.front().empty()) {
          throw std::runtime_error(
              "Invalid path: back-traversals exceed depth of absolute path");
        }
        dirs.pop_back();
      } else if (cur_dir != ".") {  // Ignore /./
        dirs.push_back(cur_dir);
      }
      cur_dir.clear();
    } else {
      after_slash = false;
      cur_dir.push_back(path[i]);
    }
  }
  if (!after_slash) {
    dirs.push_back(cur_dir);
  }
  std::stringstream ss;
  for (int i = 0; i < (int)dirs.size() - 1; ++i) {
    ss << dirs[i] << "/";
  }
  if (!dirs.empty()) ss << dirs.back();
  if (after_slash) ss << "/";
  return ss.str();
}
inline unsigned long long hash_larson64(const char* s,
                                        unsigned long long seed = 0) {
  unsigned long long hash = seed;
  while (*s) {
    hash = hash * 101 + *s++;
  }
  return hash;
}

inline uint64_t hash_combine(uint64_t a, uint64_t b) {
  // Note: The magic number comes from the golden ratio
  return a ^ (0x9E3779B97F4A7C17ull + b + (b >> 2) + (a << 6));
}

inline bool extract_include_info_from_compile_error(std::string log,
                                                    std::string& name,
                                                    std::string& parent,
                                                    int& line_num) {
  static const std::vector<std::string> pattern = {
      "could not open source file \"", "cannot open source file \""};

  for (auto& p : pattern) {
    size_t beg = log.find(p);
    if (beg != std::string::npos) {
      beg += p.size();
      size_t end = log.find("\"", beg);
      name = log.substr(beg, end - beg);

      size_t line_beg = log.rfind("\n", beg);
      if (line_beg == std::string::npos) {
        line_beg = 0;
      } else {
        line_beg += 1;
      }

      size_t split = log.find("(", line_beg);
      parent = log.substr(line_beg, split - line_beg);
      line_num =
          atoi(log.substr(split + 1, log.find(")", split + 1) - (split + 1))
                   .c_str());

      return true;
    }
  }

  return false;
}

inline bool is_include_directive_with_quotes(const std::string& source,
                                             int line_num) {
  // TODO: Check each find() for failure.
  size_t beg = 0;
  for (int i = 1; i < line_num; ++i) {
    beg = source.find("\n", beg) + 1;
  }
  beg = source.find("include", beg) + 7;
  beg = source.find_first_of("\"<", beg);
  return source[beg] == '"';
}

inline std::string comment_out_code_line(int line_num, std::string source) {
  size_t beg = 0;
  for (int i = 1; i < line_num; ++i) {
    beg = source.find("\n", beg) + 1;
  }
  return (source.substr(0, beg) + "//" + source.substr(beg));
}

inline void print_with_line_numbers(std::string const& source) {
  int linenum = 1;
  std::stringstream source_ss(source);
  std::stringstream output_ss;
  output_ss.imbue(std::locale::classic());
  for (std::string line; std::getline(source_ss, line); ++linenum) {
    output_ss << std::setfill(' ') << std::setw(3) << linenum << " " << line
              << std::endl;
  }
  std::cout << output_ss.str();
}

inline void print_compile_log(std::string program_name,
                              std::string const& log) {
  std::cout << "---------------------------------------------------"
            << std::endl;
  std::cout << "--- JIT compile log for " << program_name << " ---"
            << std::endl;
  std::cout << "---------------------------------------------------"
            << std::endl;
  std::cout << log << std::endl;
  std::cout << "---------------------------------------------------"
            << std::endl;
}

inline std::vector<std::string> split_string(std::string str,
                                             long maxsplit = -1,
                                             std::string delims = " \t") {
  std::vector<std::string> results;
  if (maxsplit == 0) {
    results.push_back(str);
    return results;
  }
  // Note: +1 to include NULL-terminator
  std::vector<char> v_str(str.c_str(), str.c_str() + (str.size() + 1));
  char* c_str = v_str.data();
  char* saveptr = c_str;
  char* token = nullptr;
  for (long i = 0; i != maxsplit; ++i) {
    token = ::strtok_r(c_str, delims.c_str(), &saveptr);
    c_str = 0;
    if (!token) {
      return results;
    }
    results.push_back(token);
  }
  // Check if there's a final piece
  token += ::strlen(token) + 1;
  if (token - v_str.data() < (ptrdiff_t)str.size()) {
    // Find the start of the final piece
    token += ::strspn(token, delims.c_str());
    if (*token) {
      results.push_back(token);
    }
  }
  return results;
}

static const std::map<std::string, std::string>& get_jitsafe_headers_map();

inline bool load_source(
    std::string filename, std::map<std::string, std::string>& sources,
    std::string current_dir = "",
    std::vector<std::string> include_paths = std::vector<std::string>(),
    file_callback_type file_callback = 0, std::string* program_name = nullptr,
    std::map<std::string, std::string>* fullpaths = nullptr,
    bool search_current_dir = true) {
  std::istream* source_stream = 0;
  std::stringstream string_stream;
  std::ifstream file_stream;
  // First detect direct source-code string ("my_program\nprogram_code...")
  size_t newline_pos = filename.find("\n");
  if (newline_pos != std::string::npos) {
    std::string source = filename.substr(newline_pos + 1);
    filename = filename.substr(0, newline_pos);
    string_stream << source;
    source_stream = &string_stream;
  }
  if (program_name) {
    *program_name = filename;
  }
  if (sources.count(filename)) {
    // Already got this one
    return true;
  }
  if (!source_stream) {
    std::string fullpath = path_join(current_dir, filename);
    // Try loading from callback
    if (!file_callback ||
        !((source_stream = file_callback(fullpath, string_stream)) != 0)) {
#if JITIFY_ENABLE_EMBEDDED_FILES
      // Try loading as embedded file
      EmbeddedData embedded;
      std::string source;
      try {
        source.assign(embedded.begin(fullpath), embedded.end(fullpath));
        string_stream << source;
        source_stream = &string_stream;
      } catch (std::runtime_error const&)
#endif  // JITIFY_ENABLE_EMBEDDED_FILES
      {
        // Try loading from filesystem
        bool found_file = false;
        if (search_current_dir) {
          file_stream.open(fullpath.c_str());
          if (file_stream) {
            source_stream = &file_stream;
            found_file = true;
          }
        }
        // Search include directories
        if (!found_file) {
          for (int i = 0; i < (int)include_paths.size(); ++i) {
            fullpath = path_join(include_paths[i], filename);
            file_stream.open(fullpath.c_str());
            if (file_stream) {
              source_stream = &file_stream;
              found_file = true;
              break;
            }
          }
          if (!found_file) {
            // Try loading from builtin headers
            fullpath = path_join("__jitify_builtin", filename);
            auto it = get_jitsafe_headers_map().find(filename);
            if (it != get_jitsafe_headers_map().end()) {
              string_stream << it->second;
              source_stream = &string_stream;
            } else {
              return false;
            }
          }
        }
      }
    }
    if (fullpaths) {
      // Record the full file path corresponding to this include name.
      (*fullpaths)[filename] = path_simplify(fullpath);
    }
  }
  sources[filename] = std::string();
  std::string& source = sources[filename];
  std::string line;
  size_t linenum = 0;
  unsigned long long hash = 0;
  bool pragma_once = false;
  bool remove_next_blank_line = false;
  while (std::getline(*source_stream, line)) {
    ++linenum;

    // HACK WAR for static variables not allowed on the device (unless
    // __shared__)
    // TODO: This breaks static member variables
    // line = replace_token(line, "static const", "/*static*/ const");

    // TODO: Need to watch out for /* */ comments too
    std::string cleanline =
        line.substr(0, line.find("//"));  // Strip line comments
    // if( cleanline.back() == "\r" ) { // Remove Windows line ending
    //	cleanline = cleanline.substr(0, cleanline.size()-1);
    //}
    // TODO: Should trim whitespace before checking .empty()
    if (cleanline.empty() && remove_next_blank_line) {
      remove_next_blank_line = false;
      continue;
    }
    // Maintain a file hash for use in #pragma once WAR
    hash = hash_larson64(line.c_str(), hash);
    if (cleanline.find("#pragma once") != std::string::npos) {
      pragma_once = true;
      // Note: This is an attempt to recover the original line numbering,
      //         which otherwise gets off-by-one due to the include guard.
      remove_next_blank_line = true;
      // line = "//" + line; // Comment out the #pragma once line
      continue;
    }

    // HACK WAR for Thrust using "#define FOO #pragma bar"
    // TODO: This is not robust to block comments, line continuations, or tabs.
    size_t pragma_beg = cleanline.find("#pragma ");
    if (pragma_beg != std::string::npos) {
      std::string line_after_pragma = line.substr(pragma_beg + 8);
      // TODO: Handle block comments (currently they cause a compilation error).
      size_t comment_start = line_after_pragma.find("//");
      std::string pragma_args = line_after_pragma.substr(0, comment_start);
      // handle quote character used in #pragma expression
      pragma_args = replace_token(pragma_args, "\"", "\\\"");
      std::string comment = comment_start != std::string::npos
                                ? line_after_pragma.substr(comment_start)
                                : "";
      line = line.substr(0, pragma_beg) + "_Pragma(\"" + pragma_args + "\")" +
             comment;
    }

    source += line + "\n";
  }
  // HACK TESTING (WAR for cub)
  source = "#define cudaDeviceSynchronize() cudaSuccess\n" + source;
  ////source = "cudaError_t cudaDeviceSynchronize() { return cudaSuccess; }\n" +
  /// source;

  // WAR for #pragma once causing problems when there are multiple inclusions
  //   of the same header from different paths.
  if (pragma_once) {
    std::stringstream ss;
    ss.imbue(std::locale::classic());
    ss << std::uppercase << std::hex << std::setw(8) << std::setfill('0')
       << hash;
    std::string include_guard_name = "_JITIFY_INCLUDE_GUARD_" + ss.str() + "\n";
    std::string include_guard_header;
    include_guard_header += "#ifndef " + include_guard_name;
    include_guard_header += "#define " + include_guard_name;
    std::string include_guard_footer;
    include_guard_footer += "#endif // " + include_guard_name;
    source = include_guard_header + source + "\n" + include_guard_footer;
  }
  // return filename;
  return true;
}

}  // namespace detail

//! \endcond

/*! Jitify reflection utilities namespace
 */
namespace reflection {

//  Provides type and value reflection via a function 'reflect':
//    reflect<Type>()   -> "Type"
//    reflect(value)    -> "(T)value"
//    reflect<VAL>()    -> "VAL"
//    reflect<Type,VAL> -> "VAL"
//    reflect_template<float,NonType<int,7>,char>() -> "<float,7,char>"
//    reflect_template({"float", "7", "char"}) -> "<float,7,char>"

/*! A wrapper class for non-type template parameters.
 */
template <typename T, T VALUE_>
struct NonType {
  constexpr static T VALUE = VALUE_;
};

// Forward declaration
template <typename T>
inline std::string reflect(T const& value);

//! \cond

namespace detail {

template <typename T>
inline std::string value_string(const T& x) {
  std::stringstream ss;
  ss << x;
  return ss.str();
}
// WAR for non-printable characters
template <>
inline std::string value_string<char>(const char& x) {
  std::stringstream ss;
  ss << (int)x;
  return ss.str();
}
template <>
inline std::string value_string<signed char>(const signed char& x) {
  std::stringstream ss;
  ss << (int)x;
  return ss.str();
}
template <>
inline std::string value_string<unsigned char>(const unsigned char& x) {
  std::stringstream ss;
  ss << (int)x;
  return ss.str();
}
template <>
inline std::string value_string<wchar_t>(const wchar_t& x) {
  std::stringstream ss;
  ss << (long)x;
  return ss.str();
}
// Specialisation for bool true/false literals
template <>
inline std::string value_string<bool>(const bool& x) {
  return x ? "true" : "false";
}

// Removes all tokens that start with double underscores.
inline void strip_double_underscore_tokens(char* s) {
  using jitify::detail::is_tokenchar;
  char* w = s;
  do {
    if (*s == '_' && *(s + 1) == '_') {
      while (is_tokenchar(*++s))
        ;
    }
  } while ((*w++ = *s++));
}

//#if CUDA_VERSION < 8000
#ifdef _MSC_VER  // MSVC compiler
inline std::string demangle_cuda_symbol(const char* mangled_name) {
  // We don't have a way to demangle CUDA symbol names under MSVC.
  return mangled_name;
}
inline std::string demangle_native_type(const std::type_info& typeinfo) {
  // Get the decorated name and skip over the leading '.'.
  const char* decorated_name = typeinfo.raw_name() + 1;
  char undecorated_name[4096];
  if (UnDecorateSymbolName(
          decorated_name, undecorated_name,
          sizeof(undecorated_name) / sizeof(*undecorated_name),
          UNDNAME_NO_ARGUMENTS |          // Treat input as a type name
              UNDNAME_NAME_ONLY           // No "class" and "struct" prefixes
          /*UNDNAME_NO_MS_KEYWORDS*/)) {  // No "__cdecl", "__ptr64" etc.
    // WAR for UNDNAME_NO_MS_KEYWORDS messing up function types.
    strip_double_underscore_tokens(undecorated_name);
    return undecorated_name;
  }
  throw std::runtime_error("UnDecorateSymbolName failed");
}
#else  // not MSVC
inline std::string demangle_cuda_symbol(const char* mangled_name) {
  size_t bufsize = 0;
  char* buf = nullptr;
  std::string demangled_name;
  int status;
  auto demangled_ptr = std::unique_ptr<char, decltype(free)*>(
      abi::__cxa_demangle(mangled_name, buf, &bufsize, &status), free);
  if (status == 0) {
    demangled_name = demangled_ptr.get();  // all worked as expected
  } else if (status == -2) {
    demangled_name = mangled_name;  // we interpret this as plain C name
  } else if (status == -1) {
    throw std::runtime_error(
        std::string("memory allocation failure in __cxa_demangle"));
  } else if (status == -3) {
    throw std::runtime_error(std::string("invalid argument to __cxa_demangle"));
  }
  return demangled_name;
}
inline std::string demangle_native_type(const std::type_info& typeinfo) {
  return demangle_cuda_symbol(typeinfo.name());
}
#endif  // not MSVC
//#endif // CUDA_VERSION < 8000

template <typename>
class JitifyTypeNameWrapper_ {};

template <typename T>
struct type_reflection {
  inline static std::string name() {
    //#if CUDA_VERSION < 8000
    // TODO: Use nvrtcGetTypeName once it has the same behavior as this.
    // WAR for typeid discarding cv qualifiers on value-types
    // Wrap type in dummy template class to preserve cv-qualifiers, then strip
    // off the wrapper from the resulting string.
    std::string wrapped_name =
        demangle_native_type(typeid(JitifyTypeNameWrapper_<T>));
    // Note: The reflected name of this class also has namespace prefixes.
    const std::string wrapper_class_name = "JitifyTypeNameWrapper_<";
    size_t start = wrapped_name.find(wrapper_class_name);
    if (start == std::string::npos) {
      throw std::runtime_error("Type reflection failed: " + wrapped_name);
    }
    start += wrapper_class_name.size();
    std::string name =
        wrapped_name.substr(start, wrapped_name.size() - (start + 1));
    return name;
    //#else
    //         std::string ret;
    //         nvrtcResult status = nvrtcGetTypeName<T>(&ret);
    //         if( status != NVRTC_SUCCESS ) {
    //                 throw std::runtime_error(std::string("nvrtcGetTypeName
    // failed:
    //")+ nvrtcGetErrorString(status));
    //         }
    //         return ret;
    //#endif
  }
};  // namespace detail
template <typename T, T VALUE>
struct type_reflection<NonType<T, VALUE> > {
  inline static std::string name() {
    return jitify::reflection::reflect(VALUE);
  }
};

}  // namespace detail

//! \endcond

/*! Create an Instance object that contains a const reference to the
 *  value.  We use this to wrap abstract objects from which we want to extract
 *  their type at runtime (e.g., derived type).  This is used to facilitate
 *  templating on derived type when all we know at compile time is abstract
 * type.
 */
template <typename T>
struct Instance {
  const T& value;
  Instance(const T& value_arg) : value(value_arg) {}
};

/*! Create an Instance object from which we can extract the value's run-time
 * type.
 *  \param value The const value to be captured.
 */
template <typename T>
inline Instance<T const> instance_of(T const& value) {
  return Instance<T const>(value);
}

/*! A wrapper used for representing types as values.
 */
template <typename T>
struct Type {};

// Type reflection
// E.g., reflect<float>() -> "float"
// Note: This strips trailing const and volatile qualifiers
/*! Generate a code-string for a type.
 *  \code{.cpp}reflect<float>() --> "float"\endcode
 */
template <typename T>
inline std::string reflect() {
  return detail::type_reflection<T>::name();
}
// Value reflection
// E.g., reflect(3.14f) -> "(float)3.14"
/*! Generate a code-string for a value.
 *  \code{.cpp}reflect(3.14f) --> "(float)3.14"\endcode
 */
template <typename T>
inline std::string reflect(T const& value) {
  return "(" + reflect<T>() + ")" + detail::value_string(value);
}
// Non-type template arg reflection (implicit conversion to int64_t)
// E.g., reflect<7>() -> "(int64_t)7"
/*! Generate a code-string for an integer non-type template argument.
 *  \code{.cpp}reflect<7>() --> "(int64_t)7"\endcode
 */
template <int64_t N>
inline std::string reflect() {
  return reflect<NonType<int64_t, N> >();
}
// Non-type template arg reflection (explicit type)
// E.g., reflect<int,7>() -> "(int)7"
/*! Generate a code-string for a generic non-type template argument.
 *  \code{.cpp} reflect<int,7>() --> "(int)7" \endcode
 */
template <typename T, T N>
inline std::string reflect() {
  return reflect<NonType<T, N> >();
}
// Type reflection via value
// E.g., reflect(Type<float>()) -> "float"
/*! Generate a code-string for a type wrapped as a Type instance.
 *  \code{.cpp}reflect(Type<float>()) --> "float"\endcode
 */
template <typename T>
inline std::string reflect(jitify::reflection::Type<T>) {
  return reflect<T>();
}

/*! Generate a code-string for a type wrapped as an Instance instance.
 *  \code{.cpp}reflect(Instance<float>(3.1f)) --> "float"\endcode
 *  or more simply when passed to a instance_of helper
 *  \code{.cpp}reflect(instance_of(3.1f)) --> "float"\endcodei
 *  This is specifically for the case where we want to extract the run-time
 * type, e.g., derived type, of an object pointer.
 */
template <typename T>
inline std::string reflect(jitify::reflection::Instance<T>& value) {
  return detail::demangle_native_type(typeid(value.value));
}

// Type from value
// E.g., type_of(3.14f) -> Type<float>()
/*! Create a Type object representing a value's type.
 *  \param value The value whose type is to be captured.
 */
template <typename T>
inline Type<T> type_of(T&) {
  return Type<T>();
}
/*! Create a Type object representing a value's type.
 *  \param value The const value whose type is to be captured.
 */
template <typename T>
inline Type<T const> type_of(T const&) {
  return Type<T const>();
}

// Multiple value reflections one call, returning list of strings
template <typename... Args>
inline std::vector<std::string> reflect_all(Args... args) {
  return {reflect(args)...};
}

inline std::string reflect_list(jitify::detail::vector<std::string> const& args,
                                std::string opener = "",
                                std::string closer = "") {
  std::stringstream ss;
  ss << opener;
  for (int i = 0; i < (int)args.size(); ++i) {
    if (i > 0) ss << ",";
    ss << args[i];
  }
  ss << closer;
  return ss.str();
}

// Template instantiation reflection
// inline std::string reflect_template(std::vector<std::string> const& args) {
inline std::string reflect_template(
    jitify::detail::vector<std::string> const& args) {
  // Note: The space in " >" is a WAR to avoid '>>' appearing
  return reflect_list(args, "<", " >");
}
// TODO: See if can make this evaluate completely at compile-time
template <typename... Ts>
inline std::string reflect_template() {
  return reflect_template({reflect<Ts>()...});
  // return reflect_template<sizeof...(Ts)>({reflect<Ts>()...});
}

}  // namespace reflection

//! \cond

namespace detail {

// Demangles nested variable names using the PTX name mangling scheme
// (which follows the Itanium64 ABI). E.g., _ZN1a3Foo2bcE -> a::Foo::bc.
inline std::string demangle_ptx_variable_name(const char* name) {
  std::stringstream ss;
  const char* c = name;
  if (*c++ != '_' || *c++ != 'Z') return name;  // Non-mangled name
  if (*c++ != 'N') return "";  // Not a nested name, unsupported
  while (true) {
    // Parse identifier length.
    int n = 0;
    while (std::isdigit(*c)) {
      n = n * 10 + (*c - '0');
      c++;
    }
    if (!n) return "";  // Invalid or unsupported mangled name
    // Parse identifier.
    const char* c0 = c;
    while (n-- && *c) c++;
    if (!*c) return "";  // Mangled name is truncated
    std::string id(c0, c);
    // Identifiers starting with "_GLOBAL" are anonymous namespaces.
    ss << (id.substr(0, 7) == "_GLOBAL" ? "(anonymous namespace)" : id);
    // Nested name specifiers end with 'E'.
    if (*c == 'E') break;
    // There are more identifiers to come, add join token.
    ss << "::";
  }
  return ss.str();
}

static const char* get_current_executable_path() {
  static const char* path = []() -> const char* {
    static char buffer[JITIFY_PATH_MAX] = {};
#ifdef __linux__
    if (!::realpath("/proc/self/exe", buffer)) return nullptr;
#elif defined(_WIN32) || defined(_WIN64)
    if (!GetModuleFileNameA(nullptr, buffer, JITIFY_PATH_MAX)) return nullptr;
#endif
    return buffer;
  }();
  return path;
}

inline bool endswith(const std::string& str, const std::string& suffix) {
  return str.size() >= suffix.size() &&
         str.substr(str.size() - suffix.size()) == suffix;
}

// Infers the JIT input type from the filename suffix. If no known suffix is
// present, the filename is assumed to refer to a library, and the associated
// suffix (and possibly prefix) is automatically added to the filename.
inline CUjitInputType get_cuda_jit_input_type(std::string* filename) {
  if (endswith(*filename, ".ptx")) {
    return CU_JIT_INPUT_PTX;
  } else if (endswith(*filename, ".cubin")) {
    return CU_JIT_INPUT_CUBIN;
  } else if (endswith(*filename, ".fatbin")) {
    return CU_JIT_INPUT_FATBINARY;
  } else if (endswith(*filename,
#if defined _WIN32 || defined _WIN64
                      ".obj"
#else  // Linux
                      ".o"
#endif
                      )) {
    return CU_JIT_INPUT_OBJECT;
  } else {  // Assume library
#if defined _WIN32 || defined _WIN64
    if (!endswith(*filename, ".lib")) {
      *filename += ".lib";
    }
#else  // Linux
    if (!endswith(*filename, ".a")) {
      *filename = "lib" + *filename + ".a";
    }
#endif
    return CU_JIT_INPUT_LIBRARY;
  }
}

class CUDAKernel {
  std::vector<std::string> _link_files;
  std::vector<std::string> _link_paths;
  CUlinkState _link_state;
  CUmodule _module;
  CUfunction _kernel;
  std::string _func_name;
  std::string _ptx;
  std::map<std::string, std::string> _global_map;
  std::vector<CUjit_option> _opts;
  std::vector<void*> _optvals;
#ifdef JITIFY_PRINT_LINKER_LOG
  static const unsigned int _log_size = 8192;
  char _error_log[_log_size];
  char _info_log[_log_size];
#endif

  inline void cuda_safe_call(CUresult res) const {
    if (res != CUDA_SUCCESS) {
      const char* msg;
      cuGetErrorName(res, &msg);
      throw std::runtime_error(msg);
    }
  }
  inline void create_module(std::vector<std::string> link_files,
                            std::vector<std::string> link_paths) {
    CUresult result;
#ifndef JITIFY_PRINT_LINKER_LOG
    // WAR since linker log does not seem to be constructed using a single call
    // to cuModuleLoadDataEx.
    if (link_files.empty()) {
      result =
          cuModuleLoadDataEx(&_module, _ptx.c_str(), (unsigned)_opts.size(),
                             _opts.data(), _optvals.data());
    } else
#endif
    {
      cuda_safe_call(cuLinkCreate((unsigned)_opts.size(), _opts.data(),
                                  _optvals.data(), &_link_state));
      cuda_safe_call(cuLinkAddData(_link_state, CU_JIT_INPUT_PTX,
                                   (void*)_ptx.c_str(), _ptx.size(),
                                   "jitified_source.ptx", 0, 0, 0));
      for (int i = 0; i < (int)link_files.size(); ++i) {
        std::string link_file = link_files[i];
        CUjitInputType jit_input_type;
        if (link_file == ".") {
          // Special case for linking to current executable.
          link_file = get_current_executable_path();
          jit_input_type = CU_JIT_INPUT_OBJECT;
        } else {
          // Infer based on filename.
          jit_input_type = get_cuda_jit_input_type(&link_file);
        }
        result = cuLinkAddFile(_link_state, jit_input_type, link_file.c_str(),
                               0, 0, 0);
        int path_num = 0;
        while (result == CUDA_ERROR_FILE_NOT_FOUND &&
               path_num < (int)link_paths.size()) {
          std::string filename = path_join(link_paths[path_num++], link_file);
          result = cuLinkAddFile(_link_state, jit_input_type, filename.c_str(),
                                 0, 0, 0);
        }
#if JITIFY_PRINT_LINKER_LOG
        if (result == CUDA_ERROR_FILE_NOT_FOUND) {
          std::cerr << "Linker error: Device library not found: " << link_file
                    << std::endl;
        } else if (result != CUDA_SUCCESS) {
          std::cerr << "Linker error: Failed to add file: " << link_file
                    << std::endl;
          std::cerr << _error_log << std::endl;
        }
#endif
        cuda_safe_call(result);
      }
      size_t cubin_size;
      void* cubin;
      result = cuLinkComplete(_link_state, &cubin, &cubin_size);
      if (result == CUDA_SUCCESS) {
        result = cuModuleLoadData(&_module, cubin);
      }
    }
#ifdef JITIFY_PRINT_LINKER_LOG
    std::cout << "---------------------------------------" << std::endl;
    std::cout << "--- Linker for "
              << reflection::detail::demangle_cuda_symbol(_func_name.c_str())
              << " ---" << std::endl;
    std::cout << "---------------------------------------" << std::endl;
    std::cout << _info_log << std::endl;
    std::cout << std::endl;
    std::cout << _error_log << std::endl;
    std::cout << "---------------------------------------" << std::endl;
#endif
    cuda_safe_call(result);
    // Allow _func_name to be empty to support cases where we want to generate
    // PTX containing extern symbol definitions but no kernels.
    if (!_func_name.empty()) {
      cuda_safe_call(
          cuModuleGetFunction(&_kernel, _module, _func_name.c_str()));
    }
  }
  inline void destroy_module() {
    if (_link_state) {
      cuda_safe_call(cuLinkDestroy(_link_state));
    }
    _link_state = 0;
    if (_module) {
      cuModuleUnload(_module);
    }
    _module = 0;
  }

  // create a map of __constant__ and __device__ variables in the ptx file
  // mapping demangled to mangled name
  inline void create_global_variable_map() {
    size_t pos = 0;
    while (pos < _ptx.size()) {
      pos = std::min(_ptx.find(".const .align", pos),
                     _ptx.find(".global .align", pos));
      if (pos == std::string::npos) break;
      size_t end = _ptx.find_first_of(";=", pos);
      if (_ptx[end] == '=') --end;
      std::string line = _ptx.substr(pos, end - pos);
      pos = end;
      size_t symbol_start = line.find_last_of(" ") + 1;
      size_t symbol_end = line.find_last_of("[");
      std::string entry = line.substr(symbol_start, symbol_end - symbol_start);
      std::string key = detail::demangle_ptx_variable_name(entry.c_str());
      // Skip unsupported mangled names. E.g., a static variable defined inside
      // a function (such variables are not directly addressable from outside
      // the function, so skipping them is the correct behavior).
      if (key == "") continue;
      _global_map[key] = entry;
    }
  }

  inline void set_linker_log() {
#ifdef JITIFY_PRINT_LINKER_LOG
    _opts.push_back(CU_JIT_INFO_LOG_BUFFER);
    _optvals.push_back((void*)_info_log);
    _opts.push_back(CU_JIT_INFO_LOG_BUFFER_SIZE_BYTES);
    _optvals.push_back((void*)(long)_log_size);
    _opts.push_back(CU_JIT_ERROR_LOG_BUFFER);
    _optvals.push_back((void*)_error_log);
    _opts.push_back(CU_JIT_ERROR_LOG_BUFFER_SIZE_BYTES);
    _optvals.push_back((void*)(long)_log_size);
    _opts.push_back(CU_JIT_LOG_VERBOSE);
    _optvals.push_back((void*)1);
#endif
  }

 public:
  inline CUDAKernel() : _link_state(0), _module(0), _kernel(0) {}
  inline CUDAKernel(const CUDAKernel& other) = delete;
  inline CUDAKernel& operator=(const CUDAKernel& other) = delete;
  inline CUDAKernel(CUDAKernel&& other) = delete;
  inline CUDAKernel& operator=(CUDAKernel&& other) = delete;
  inline CUDAKernel(const char* func_name, const char* ptx,
                    std::vector<std::string> link_files,
                    std::vector<std::string> link_paths, unsigned int nopts = 0,
                    CUjit_option* opts = 0, void** optvals = 0)
      : _link_files(link_files),
        _link_paths(link_paths),
        _link_state(0),
        _module(0),
        _kernel(0),
        _func_name(func_name),
        _ptx(ptx),
        _opts(opts, opts + nopts),
        _optvals(optvals, optvals + nopts) {
    this->set_linker_log();
    this->create_module(link_files, link_paths);
    this->create_global_variable_map();
  }

  inline CUDAKernel& set(const char* func_name, const char* ptx,
                         std::vector<std::string> link_files,
                         std::vector<std::string> link_paths,
                         unsigned int nopts = 0, CUjit_option* opts = 0,
                         void** optvals = 0) {
    this->destroy_module();
    _func_name = func_name;
    _ptx = ptx;
    _link_files = link_files;
    _link_paths = link_paths;
    _opts.assign(opts, opts + nopts);
    _optvals.assign(optvals, optvals + nopts);
    this->set_linker_log();
    this->create_module(link_files, link_paths);
    this->create_global_variable_map();
    return *this;
  }
  inline ~CUDAKernel() { this->destroy_module(); }
  inline operator CUfunction() const { return _kernel; }

  inline CUresult launch(dim3 grid, dim3 block, unsigned int smem,
                         CUstream stream, std::vector<void*> arg_ptrs) const {
    return cuLaunchKernel(_kernel, grid.x, grid.y, grid.z, block.x, block.y,
                          block.z, smem, stream, arg_ptrs.data(), NULL);
  }

  inline void safe_launch(dim3 grid, dim3 block, unsigned int smem,
                          CUstream stream, std::vector<void*> arg_ptrs) const {
    return cuda_safe_call(cuLaunchKernel(_kernel, grid.x, grid.y, grid.z,
                                         block.x, block.y, block.z, smem,
                                         stream, arg_ptrs.data(), NULL));
  }

  inline int get_func_attribute(CUfunction_attribute attribute) const {
    int value;
    cuda_safe_call(cuFuncGetAttribute(&value, attribute, _kernel));
    return value;
  }

  inline void set_func_attribute(CUfunction_attribute attribute,
                                 int value) const {
    cuda_safe_call(cuFuncSetAttribute(_kernel, attribute, value));
  }

  inline CUdeviceptr get_global_ptr(const char* name,
                                    size_t* size = nullptr) const {
    CUdeviceptr global_ptr = 0;
    auto global = _global_map.find(name);
    if (global != _global_map.end()) {
      cuda_safe_call(cuModuleGetGlobal(&global_ptr, size, _module,
                                       global->second.c_str()));
    } else {
      throw std::runtime_error(std::string("failed to look up global ") + name);
    }
    return global_ptr;
  }

  template <typename T>
  inline CUresult get_global_data(const char* name, T* data, size_t count,
                                  CUstream stream = 0) const {
    size_t size_bytes;
    CUdeviceptr ptr = get_global_ptr(name, &size_bytes);
    size_t given_size_bytes = count * sizeof(T);
    if (given_size_bytes != size_bytes) {
      throw std::runtime_error(
          std::string("Value for global variable ") + name +
          " has wrong size: got " + std::to_string(given_size_bytes) +
          " bytes, expected " + std::to_string(size_bytes));
    }
    return cuMemcpyDtoHAsync(data, ptr, size_bytes, stream);
  }

  template <typename T>
  inline CUresult set_global_data(const char* name, const T* data, size_t count,
                                  CUstream stream = 0) const {
    size_t size_bytes;
    CUdeviceptr ptr = get_global_ptr(name, &size_bytes);
    size_t given_size_bytes = count * sizeof(T);
    if (given_size_bytes != size_bytes) {
      throw std::runtime_error(
          std::string("Value for global variable ") + name +
          " has wrong size: got " + std::to_string(given_size_bytes) +
          " bytes, expected " + std::to_string(size_bytes));
    }
    return cuMemcpyHtoDAsync(ptr, data, size_bytes, stream);
  }

  const std::string& function_name() const { return _func_name; }
  const std::string& ptx() const { return _ptx; }
  const std::vector<std::string>& link_files() const { return _link_files; }
  const std::vector<std::string>& link_paths() const { return _link_paths; }
};

static const char* jitsafe_header_preinclude_h = R"(
//// WAR for Thrust (which appears to have forgotten to include this in result_of_adaptable_function.h
//#include <type_traits>

//// WAR for Thrust (which appear to have forgotten to include this in error_code.h)
//#include <string>

// WAR for generics/shfl.h
#define THRUST_STATIC_ASSERT(x)

// WAR for CUB
#ifdef __host__
#undef __host__
#endif
#define __host__

// WAR to allow exceptions to be parsed
#define try
#define catch(...)
)"
#if defined(_WIN32) || defined(_WIN64)
// WAR for NVRTC <= 11.0 not defining _WIN64.
R"(
#ifndef _WIN64
#define _WIN64 1
#endif
)"
#endif
;

static const char* jitsafe_header_float_h = R"(
#pragma once

#define FLT_RADIX       2
#define FLT_MANT_DIG    24
#define DBL_MANT_DIG    53
#define FLT_DIG         6
#define DBL_DIG         15
#define FLT_MIN_EXP     -125
#define DBL_MIN_EXP     -1021
#define FLT_MIN_10_EXP  -37
#define DBL_MIN_10_EXP  -307
#define FLT_MAX_EXP     128
#define DBL_MAX_EXP     1024
#define FLT_MAX_10_EXP  38
#define DBL_MAX_10_EXP  308
#define FLT_MAX         3.4028234e38f
#define DBL_MAX         1.7976931348623157e308
#define FLT_EPSILON     1.19209289e-7f
#define DBL_EPSILON     2.220440492503130e-16
#define FLT_MIN         1.1754943e-38f
#define DBL_MIN         2.2250738585072013e-308
#define FLT_ROUNDS      1
#if defined __cplusplus && __cplusplus >= 201103L
#define FLT_EVAL_METHOD 0
#define DECIMAL_DIG     21
#endif
)";

static const char* jitsafe_header_limits_h = R"(
#pragma once
#if __has_include(<cuda/std/climits>)
 #include <cuda/std/climits>
 #include <cuda/std/limits>
 #include <cuda/std/cstdint>
#else
 #if defined _WIN32 || defined _WIN64
  #define __WORDSIZE 32
 #else
  #if defined(__LP64__) || (defined __x86_64__ && !defined __ILP32__)
   #define __WORDSIZE 64
  #else
   #define __WORDSIZE 32
  #endif
 #endif
 #define MB_LEN_MAX  16
 #define CHAR_BIT    8
 #define SCHAR_MIN   (-128)
 #define SCHAR_MAX   127
 #define UCHAR_MAX   255
 enum {
   _JITIFY_CHAR_IS_UNSIGNED = (char)-1 >= 0,
   CHAR_MIN = _JITIFY_CHAR_IS_UNSIGNED ? 0 : SCHAR_MIN,
   CHAR_MAX = _JITIFY_CHAR_IS_UNSIGNED ? UCHAR_MAX : SCHAR_MAX,
 };
 #define SHRT_MIN    (-SHRT_MAX - 1)
 #define SHRT_MAX    0x7fff
 #define USHRT_MAX   0xffff
 #define INT_MIN     (-INT_MAX - 1)
 #define INT_MAX     0x7fffffff
 #define UINT_MAX    0xffffffff
 #if __WORDSIZE == 64
  # define LONG_MAX  LLONG_MAX
 #else
  # define LONG_MAX  INT_MAX
 #endif
 #define LONG_MIN    (-LONG_MAX - 1)
 #if __WORDSIZE == 64
  #define ULONG_MAX  ULLONG_MAX
 #else
  #define ULONG_MAX  UINT_MAX
 #endif
 #define LLONG_MAX  0x7fffffffffffffff
 #define LLONG_MIN  (-LLONG_MAX - 1)
 #define ULLONG_MAX 0xffffffffffffffff
#endif
)";

static const char* jitsafe_header_iterator = R"(
#pragma once

namespace std {
struct output_iterator_tag {};
struct input_iterator_tag {};
struct forward_iterator_tag {};
struct bidirectional_iterator_tag {};
struct random_access_iterator_tag {};
template<class Iterator>
struct iterator_traits {
  typedef typename Iterator::iterator_category iterator_category;
  typedef typename Iterator::value_type        value_type;
  typedef typename Iterator::difference_type   difference_type;
  typedef typename Iterator::pointer           pointer;
  typedef typename Iterator::reference         reference;
};
template<class T>
struct iterator_traits<T*> {
  typedef random_access_iterator_tag iterator_category;
  typedef T                          value_type;
  typedef ptrdiff_t                  difference_type;
  typedef T*                         pointer;
  typedef T&                         reference;
};
template<class T>
struct iterator_traits<T const*> {
  typedef random_access_iterator_tag iterator_category;
  typedef T                          value_type;
  typedef ptrdiff_t                  difference_type;
  typedef T const*                   pointer;
  typedef T const&                   reference;
};
}  // namespace std
)";

// TODO: This is incomplete; need floating point limits
//   Joe Eaton: added IEEE float and double types, none of the smaller types
//              using type specific structs since we can't template on floats.
static const char* jitsafe_header_limits = R"(
#pragma once
#if __has_include(<cuda/std/limits>)
 #include <cuda/std/climits>
 #include <cuda/std/limits>
 #include <cuda/std/cstdint>
#endif
#include <cfloat>
#include <climits>
#include <cstdint>
// TODO: epsilon(), infinity(), etc
namespace std {
namespace __jitify_detail {
#if __cplusplus >= 201103L
#define JITIFY_CXX11_CONSTEXPR constexpr
#define JITIFY_CXX11_NOEXCEPT noexcept
#else
#define JITIFY_CXX11_CONSTEXPR
#define JITIFY_CXX11_NOEXCEPT
#endif

struct FloatLimits {
#if __cplusplus >= 201103L
   static JITIFY_CXX11_CONSTEXPR inline __host__ __device__ 
          float lowest() JITIFY_CXX11_NOEXCEPT {   return -FLT_MAX;}
   static JITIFY_CXX11_CONSTEXPR inline __host__ __device__ 
          float min() JITIFY_CXX11_NOEXCEPT {      return FLT_MIN; }
   static JITIFY_CXX11_CONSTEXPR inline __host__ __device__ 
          float max() JITIFY_CXX11_NOEXCEPT {      return FLT_MAX; }
#endif  // __cplusplus >= 201103L
   enum {
   is_specialized    = true,
   is_signed         = true,
   is_integer        = false,
   is_exact          = false,
   has_infinity      = true,
   has_quiet_NaN     = true,
   has_signaling_NaN = true,
   has_denorm        = 1,
   has_denorm_loss   = true,
   round_style       = 1,
   is_iec559         = true,
   is_bounded        = true,
   is_modulo         = false,
   digits            = 24,
   digits10          = 6,
   max_digits10      = 9,
   radix             = 2,
   min_exponent      = -125,
   min_exponent10    = -37,
   max_exponent      = 128,
   max_exponent10    = 38,
   tinyness_before   = false,
   traps             = false
   };
};
struct DoubleLimits {
#if __cplusplus >= 201103L
   static JITIFY_CXX11_CONSTEXPR inline __host__ __device__ 
          double lowest() noexcept { return -DBL_MAX; }
   static JITIFY_CXX11_CONSTEXPR inline __host__ __device__ 
          double min() noexcept { return DBL_MIN; }
   static JITIFY_CXX11_CONSTEXPR inline __host__ __device__ 
          double max() noexcept { return DBL_MAX; }
#endif  // __cplusplus >= 201103L
   enum {
   is_specialized    = true,
   is_signed         = true,
   is_integer        = false,
   is_exact          = false,
   has_infinity      = true,
   has_quiet_NaN     = true,
   has_signaling_NaN = true,
   has_denorm        = 1,
   has_denorm_loss   = true,
   round_style       = 1,
   is_iec559         = true,
   is_bounded        = true,
   is_modulo         = false,
   digits            = 53,
   digits10          = 15,
   max_digits10      = 17,
   radix             = 2,
   min_exponent      = -1021,
   min_exponent10    = -307,
   max_exponent      = 1024,
   max_exponent10    = 308,
   tinyness_before   = false,
   traps             = false
   };
};
template<class T, T Min, T Max, int Digits=-1>
struct IntegerLimits {
	static inline __host__ __device__ T min() { return Min; }
	static inline __host__ __device__ T max() { return Max; }
#if __cplusplus >= 201103L
	static constexpr inline __host__ __device__ T lowest() noexcept {
		return Min;
	}
#endif  // __cplusplus >= 201103L
	enum {
       is_specialized = true,
       digits            = (Digits == -1) ? (int)(sizeof(T)*8 - (Min != 0)) : Digits,
       digits10          = (digits * 30103) / 100000,
       is_signed         = ((T)(-1)<0),
       is_integer        = true,
       is_exact          = true,
       has_infinity      = false,
       has_quiet_NaN     = false,
       has_signaling_NaN = false,
       has_denorm        = 0,
       has_denorm_loss   = false,
       round_style       = 0,
       is_iec559         = false,
       is_bounded        = true,
       is_modulo         = !(is_signed || Max == 1 /*is bool*/),
       max_digits10      = 0,
       radix             = 2,
       min_exponent      = 0,
       min_exponent10    = 0,
       max_exponent      = 0,
       max_exponent10    = 0,
       tinyness_before   = false,
       traps             = false
	};
};
} // namespace __jitify_detail
template<typename T> struct numeric_limits {
    enum { is_specialized = false };
};
template<> struct numeric_limits<bool>               : public 
__jitify_detail::IntegerLimits<bool,              false,    true,1> {};
template<> struct numeric_limits<char>               : public 
__jitify_detail::IntegerLimits<char,              CHAR_MIN, CHAR_MAX> 
{};
template<> struct numeric_limits<signed char>        : public 
__jitify_detail::IntegerLimits<signed char,       SCHAR_MIN,SCHAR_MAX> 
{};
template<> struct numeric_limits<unsigned char>      : public 
__jitify_detail::IntegerLimits<unsigned char,     0,        UCHAR_MAX> 
{};
template<> struct numeric_limits<wchar_t>            : public 
__jitify_detail::IntegerLimits<wchar_t,           WCHAR_MIN, WCHAR_MAX> {};
template<> struct numeric_limits<short>              : public 
__jitify_detail::IntegerLimits<short,             SHRT_MIN, SHRT_MAX> 
{};
template<> struct numeric_limits<unsigned short>     : public 
__jitify_detail::IntegerLimits<unsigned short,    0,        USHRT_MAX> 
{};
template<> struct numeric_limits<int>                : public 
__jitify_detail::IntegerLimits<int,               INT_MIN,  INT_MAX> {};
template<> struct numeric_limits<unsigned int>       : public 
__jitify_detail::IntegerLimits<unsigned int,      0,        UINT_MAX> 
{};
template<> struct numeric_limits<long>               : public 
__jitify_detail::IntegerLimits<long,              LONG_MIN, LONG_MAX> 
{};
template<> struct numeric_limits<unsigned long>      : public 
__jitify_detail::IntegerLimits<unsigned long,     0,        ULONG_MAX> 
{};
template<> struct numeric_limits<long long>          : public 
__jitify_detail::IntegerLimits<long long,         LLONG_MIN,LLONG_MAX> 
{};
template<> struct numeric_limits<unsigned long long> : public 
__jitify_detail::IntegerLimits<unsigned long long,0,        ULLONG_MAX> 
{};
//template<typename T> struct numeric_limits { static const bool 
//is_signed = ((T)(-1)<0); };
template<> struct numeric_limits<float>              : public 
__jitify_detail::FloatLimits 
{};
template<> struct numeric_limits<double>             : public 
__jitify_detail::DoubleLimits 
{};
}  // namespace std
)";

// TODO: This is highly incomplete
static const char* jitsafe_header_type_traits = R"(
    #pragma once
    #if __cplusplus >= 201103L
    namespace std {

    template<bool B, class T = void> struct enable_if {};
    template<class T>                struct enable_if<true, T> { typedef T type; };
    #if __cplusplus >= 201402L
    template< bool B, class T = void > using enable_if_t = typename enable_if<B,T>::type;
    #endif

    struct true_type  {
      enum { value = true };
      operator bool() const { return true; }
    };
    struct false_type {
      enum { value = false };
      operator bool() const { return false; }
    };

    template<typename T> struct is_floating_point    : false_type {};
    template<> struct is_floating_point<float>       :  true_type {};
    template<> struct is_floating_point<double>      :  true_type {};
    template<> struct is_floating_point<long double> :  true_type {};
    #if __cplusplus >= 201703L
    template<typename T> inline constexpr bool is_floating_point_v = is_floating_point<T>::value;
    #endif  // __cplusplus >= 201703L

    template<class T> struct is_integral              : false_type {};
    template<> struct is_integral<bool>               :  true_type {};
    template<> struct is_integral<char>               :  true_type {};
    template<> struct is_integral<signed char>        :  true_type {};
    template<> struct is_integral<unsigned char>      :  true_type {};
    template<> struct is_integral<short>              :  true_type {};
    template<> struct is_integral<unsigned short>     :  true_type {};
    template<> struct is_integral<int>                :  true_type {};
    template<> struct is_integral<unsigned int>       :  true_type {};
    template<> struct is_integral<long>               :  true_type {};
    template<> struct is_integral<unsigned long>      :  true_type {};
    template<> struct is_integral<long long>          :  true_type {};
    template<> struct is_integral<unsigned long long> :  true_type {};
    #if __cplusplus >= 201703L
    template<typename T> inline constexpr bool is_integral_v = is_integral<T>::value;
    #endif  // __cplusplus >= 201703L

    template<typename T> struct is_signed    : false_type {};
    template<> struct is_signed<float>       :  true_type {};
    template<> struct is_signed<double>      :  true_type {};
    template<> struct is_signed<long double> :  true_type {};
    template<> struct is_signed<signed char> :  true_type {};
    template<> struct is_signed<short>       :  true_type {};
    template<> struct is_signed<int>         :  true_type {};
    template<> struct is_signed<long>        :  true_type {};
    template<> struct is_signed<long long>   :  true_type {};

    template<typename T> struct is_unsigned             : false_type {};
    template<> struct is_unsigned<unsigned char>      :  true_type {};
    template<> struct is_unsigned<unsigned short>     :  true_type {};
    template<> struct is_unsigned<unsigned int>       :  true_type {};
    template<> struct is_unsigned<unsigned long>      :  true_type {};
    template<> struct is_unsigned<unsigned long long> :  true_type {};

    template<typename T, typename U> struct is_same      : false_type {};
    template<typename T>             struct is_same<T,T> :  true_type {};
    #if __cplusplus >= 201703L
    template<typename T, typename U> inline constexpr bool is_same_v = is_same<T, U>::value;
    #endif  // __cplusplus >= 201703L

    template<class T> struct is_array : false_type {};
    template<class T> struct is_array<T[]> : true_type {};
    template<class T, size_t N> struct is_array<T[N]> : true_type {};

    //partial implementation only of is_function
    template<class> struct is_function : false_type { };
    template<class Ret, class... Args> struct is_function<Ret(Args...)> : true_type {}; //regular
    template<class Ret, class... Args> struct is_function<Ret(Args......)> : true_type {}; // variadic

    template<class> struct result_of;
    template<class F, typename... Args>
    struct result_of<F(Args...)> {
    // TODO: This is a hack; a proper implem is quite complicated.
    typedef typename F::result_type type;
    };

    template<class T> struct is_pointer                    : false_type {};
    template<class T> struct is_pointer<T*>                : true_type {};
    template<class T> struct is_pointer<T* const>          : true_type {};
    template<class T> struct is_pointer<T* volatile>       : true_type {};
    template<class T> struct is_pointer<T* const volatile> : true_type {};
    #if __cplusplus >= 201703L
    template< class T > inline constexpr bool is_pointer_v = is_pointer<T>::value;
    #endif  // __cplusplus >= 201703L

    template <class T> struct remove_pointer { typedef T type; };
    template <class T> struct remove_pointer<T*> { typedef T type; };
    template <class T> struct remove_pointer<T* const> { typedef T type; };
    template <class T> struct remove_pointer<T* volatile> { typedef T type; };
    template <class T> struct remove_pointer<T* const volatile> { typedef T type; };

    template <class T> struct remove_reference { typedef T type; };
    template <class T> struct remove_reference<T&> { typedef T type; };
    template <class T> struct remove_reference<T&&> { typedef T type; };
    #if __cplusplus >= 201402L
    template< class T > using remove_reference_t = typename remove_reference<T>::type;
    #endif

    template<class T> struct remove_extent { typedef T type; };
    template<class T> struct remove_extent<T[]> { typedef T type; };
    template<class T, size_t N> struct remove_extent<T[N]> { typedef T type; };
    #if __cplusplus >= 201402L
    template< class T > using remove_extent_t = typename remove_extent<T>::type;
    #endif

    template< class T > struct remove_const          { typedef T type; };
    template< class T > struct remove_const<const T> { typedef T type; };
    template< class T > struct remove_volatile             { typedef T type; };
    template< class T > struct remove_volatile<volatile T> { typedef T type; };
    template< class T > struct remove_cv { typedef typename remove_volatile<typename remove_const<T>::type>::type type; };
    #if __cplusplus >= 201402L
    template< class T > using remove_cv_t       = typename remove_cv<T>::type;
    template< class T > using remove_const_t    = typename remove_const<T>::type;
    template< class T > using remove_volatile_t = typename remove_volatile<T>::type;
    #endif

    template<bool B, class T, class F> struct conditional { typedef T type; };
    template<class T, class F> struct conditional<false, T, F> { typedef F type; };
    #if __cplusplus >= 201402L
    template< bool B, class T, class F > using conditional_t = typename conditional<B,T,F>::type;
    #endif

    namespace __jitify_detail {
    template< class T, bool is_function_type = false > struct add_pointer { using type = typename remove_reference<T>::type*; };
    template< class T > struct add_pointer<T, true> { using type = T; };
    template< class T, class... Args > struct add_pointer<T(Args...), true> { using type = T(*)(Args...); };
    template< class T, class... Args > struct add_pointer<T(Args..., ...), true> { using type = T(*)(Args..., ...); };
    }  // namespace __jitify_detail
    template< class T > struct add_pointer : __jitify_detail::add_pointer<T, is_function<T>::value> {};
    #if __cplusplus >= 201402L
    template< class T > using add_pointer_t = typename add_pointer<T>::type;
    #endif

    template< class T > struct decay {
    private:
      typedef typename remove_reference<T>::type U;
    public:
      typedef typename conditional<is_array<U>::value, typename remove_extent<U>::type*,
        typename conditional<is_function<U>::value,typename add_pointer<U>::type,typename remove_cv<U>::type
        >::type>::type type;
    };
    #if __cplusplus >= 201402L
    template< class T > using decay_t = typename decay<T>::type;
    #endif

    template<class T, T v>
    struct integral_constant {
    static constexpr T value = v;
    typedef T value_type;
    typedef integral_constant type; // using injected-class-name
    constexpr operator value_type() const noexcept { return value; }
    #if __cplusplus >= 201402L
    constexpr value_type operator()() const noexcept { return value; }
    #endif
    };

    template<typename T> struct is_arithmetic :
    std::integral_constant<bool, std::is_integral<T>::value ||
                                 std::is_floating_point<T>::value> {};
    #if __cplusplus >= 201703L
    template<typename T> inline constexpr bool is_arithmetic_v = is_arithmetic<T>::value;
    #endif  // __cplusplus >= 201703L

    template<class T> struct is_lvalue_reference : false_type {};
    template<class T> struct is_lvalue_reference<T&> : true_type {};

    template<class T> struct is_rvalue_reference : false_type {};
    template<class T> struct is_rvalue_reference<T&&> : true_type {};

    namespace __jitify_detail {
    template <class T> struct type_identity { using type = T; };
    template <class T> auto add_lvalue_reference(int) -> type_identity<T&>;
    template <class T> auto add_lvalue_reference(...) -> type_identity<T>;
    template <class T> auto add_rvalue_reference(int) -> type_identity<T&&>;
    template <class T> auto add_rvalue_reference(...) -> type_identity<T>;
    } // namespace _jitify_detail

    template <class T> struct add_lvalue_reference : decltype(__jitify_detail::add_lvalue_reference<T>(0)) {};
    template <class T> struct add_rvalue_reference : decltype(__jitify_detail::add_rvalue_reference<T>(0)) {};
    #if __cplusplus >= 201402L
    template <class T> using add_lvalue_reference_t = typename add_lvalue_reference<T>::type;
    template <class T> using add_rvalue_reference_t = typename add_rvalue_reference<T>::type;
    #endif

    template<typename T> struct is_const          : public false_type {};
    template<typename T> struct is_const<const T> : public true_type {};

    template<typename T> struct is_volatile             : public false_type {};
    template<typename T> struct is_volatile<volatile T> : public true_type {};

    template<typename T> struct is_void             : public false_type {};
    template<>           struct is_void<void>       : public true_type {};
    template<>           struct is_void<const void> : public true_type {};

    template<typename T> struct is_reference     : public false_type {};
    template<typename T> struct is_reference<T&> : public true_type {};

    template<typename _Tp, bool = (is_void<_Tp>::value || is_reference<_Tp>::value)>
    struct __add_reference_helper { typedef _Tp&    type; };

    template<typename _Tp> struct __add_reference_helper<_Tp, true> { typedef _Tp     type; };
    template<typename _Tp> struct add_reference : public __add_reference_helper<_Tp>{};

    namespace __jitify_detail {
    template<typename T> struct is_int_or_cref {
    typedef typename remove_reference<T>::type type_sans_ref;
    static const bool value = (is_integral<T>::value || (is_integral<type_sans_ref>::value
      && is_const<type_sans_ref>::value && !is_volatile<type_sans_ref>::value));
    }; // end is_int_or_cref
    template<typename From, typename To> struct is_convertible_sfinae {
    private:
    typedef char                          yes;
    typedef struct { char two_chars[2]; } no;
    static inline yes   test(To) { return yes(); }
    static inline no    test(...) { return no(); }
    static inline typename remove_reference<From>::type& from() { typename remove_reference<From>::type* ptr = 0; return *ptr; }
    public:
    static const bool value = sizeof(test(from())) == sizeof(yes);
    }; // end is_convertible_sfinae
    template<typename From, typename To> struct is_convertible_needs_simple_test {
    static const bool from_is_void      = is_void<From>::value;
    static const bool to_is_void        = is_void<To>::value;
    static const bool from_is_float     = is_floating_point<typename remove_reference<From>::type>::value;
    static const bool to_is_int_or_cref = is_int_or_cref<To>::value;
    static const bool value = (from_is_void || to_is_void || (from_is_float && to_is_int_or_cref));
    }; // end is_convertible_needs_simple_test
    template<typename From, typename To, bool = is_convertible_needs_simple_test<From,To>::value>
    struct is_convertible {
    static const bool value = (is_void<To>::value || (is_int_or_cref<To>::value && !is_void<From>::value));
    }; // end is_convertible
    template<typename From, typename To> struct is_convertible<From, To, false> {
    static const bool value = (is_convertible_sfinae<typename add_reference<From>::type, To>::value);
    }; // end is_convertible
    } // end __jitify_detail
    // implementation of is_convertible taken from thrust's pre C++11 path
    template<typename From, typename To> struct is_convertible
    : public integral_constant<bool, __jitify_detail::is_convertible<From, To>::value>
    { }; // end is_convertible

    template<class A, class B> struct is_base_of { };

    template<size_t len, size_t alignment> struct aligned_storage { struct type { alignas(alignment) char data[len]; }; };
    template <class T> struct alignment_of : std::integral_constant<size_t,alignof(T)> {};

    template <typename T> struct make_unsigned;
    template <> struct make_unsigned<signed char>        { typedef unsigned char type; };
    template <> struct make_unsigned<signed short>       { typedef unsigned short type; };
    template <> struct make_unsigned<signed int>         { typedef unsigned int type; };
    template <> struct make_unsigned<signed long>        { typedef unsigned long type; };
    template <> struct make_unsigned<signed long long>   { typedef unsigned long long type; };
    template <> struct make_unsigned<unsigned char>      { typedef unsigned char type; };
    template <> struct make_unsigned<unsigned short>     { typedef unsigned short type; };
    template <> struct make_unsigned<unsigned int>       { typedef unsigned int type; };
    template <> struct make_unsigned<unsigned long>      { typedef unsigned long type; };
    template <> struct make_unsigned<unsigned long long> { typedef unsigned long long type; };
    template <> struct make_unsigned<char>               { typedef unsigned char type; };
    #if defined _WIN32 || defined _WIN64
    template <> struct make_unsigned<wchar_t>            { typedef unsigned short type; };
    #else
    template <> struct make_unsigned<wchar_t>            { typedef unsigned int type; };
    #endif

    template <typename T> struct make_signed;
    template <> struct make_signed<signed char>        { typedef signed char type; };
    template <> struct make_signed<signed short>       { typedef signed short type; };
    template <> struct make_signed<signed int>         { typedef signed int type; };
    template <> struct make_signed<signed long>        { typedef signed long type; };
    template <> struct make_signed<signed long long>   { typedef signed long long type; };
    template <> struct make_signed<unsigned char>      { typedef signed char type; };
    template <> struct make_signed<unsigned short>     { typedef signed short type; };
    template <> struct make_signed<unsigned int>       { typedef signed int type; };
    template <> struct make_signed<unsigned long>      { typedef signed long type; };
    template <> struct make_signed<unsigned long long> { typedef signed long long type; };
    template <> struct make_signed<char>               { typedef signed char type; };
    #if defined _WIN32 || defined _WIN64
    template <> struct make_signed<wchar_t>            { typedef signed short type; };
    #else
    template <> struct make_signed<wchar_t>            { typedef signed int type; };
    #endif

    }  // namespace std
    #endif // c++11
)";

// TODO: INT_FAST8_MAX et al. and a few other misc constants
static const char* jitsafe_header_stdint_h =
    "#pragma once\n"
    "#if __has_include(<cuda/std/cstdint>)\n"
    " #include <cuda/std/climits>\n"
    " #include <cuda/std/cstdint>\n"
    " #define __jitify_using_libcudacxx\n"
    "#endif\n"
    "#include <climits>\n"
    "namespace __jitify_stdint_ns {\n"
    "typedef signed char      int8_t;\n"
    "typedef signed short     int16_t;\n"
    "typedef signed int       int32_t;\n"
    "typedef signed long long int64_t;\n"
    "typedef signed char      int_fast8_t;\n"
    "typedef signed short     int_fast16_t;\n"
    "typedef signed int       int_fast32_t;\n"
    "typedef signed long long int_fast64_t;\n"
    "typedef signed char      int_least8_t;\n"
    "typedef signed short     int_least16_t;\n"
    "typedef signed int       int_least32_t;\n"
    "typedef signed long long int_least64_t;\n"
    "typedef signed long long intmax_t;\n"
    "typedef unsigned char      uint8_t;\n"
    "typedef unsigned short     uint16_t;\n"
    "typedef unsigned int       uint32_t;\n"
    "typedef unsigned long long uint64_t;\n"
    "typedef unsigned char      uint_fast8_t;\n"
    "typedef unsigned short     uint_fast16_t;\n"
    "typedef unsigned int       uint_fast32_t;\n"
    "typedef unsigned long long uint_fast64_t;\n"
    "typedef unsigned char      uint_least8_t;\n"
    "typedef unsigned short     uint_least16_t;\n"
    "typedef unsigned int       uint_least32_t;\n"
    "typedef unsigned long long uint_least64_t;\n"
    "typedef unsigned long long uintmax_t;\n"
    "#ifndef __jitify_using_libcudacxx\n"
    " typedef signed long      intptr_t; //optional\n"
    " #define INT8_MIN    SCHAR_MIN\n"
    " #define INT16_MIN   SHRT_MIN\n"
    " #define INT32_MIN   INT_MIN\n"
    " #define INT64_MIN   LLONG_MIN\n"
    " #define INT8_MAX    SCHAR_MAX\n"
    " #define INT16_MAX   SHRT_MAX\n"
    " #define INT32_MAX   INT_MAX\n"
    " #define INT64_MAX   LLONG_MAX\n"
    " #define UINT8_MAX   UCHAR_MAX\n"
    " #define UINT16_MAX  USHRT_MAX\n"
    " #define UINT32_MAX  UINT_MAX\n"
    " #define UINT64_MAX  ULLONG_MAX\n"
    " #define INTPTR_MIN  LONG_MIN\n"
    " #define INTMAX_MIN  LLONG_MIN\n"
    " #define INTPTR_MAX  LONG_MAX\n"
    " #define INTMAX_MAX  LLONG_MAX\n"
    " #define UINTPTR_MAX ULONG_MAX\n"
    " #define UINTMAX_MAX ULLONG_MAX\n"
    " #define PTRDIFF_MIN INTPTR_MIN\n"
    " #define PTRDIFF_MAX INTPTR_MAX\n"
    " #define SIZE_MAX    UINT64_MAX\n"
    "#endif\n"
    "#if defined _WIN32 || defined _WIN64\n"
    " #define WCHAR_MIN   0\n"
    " #define WCHAR_MAX   USHRT_MAX\n"
    " #ifndef __jitify_using_libcudacxx\n"
    "  typedef unsigned long long uintptr_t; //optional\n"
    " #endif\n"
    "#else\n"
    " #define WCHAR_MIN   INT_MIN\n"
    " #define WCHAR_MAX   INT_MAX\n"
    " #ifndef __jitify_using_libcudacxx\n"
    "  typedef unsigned long      uintptr_t; //optional\n"
    " #endif\n"
    "#endif\n"
    "} // namespace __jitify_stdint_ns\n"
    "namespace std { using namespace __jitify_stdint_ns; }\n"
    "using namespace __jitify_stdint_ns;\n";

// TODO: offsetof
static const char* jitsafe_header_stddef_h =
    "#pragma once\n"
    "#include <climits>\n"
    "namespace __jitify_stddef_ns {\n"
    "#if __cplusplus >= 201103L\n"
    "typedef decltype(nullptr) nullptr_t;\n"
    "#if defined(_MSC_VER)\n"
    "  typedef double max_align_t;\n"
    "#elif defined(__APPLE__)\n"
    "  typedef long double max_align_t;\n"
    "#else\n"
    "  // Define max_align_t to match the GCC definition.\n"
    "  typedef struct {\n"
    "    long long __jitify_max_align_nonce1\n"
    "        __attribute__((__aligned__(__alignof__(long long))));\n"
    "    long double __jitify_max_align_nonce2\n"
    "        __attribute__((__aligned__(__alignof__(long double))));\n"
    "  } max_align_t;\n"
    "#endif\n"
    "#endif  // __cplusplus >= 201103L\n"
    "#if __cplusplus >= 201703L\n"
    "enum class byte : unsigned char {};\n"
    "#endif  // __cplusplus >= 201703L\n"
    "} // namespace __jitify_stddef_ns\n"
    "namespace std {\n"
    "  // NVRTC provides built-in definitions of ::size_t and ::ptrdiff_t.\n"
    "  using ::size_t;\n"
    "  using ::ptrdiff_t;\n"
    "  using namespace __jitify_stddef_ns;\n"
    "} // namespace std\n"
    "using namespace __jitify_stddef_ns;\n";

static const char* jitsafe_header_stdlib_h =
    "#pragma once\n"
    "#include <stddef.h>\n";
static const char* jitsafe_header_stdio_h =
    "#pragma once\n"
    "#include <stddef.h>\n"
    "#define FILE int\n"
    "int fflush ( FILE * stream );\n"
    "int fprintf ( FILE * stream, const char * format, ... );\n";

static const char* jitsafe_header_string_h =
    "#pragma once\n"
    "char* strcpy ( char * destination, const char * source );\n"
    "int strcmp ( const char * str1, const char * str2 );\n"
    "char* strerror( int errnum );\n";

static const char* jitsafe_header_cstring =
    "#pragma once\n"
    "\n"
    "namespace __jitify_cstring_ns {\n"
    "char* strcpy ( char * destination, const char * source );\n"
    "int strcmp ( const char * str1, const char * str2 );\n"
    "char* strerror( int errnum );\n"
    "} // namespace __jitify_cstring_ns\n"
    "namespace std { using namespace __jitify_cstring_ns; }\n"
    "using namespace __jitify_cstring_ns;\n";

// HACK TESTING (WAR for cub)
static const char* jitsafe_header_iostream =
    "#pragma once\n"
    "#include <ostream>\n"
    "#include <istream>\n";
// HACK TESTING (WAR for Thrust)
static const char* jitsafe_header_ostream =
    "#pragma once\n"
    "\n"
    "namespace std {\n"
    "template<class CharT,class Traits=void>\n"  // = std::char_traits<CharT>
                                                 // >\n"
    "struct basic_ostream {\n"
    "};\n"
    "typedef basic_ostream<char> ostream;\n"
    "ostream& endl(ostream& os);\n"
    "ostream& operator<<( ostream&, ostream& (*f)( ostream& ) );\n"
    "template< class CharT, class Traits > basic_ostream<CharT, Traits>& endl( "
    "basic_ostream<CharT, Traits>& os );\n"
    "template< class CharT, class Traits > basic_ostream<CharT, Traits>& "
    "operator<<( basic_ostream<CharT,Traits>& os, const char* c );\n"
    "#if __cplusplus >= 201103L\n"
    "template< class CharT, class Traits, class T > basic_ostream<CharT, "
    "Traits>& operator<<( basic_ostream<CharT,Traits>&& os, const T& value );\n"
    "#endif  // __cplusplus >= 201103L\n"
    "}  // namespace std\n";

static const char* jitsafe_header_istream =
    "#pragma once\n"
    "\n"
    "namespace std {\n"
    "template<class CharT,class Traits=void>\n"  // = std::char_traits<CharT>
                                                 // >\n"
    "struct basic_istream {\n"
    "};\n"
    "typedef basic_istream<char> istream;\n"
    "}  // namespace std\n";

static const char* jitsafe_header_sstream =
    "#pragma once\n"
    "#include <ostream>\n"
    "#include <istream>\n";

static const char* jitsafe_header_utility =
    "#pragma once\n"
    "namespace std {\n"
    "template<class T1, class T2>\n"
    "struct pair {\n"
    "	T1 first;\n"
    "	T2 second;\n"
    "	inline pair() {}\n"
    "	inline pair(T1 const& first_, T2 const& second_)\n"
    "		: first(first_), second(second_) {}\n"
    "	// TODO: Standard includes many more constructors...\n"
    "	// TODO: Comparison operators\n"
    "};\n"
    "template<class T1, class T2>\n"
    "pair<T1,T2> make_pair(T1 const& first, T2 const& second) {\n"
    "	return pair<T1,T2>(first, second);\n"
    "}\n"
    "}  // namespace std\n";

// TODO: incomplete
static const char* jitsafe_header_vector =
    "#pragma once\n"
    "namespace std {\n"
    "template<class T, class Allocator=void>\n"  // = std::allocator> \n"
    "struct vector {\n"
    "};\n"
    "}  // namespace std\n";

// TODO: incomplete
static const char* jitsafe_header_string =
    "#pragma once\n"
    "namespace std {\n"
    "template<class CharT,class Traits=void,class Allocator=void>\n"
    "struct basic_string {\n"
    "basic_string();\n"
    "basic_string( const CharT* s );\n"  //, const Allocator& alloc =
                                         // Allocator() );\n"
    "const CharT* c_str() const;\n"
    "bool empty() const;\n"
    "void operator+=(const char *);\n"
    "void operator+=(const basic_string &);\n"
    "};\n"
    "typedef basic_string<char> string;\n"
    "}  // namespace std\n";

// TODO: incomplete
static const char* jitsafe_header_stdexcept =
    "#pragma once\n"
    "namespace std {\n"
    "struct runtime_error {\n"
    "explicit runtime_error( const std::string& what_arg );"
    "explicit runtime_error( const char* what_arg );"
    "virtual const char* what() const;\n"
    "};\n"
    "}  // namespace std\n";

// TODO: incomplete
static const char* jitsafe_header_complex =
    "#pragma once\n"
    "namespace std {\n"
    "template<typename T>\n"
    "class complex {\n"
    "	T _real;\n"
    "	T _imag;\n"
    "public:\n"
    "	complex() : _real(0), _imag(0) {}\n"
    "	complex(T const& real, T const& imag)\n"
    "		: _real(real), _imag(imag) {}\n"
    "	complex(T const& real)\n"
    "               : _real(real), _imag(static_cast<T>(0)) {}\n"
    "	T const& real() const { return _real; }\n"
    "	T&       real()       { return _real; }\n"
    "	void real(const T &r) { _real = r; }\n"
    "	T const& imag() const { return _imag; }\n"
    "	T&       imag()       { return _imag; }\n"
    "	void imag(const T &i) { _imag = i; }\n"
    "       complex<T>& operator+=(const complex<T> z)\n"
    "         { _real += z.real(); _imag += z.imag(); return *this; }\n"
    "};\n"
    "template<typename T>\n"
    "complex<T> operator*(const complex<T>& lhs, const complex<T>& rhs)\n"
    "  { return complex<T>(lhs.real()*rhs.real()-lhs.imag()*rhs.imag(),\n"
    "                      lhs.real()*rhs.imag()+lhs.imag()*rhs.real()); }\n"
    "template<typename T>\n"
    "complex<T> operator*(const complex<T>& lhs, const T & rhs)\n"
    "  { return complexs<T>(lhs.real()*rhs,lhs.imag()*rhs); }\n"
    "template<typename T>\n"
    "complex<T> operator*(const T& lhs, const complex<T>& rhs)\n"
    "  { return complexs<T>(rhs.real()*lhs,rhs.imag()*lhs); }\n"
    "}  // namespace std\n";

// TODO: This is incomplete (missing binary and integer funcs, macros,
// constants, types)
static const char* jitsafe_header_math_h =
    "#pragma once\n"
    "namespace __jitify_math_ns {\n"
    "#if __cplusplus >= 201103L\n"
    "#define DEFINE_MATH_UNARY_FUNC_WRAPPER(f) \\\n"
    "	inline double      f(double x)         { return ::f(x); } \\\n"
    "	inline float       f##f(float x)       { return ::f(x); } \\\n"
    "	/*inline long double f##l(long double x) { return ::f(x); }*/ \\\n"
    "	inline float       f(float x)          { return ::f(x); } \\\n"
    "	/*inline long double f(long double x)    { return ::f(x); }*/\n"
    "#else\n"
    "#define DEFINE_MATH_UNARY_FUNC_WRAPPER(f) \\\n"
    "	inline double      f(double x)         { return ::f(x); } \\\n"
    "	inline float       f##f(float x)       { return ::f(x); } \\\n"
    "	/*inline long double f##l(long double x) { return ::f(x); }*/\n"
    "#endif\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(cos)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(sin)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(tan)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(acos)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(asin)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(atan)\n"
    "template<typename T> inline T atan2(T y, T x) { return ::atan2(y, x); }\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(cosh)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(sinh)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(tanh)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(exp)\n"
    "template<typename T> inline T frexp(T x, int* exp) { return ::frexp(x, "
    "exp); }\n"
    "template<typename T> inline T ldexp(T x, int  exp) { return ::ldexp(x, "
    "exp); }\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(log)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(log10)\n"
    "template<typename T> inline T modf(T x, T* intpart) { return ::modf(x, "
    "intpart); }\n"
    "template<typename T> inline T pow(T x, T y) { return ::pow(x, y); }\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(sqrt)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(ceil)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(floor)\n"
    "template<typename T> inline T fmod(T n, T d) { return ::fmod(n, d); }\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(fabs)\n"
    "template<typename T> inline T abs(T x) { return ::abs(x); }\n"
    "#if __cplusplus >= 201103L\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(acosh)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(asinh)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(atanh)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(exp2)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(expm1)\n"
    "template<typename T> inline int ilogb(T x) { return ::ilogb(x); }\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(log1p)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(log2)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(logb)\n"
    "template<typename T> inline T scalbn (T x, int n)  { return ::scalbn(x, "
    "n); }\n"
    "template<typename T> inline T scalbln(T x, long n) { return ::scalbn(x, "
    "n); }\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(cbrt)\n"
    "template<typename T> inline T hypot(T x, T y) { return ::hypot(x, y); }\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(erf)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(erfc)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(tgamma)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(lgamma)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(trunc)\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(round)\n"
    "template<typename T> inline long lround(T x) { return ::lround(x); }\n"
    "template<typename T> inline long long llround(T x) { return ::llround(x); "
    "}\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(rint)\n"
    "template<typename T> inline long lrint(T x) { return ::lrint(x); }\n"
    "template<typename T> inline long long llrint(T x) { return ::llrint(x); "
    "}\n"
    "DEFINE_MATH_UNARY_FUNC_WRAPPER(nearbyint)\n"
    // TODO: remainder, remquo, copysign, nan, nextafter, nexttoward, fdim,
    // fmax, fmin, fma
    "#endif\n"
    "#undef DEFINE_MATH_UNARY_FUNC_WRAPPER\n"
    "} // namespace __jitify_math_ns\n"
    "namespace std { using namespace __jitify_math_ns; }\n"
    "#define M_PI 3.14159265358979323846\n"
    // Note: Global namespace already includes CUDA math funcs
    "//using namespace __jitify_math_ns;\n";

static const char* jitsafe_header_memory_h = R"(
    #pragma once
    #include <string.h>
 )";

// TODO: incomplete
static const char* jitsafe_header_mutex = R"(
    #pragma once
    #if __cplusplus >= 201103L
    namespace std {
    class mutex {
    public:
    void lock();
    bool try_lock();
    void unlock();
    };
    }  // namespace std
    #endif
 )";

static const char* jitsafe_header_algorithm = R"(
    #pragma once
    #if __cplusplus >= 201103L
    namespace std {

    #if __cplusplus == 201103L
    #define JITIFY_CXX14_CONSTEXPR
    #else
    #define JITIFY_CXX14_CONSTEXPR constexpr
    #endif

    template<class T> JITIFY_CXX14_CONSTEXPR const T& max(const T& a, const T& b)
    {
      return (b > a) ? b : a;
    }
    template<class T> JITIFY_CXX14_CONSTEXPR const T& min(const T& a, const T& b)
    {
      return (b < a) ? b : a;
    }

    }  // namespace std
    #endif
 )";

static const char* jitsafe_header_time_h = R"(
    #pragma once
    #define NULL 0
    #define CLOCKS_PER_SEC 1000000
    namespace __jitify_time_ns {
    typedef long time_t;
    struct tm {
      int tm_sec;
      int tm_min;
      int tm_hour;
      int tm_mday;
      int tm_mon;
      int tm_year;
      int tm_wday;
      int tm_yday;
      int tm_isdst;
    };
    #if __cplusplus >= 201703L
    struct timespec {
      time_t tv_sec;
      long tv_nsec;
    };
    #endif
    }  // namespace __jitify_time_ns
    namespace std {
      // NVRTC provides built-in definitions of ::size_t and ::clock_t.
      using ::size_t;
      using ::clock_t;
      using namespace __jitify_time_ns;
    }
    using namespace __jitify_time_ns;
 )";

static const char* jitsafe_header_tuple = R"(
    #pragma once
    #if __cplusplus >= 201103L
    namespace std {
    template<class... Types > class tuple;

    template< size_t I, class T >
    struct tuple_element;
    // recursive case
    template< size_t I, class Head, class... Tail >
    struct tuple_element<I, tuple<Head, Tail...>>
        : tuple_element<I-1, tuple<Tail...>> { };
    // base case
    template< class Head, class... Tail >
    struct tuple_element<0, tuple<Head, Tail...>> {
      using type = Head;
    };
    } // namespace std
    #endif
 )";

static const char* jitsafe_header_assert = R"(
    #pragma once
 )";

// WAR: These need to be pre-included as a workaround for NVRTC implicitly using
// /usr/include as an include path. The other built-in headers will be included
// lazily as needed.
static const char* preinclude_jitsafe_header_names[] = {"jitify_preinclude.h",
                                                        "limits.h",
                                                        "math.h",
                                                        "memory.h",
                                                        "stdint.h",
                                                        "stdlib.h",
                                                        "stdio.h",
                                                        "string.h",
                                                        "time.h",
                                                        "assert.h"};

template <class T, int N>
int array_size(T (&)[N]) {
  return N;
}
const int preinclude_jitsafe_headers_count =
    array_size(preinclude_jitsafe_header_names);

static const std::map<std::string, std::string>& get_jitsafe_headers_map() {
  static const std::map<std::string, std::string> jitsafe_headers_map = {
      {"jitify_preinclude.h", jitsafe_header_preinclude_h},
      {"float.h", jitsafe_header_float_h},
      {"cfloat", jitsafe_header_float_h},
      {"limits.h", jitsafe_header_limits_h},
      {"climits", jitsafe_header_limits_h},
      {"stdint.h", jitsafe_header_stdint_h},
      {"cstdint", jitsafe_header_stdint_h},
      {"stddef.h", jitsafe_header_stddef_h},
      {"cstddef", jitsafe_header_stddef_h},
      {"stdlib.h", jitsafe_header_stdlib_h},
      {"cstdlib", jitsafe_header_stdlib_h},
      {"stdio.h", jitsafe_header_stdio_h},
      {"cstdio", jitsafe_header_stdio_h},
      {"string.h", jitsafe_header_string_h},
      {"cstring", jitsafe_header_cstring},
      {"iterator", jitsafe_header_iterator},
      {"limits", jitsafe_header_limits},
      {"type_traits", jitsafe_header_type_traits},
      {"utility", jitsafe_header_utility},
      {"math.h", jitsafe_header_math_h},
      {"cmath", jitsafe_header_math_h},
      {"memory.h", jitsafe_header_memory_h},
      {"complex", jitsafe_header_complex},
      {"iostream", jitsafe_header_iostream},
      {"ostream", jitsafe_header_ostream},
      {"istream", jitsafe_header_istream},
      {"sstream", jitsafe_header_sstream},
      {"vector", jitsafe_header_vector},
      {"string", jitsafe_header_string},
      {"stdexcept", jitsafe_header_stdexcept},
      {"mutex", jitsafe_header_mutex},
      {"algorithm", jitsafe_header_algorithm},
      {"time.h", jitsafe_header_time_h},
      {"ctime", jitsafe_header_time_h},
      {"tuple", jitsafe_header_tuple},
      {"assert.h", jitsafe_header_assert},
      {"cassert", jitsafe_header_assert}};
  return jitsafe_headers_map;
}

inline void add_options_from_env(std::vector<std::string>& options) {
  // Add options from environment variable
  const char* env_options = std::getenv("JITIFY_OPTIONS");
  if (env_options) {
    std::stringstream ss;
    ss << env_options;
    std::string opt;
    while (!(ss >> opt).fail()) {
      options.push_back(opt);
    }
  }
  // Add options from JITIFY_OPTIONS macro
#ifdef JITIFY_OPTIONS
#define JITIFY_TOSTRING_IMPL(x) #x
#define JITIFY_TOSTRING(x) JITIFY_TOSTRING_IMPL(x)
  std::stringstream ss;
  ss << JITIFY_TOSTRING(JITIFY_OPTIONS);
  std::string opt;
  while (!(ss >> opt).fail()) {
    options.push_back(opt);
  }
#undef JITIFY_TOSTRING
#undef JITIFY_TOSTRING_IMPL
#endif  // JITIFY_OPTIONS
}

inline void detect_and_add_cuda_arch(std::vector<std::string>& options) {
  for (int i = 0; i < (int)options.size(); ++i) {
    // Note that this will also match the middle of "--gpu-architecture".
    if (options[i].find("-arch") != std::string::npos) {
      // Arch already specified in options
      return;
    }
  }
  // Use the compute capability of the current device
  // TODO: Check these API calls for errors
  cudaError_t status;
  int device;
  status = cudaGetDevice(&device);
  if (status != cudaSuccess) {
    throw std::runtime_error(
        std::string(
            "Failed to detect GPU architecture: cudaGetDevice failed: ") +
        cudaGetErrorString(status));
  }
  int cc_major;
  cudaDeviceGetAttribute(&cc_major, cudaDevAttrComputeCapabilityMajor, device);
  int cc_minor;
  cudaDeviceGetAttribute(&cc_minor, cudaDevAttrComputeCapabilityMinor, device);
  int cc = cc_major * 10 + cc_minor;
  // Note: We must limit the architecture to the max supported by the current
  //         version of NVRTC, otherwise newer hardware will cause errors
  //         on older versions of CUDA.
  // TODO: It would be better to detect this somehow, rather than hard-coding it

  // Tegra chips do not have forwards compatibility so we need to special case
  // them.
  bool is_tegra = ((cc_major == 3 && cc_minor == 2) ||  // Logan
                   (cc_major == 5 && cc_minor == 3) ||  // Erista
                   (cc_major == 6 && cc_minor == 2) ||  // Parker
                   (cc_major == 7 && cc_minor == 2));   // Xavier
  if (!is_tegra) {
    // ensure that future CUDA versions just work (even if suboptimal)
    const int cuda_major = std::min(10, CUDA_VERSION / 1000);
    // clang-format off
    switch (cuda_major) {
      case 10: cc = std::min(cc, 75); break; // Turing
      case  9: cc = std::min(cc, 70); break; // Volta
      case  8: cc = std::min(cc, 61); break; // Pascal
      case  7: cc = std::min(cc, 52); break; // Maxwell
      default:
        throw std::runtime_error("Unexpected CUDA major version " +
                                 std::to_string(cuda_major));
    }
    // clang-format on
  }

  std::stringstream ss;
  ss << cc;
  options.push_back("-arch=compute_" + ss.str());
}

inline void detect_and_add_cxx11_flag(std::vector<std::string>& options) {
  // Reverse loop so we can erase on the fly.
  for (int i = (int)options.size() - 1; i >= 0; --i) {
    if (options[i].find("-std=c++98") != std::string::npos) {
      // NVRTC doesn't support specifying c++98 explicitly, so we remove it.
      options.erase(options.begin() + i);
      return;
    } else if (options[i].find("-std") != std::string::npos) {
      // Some other standard was explicitly specified, don't change anything.
      return;
    }
  }
  // Jitify must be compiled with C++11 support, so we default to enabling it
  // for the JIT-compiled code too.
  options.push_back("-std=c++11");
}

inline void split_compiler_and_linker_options(
    std::vector<std::string> options,
    std::vector<std::string>* compiler_options,
    std::vector<std::string>* linker_files,
    std::vector<std::string>* linker_paths) {
  for (int i = 0; i < (int)options.size(); ++i) {
    std::string opt = options[i];
    std::string flag = opt.substr(0, 2);
    std::string value = opt.substr(2);
    if (flag == "-l") {
      linker_files->push_back(value);
    } else if (flag == "-L") {
      linker_paths->push_back(value);
    } else {
      compiler_options->push_back(opt);
    }
  }
}

inline bool pop_remove_unused_globals_flag(std::vector<std::string>* options) {
  auto it = std::remove_if(
      options->begin(), options->end(), [](const std::string& opt) {
        return opt.find("-remove-unused-globals") != std::string::npos;
      });
  if (it != options->end()) {
    options->resize(it - options->begin());
    return true;
  }
  return false;
}

inline std::string ptx_parse_decl_name(const std::string& line) {
  size_t name_end = line.find_first_of("[;");
  if (name_end == std::string::npos) {
    throw std::runtime_error(
        "Failed to parse .global/.const declaration in PTX: expected a "
        "semicolon");
  }
  size_t name_start_minus1 = line.find_last_of(" \t", name_end);
  if (name_start_minus1 == std::string::npos) {
    throw std::runtime_error(
        "Failed to parse .global/.const declaration in PTX: expected "
        "whitespace");
  }
  size_t name_start = name_start_minus1 + 1;
  std::string name = line.substr(name_start, name_end - name_start);
  return name;
}

inline void ptx_remove_unused_globals(std::string* ptx) {
  std::istringstream iss(*ptx);
  std::vector<std::string> lines;
  std::unordered_map<size_t, std::string> line_num_to_global_name;
  std::unordered_set<std::string> name_set;
  for (std::string line; std::getline(iss, line);) {
    size_t line_num = lines.size();
    lines.push_back(line);
    auto terms = split_string(line);
    if (terms.size() <= 1) continue;  // Ignore lines with no arguments
    if (terms[0].substr(0, 2) == "//") continue;  // Ignore comment lines
    if (terms[0].substr(0, 7) == ".global" ||
        terms[0].substr(0, 6) == ".const") {
      line_num_to_global_name.emplace(line_num, ptx_parse_decl_name(line));
      continue;
    }
    if (terms[0][0] == '.') continue;  // Ignore .version, .reg, .param etc.
    // Note: The first term will always be an instruction name; starting at 1
    // also allows unchecked inspection of the previous term.
    for (int i = 1; i < (int)terms.size(); ++i) {
      if (terms[i].substr(0, 2) == "//") break;  // Ignore comments
      // Note: The characters '.' and '%' are not treated as delimiters.
      const char* token_delims = " \t()[]{},;+-*/~&|^?:=!<>\"'\\";
      for (auto token : split_string(terms[i], -1, token_delims)) {
        if (  // Ignore non-names
            !(std::isalpha(token[0]) || token[0] == '_' || token[0] == '$') ||
            token.find('.') != std::string::npos ||
            // Ignore variable/parameter declarations
            terms[i - 1][0] == '.' ||
            // Ignore branch instructions
            (token == "bra" && terms[i - 1][0] == '@') ||
            // Ignore branch labels
            (token.substr(0, 2) == "BB" &&
             terms[i - 1].substr(0, 3) == "bra")) {
          continue;
        }
        name_set.insert(token);
      }
    }
  }
  std::ostringstream oss;
  for (size_t line_num = 0; line_num < lines.size(); ++line_num) {
    auto it = line_num_to_global_name.find(line_num);
    if (it != line_num_to_global_name.end()) {
      const std::string& name = it->second;
      if (!name_set.count(name)) {
        continue;  // Remove unused .global declaration.
      }
    }
    oss << lines[line_num] << '\n';
  }
  *ptx = oss.str();
}

inline nvrtcResult compile_kernel(std::string program_name,
                                  std::map<std::string, std::string> sources,
                                  std::vector<std::string> options,
                                  std::string instantiation = "",
                                  std::string* log = 0, std::string* ptx = 0,
                                  std::string* mangled_instantiation = 0) {
  std::string program_source = sources[program_name];
  // Build arrays of header names and sources
  std::vector<const char*> header_names_c;
  std::vector<const char*> header_sources_c;
  int num_headers = (int)(sources.size() - 1);
  header_names_c.reserve(num_headers);
  header_sources_c.reserve(num_headers);
  typedef std::map<std::string, std::string> source_map;
  for (source_map::const_iterator iter = sources.begin(); iter != sources.end();
       ++iter) {
    std::string const& name = iter->first;
    std::string const& code = iter->second;
    if (name == program_name) {
      continue;
    }
    header_names_c.push_back(name.c_str());
    header_sources_c.push_back(code.c_str());
  }

  // TODO: This WAR is expected to be unnecessary as of CUDA > 10.2.
  bool should_remove_unused_globals =
      detail::pop_remove_unused_globals_flag(&options);

  std::vector<const char*> options_c(options.size() + 2);
  options_c[0] = "--device-as-default-execution-space";
  options_c[1] = "--pre-include=jitify_preinclude.h";
  for (int i = 0; i < (int)options.size(); ++i) {
    options_c[i + 2] = options[i].c_str();
  }

#if CUDA_VERSION < 8000
  std::string inst_dummy;
  if (!instantiation.empty()) {
    // WAR for no nvrtcAddNameExpression before CUDA 8.0
    // Force template instantiation by adding dummy reference to kernel
    inst_dummy = "__jitify_instantiation";
    program_source +=
        "\nvoid* " + inst_dummy + " = (void*)" + instantiation + ";\n";
  }
#endif

#define CHECK_NVRTC(call)                         \
  do {                                            \
    nvrtcResult check_nvrtc_macro_ret = call;     \
    if (check_nvrtc_macro_ret != NVRTC_SUCCESS) { \
      return check_nvrtc_macro_ret;               \
    }                                             \
  } while (0)

  nvrtcProgram nvrtc_program;
  CHECK_NVRTC(nvrtcCreateProgram(
      &nvrtc_program, program_source.c_str(), program_name.c_str(), num_headers,
      header_sources_c.data(), header_names_c.data()));

  // Ensure nvrtc_program gets destroyed.
  struct ScopedNvrtcProgramDestroyer {
    nvrtcProgram& nvrtc_program_;
    ScopedNvrtcProgramDestroyer(nvrtcProgram& nvrtc_program)
        : nvrtc_program_(nvrtc_program) {}    
    ~ScopedNvrtcProgramDestroyer() { nvrtcDestroyProgram(&nvrtc_program_); }
    ScopedNvrtcProgramDestroyer(const ScopedNvrtcProgramDestroyer&) = delete;
    ScopedNvrtcProgramDestroyer& operator=(const ScopedNvrtcProgramDestroyer&) =
        delete;
  } nvrtc_program_scope_guard{nvrtc_program};

#if CUDA_VERSION >= 8000
  if (!instantiation.empty()) {
    CHECK_NVRTC(nvrtcAddNameExpression(nvrtc_program, instantiation.c_str()));
  }
#endif

  nvrtcResult ret = nvrtcCompileProgram(nvrtc_program, (int)options_c.size(),
                                        options_c.data());
  if (log) {
    size_t logsize;
    CHECK_NVRTC(nvrtcGetProgramLogSize(nvrtc_program, &logsize));
    std::vector<char> vlog(logsize, 0);
    CHECK_NVRTC(nvrtcGetProgramLog(nvrtc_program, vlog.data()));
    log->assign(vlog.data(), logsize);
  }
  if (ret != NVRTC_SUCCESS) {
    return ret;
  }

  if (ptx) {
    size_t ptxsize;
    CHECK_NVRTC(nvrtcGetPTXSize(nvrtc_program, &ptxsize));
    std::vector<char> vptx(ptxsize);
    CHECK_NVRTC(nvrtcGetPTX(nvrtc_program, vptx.data()));
    ptx->assign(vptx.data(), ptxsize);
    if (should_remove_unused_globals) {
      detail::ptx_remove_unused_globals(ptx);
    }
  }

  if (!instantiation.empty() && mangled_instantiation) {
#if CUDA_VERSION >= 8000
    const char* mangled_instantiation_cstr;
    // Note: The returned string pointer becomes invalid after
    //         nvrtcDestroyProgram has been called, so we save it.
    CHECK_NVRTC(nvrtcGetLoweredName(nvrtc_program, instantiation.c_str(),
                                    &mangled_instantiation_cstr));
    *mangled_instantiation = mangled_instantiation_cstr;
#else
    // Extract mangled kernel template instantiation from PTX
    inst_dummy += " = ";  // Note: This must match how the PTX is generated
    int mi_beg = ptx->find(inst_dummy) + inst_dummy.size();
    int mi_end = ptx->find(";", mi_beg);
    *mangled_instantiation = ptx->substr(mi_beg, mi_end - mi_beg);
#endif
  }

#undef CHECK_NVRTC
  return NVRTC_SUCCESS;
}

inline void load_program(std::string const& cuda_source,
                         std::vector<std::string> const& headers,
                         file_callback_type file_callback,
                         std::vector<std::string>* include_paths,
                         std::map<std::string, std::string>* program_sources,
                         std::vector<std::string>* program_options,
                         std::string* program_name) {
  // Extract include paths from compile options
  std::vector<std::string>::iterator iter = program_options->begin();
  while (iter != program_options->end()) {
    std::string const& opt = *iter;
    if (opt.substr(0, 2) == "-I") {
      include_paths->push_back(opt.substr(2));
      iter = program_options->erase(iter);
    } else {
      ++iter;
    }
  }

  // Load program source
  if (!detail::load_source(cuda_source, *program_sources, "", *include_paths,
                           file_callback, program_name)) {
    throw std::runtime_error("Source not found: " + cuda_source);
  }

  // Maps header include names to their full file paths.
  std::map<std::string, std::string> header_fullpaths;

  // Load header sources
  for (std::string const& header : headers) {
    if (!detail::load_source(header, *program_sources, "", *include_paths,
                             file_callback, nullptr, &header_fullpaths)) {
      // **TODO: Deal with source not found
      throw std::runtime_error("Source not found: " + header);
    }
  }

#if JITIFY_PRINT_SOURCE
  std::string& program_source = (*program_sources)[*program_name];
  std::cout << "---------------------------------------" << std::endl;
  std::cout << "--- Source of " << *program_name << " ---" << std::endl;
  std::cout << "---------------------------------------" << std::endl;
  detail::print_with_line_numbers(program_source);
  std::cout << "---------------------------------------" << std::endl;
#endif

  std::vector<std::string> compiler_options, linker_files, linker_paths;
  detail::split_compiler_and_linker_options(*program_options, &compiler_options,
                                            &linker_files, &linker_paths);

  // If no arch is specified at this point we use whatever the current
  // context is. This ensures we pick up the correct internal headers
  // for arch-dependent compilation, e.g., some intrinsics are only
  // present for specific architectures.
  detail::detect_and_add_cuda_arch(compiler_options);
  detail::detect_and_add_cxx11_flag(compiler_options);

  // Iteratively try to compile the sources, and use the resulting errors to
  // identify missing headers.
  std::string log;
  nvrtcResult ret;
  while ((ret = detail::compile_kernel(*program_name, *program_sources,
                                       compiler_options, "", &log)) ==
         NVRTC_ERROR_COMPILATION) {
    std::string include_name;
    std::string include_parent;
    int line_num = 0;
    if (!detail::extract_include_info_from_compile_error(
            log, include_name, include_parent, line_num)) {
#if JITIFY_PRINT_LOG
      detail::print_compile_log(*program_name, log);
#endif
      // There was a non include-related compilation error
      // TODO: How to handle error?
      throw std::runtime_error("Runtime compilation failed");
    }

    bool is_included_with_quotes = false;
    if (program_sources->count(include_parent)) {
      const std::string& parent_source = (*program_sources)[include_parent];
      is_included_with_quotes =
          is_include_directive_with_quotes(parent_source, line_num);
    }

    // Try to load the new header
    // Note: This fullpath lookup is needed because the compiler error
    // messages have the include name of the header instead of its full path.
    std::string include_parent_fullpath = header_fullpaths[include_parent];
    std::string include_path = detail::path_base(include_parent_fullpath);
    if (detail::load_source(include_name, *program_sources, include_path,
                            *include_paths, file_callback, nullptr,
                            &header_fullpaths, is_included_with_quotes)) {
#if JITIFY_PRINT_HEADER_PATHS
      std::cout << "Found #include " << include_name << " from "
                << include_parent << ":" << line_num << " ["
                << include_parent_fullpath << "]"
                << " at:\n  " << header_fullpaths[include_name] << std::endl;
#endif
    } else {  // Failed to find header file.
      // Comment-out the include line and print a warning
      if (!program_sources->count(include_parent)) {
        // ***TODO: Unless there's another mechanism (e.g., potentially
        //            the parent path vs. filename problem), getting
        //            here means include_parent was found automatically
        //            in a system include path.
        //            We need a WAR to zap it from *its parent*.

        typedef std::map<std::string, std::string> source_map;
        for (source_map::const_iterator it = program_sources->begin();
             it != program_sources->end(); ++it) {
          std::cout << "  " << it->first << std::endl;
        }
        throw std::out_of_range(include_parent +
                                " not in loaded sources!"
                                " This may be due to a header being loaded by"
                                " NVRTC without Jitify's knowledge.");
      }
      std::string& parent_source = (*program_sources)[include_parent];
      parent_source = detail::comment_out_code_line(line_num, parent_source);
#if JITIFY_PRINT_LOG
      std::cout << include_parent << "(" << line_num
                << "): warning: " << include_name << ": [jitify] File not found"
                << std::endl;
#endif
    }
  }
  if (ret != NVRTC_SUCCESS) {
#if JITIFY_PRINT_LOG
    if (ret == NVRTC_ERROR_INVALID_OPTION) {
      std::cout << "Compiler options: ";
      for (int i = 0; i < (int)compiler_options.size(); ++i) {
        std::cout << compiler_options[i] << " ";
      }
      std::cout << std::endl;
    }
#endif
    throw std::runtime_error(std::string("NVRTC error: ") +
                             nvrtcGetErrorString(ret));
  }
}

inline void instantiate_kernel(
    std::string const& program_name,
    std::map<std::string, std::string> const& program_sources,
    std::string const& instantiation, std::vector<std::string> const& options,
    std::string* log, std::string* ptx, std::string* mangled_instantiation,
    std::vector<std::string>* linker_files,
    std::vector<std::string>* linker_paths) {
  std::vector<std::string> compiler_options;
  detail::split_compiler_and_linker_options(options, &compiler_options,
                                            linker_files, linker_paths);

  nvrtcResult ret =
      detail::compile_kernel(program_name, program_sources, compiler_options,
                             instantiation, log, ptx, mangled_instantiation);
#if JITIFY_PRINT_LOG
  if (log->size() > 1) {
    detail::print_compile_log(program_name, *log);
  }
#endif
  if (ret != NVRTC_SUCCESS) {
    throw std::runtime_error(std::string("NVRTC error: ") +
                             nvrtcGetErrorString(ret));
  }

#if JITIFY_PRINT_PTX
  std::cout << "---------------------------------------" << std::endl;
  std::cout << *mangled_instantiation << std::endl;
  std::cout << "---------------------------------------" << std::endl;
  std::cout << "--- PTX for " << mangled_instantiation << " in " << program_name
            << " ---" << std::endl;
  std::cout << "---------------------------------------" << std::endl;
  std::cout << *ptx << std::endl;
  std::cout << "---------------------------------------" << std::endl;
#endif
}

inline void get_1d_max_occupancy(CUfunction func,
                                 CUoccupancyB2DSize smem_callback,
                                 unsigned int* smem, int max_block_size,
                                 unsigned int flags, int* grid, int* block) {
  if (!func) {
    throw std::runtime_error(
        "Kernel pointer is NULL; you may need to define JITIFY_THREAD_SAFE "
        "1");
  }
  CUresult res = cuOccupancyMaxPotentialBlockSizeWithFlags(
      grid, block, func, smem_callback, *smem, max_block_size, flags);
  if (res != CUDA_SUCCESS) {
    const char* msg;
    cuGetErrorName(res, &msg);
    throw std::runtime_error(msg);
  }
  if (smem_callback) {
    *smem = (unsigned int)smem_callback(*block);
  }
}

}  // namespace detail

//! \endcond

class KernelInstantiation;
class Kernel;
class Program;
class JitCache;

struct ProgramConfig {
  std::vector<std::string> options;
  std::vector<std::string> include_paths;
  std::string name;
  typedef std::map<std::string, std::string> source_map;
  source_map sources;
};

class JitCache_impl {
  friend class Program_impl;
  friend class KernelInstantiation_impl;
  friend class KernelLauncher_impl;
  typedef uint64_t key_type;
  jitify::ObjectCache<key_type, detail::CUDAKernel> _kernel_cache;
  jitify::ObjectCache<key_type, ProgramConfig> _program_config_cache;
  std::vector<std::string> _options;
#if JITIFY_THREAD_SAFE
  std::mutex _kernel_cache_mutex;
  std::mutex _program_cache_mutex;
#endif
 public:
  inline JitCache_impl(size_t cache_size)
      : _kernel_cache(cache_size), _program_config_cache(cache_size) {
    detail::add_options_from_env(_options);

    // Bootstrap the cuda context to avoid errors
    cudaFree(0);
  }
};

class Program_impl {
  // A friendly class
  friend class Kernel_impl;
  friend class KernelLauncher_impl;
  friend class KernelInstantiation_impl;
 public:

  // TODO: This can become invalid if JitCache is destroyed before the
  //         Program object is. However, this can't happen if JitCache
  //           instances are static.
  JitCache_impl& _cache;
  uint64_t _hash;
  ProgramConfig* _config;
  void load_sources(std::string source, std::vector<std::string> headers,
                    std::vector<std::string> options,
                    file_callback_type file_callback);

  inline Program_impl(JitCache_impl& cache, std::string source,
                      jitify::detail::vector<std::string> headers = 0,
                      jitify::detail::vector<std::string> options = 0,
                      file_callback_type file_callback = 0);
  inline Program_impl(Program_impl const&) = default;
  inline Program_impl(Program_impl&&) = default;
  inline std::vector<std::string> const& options() const {
    return _config->options;
  }
  inline std::string const& name() const { return _config->name; }
  inline ProgramConfig::source_map const& sources() const {
    return _config->sources;
  }
  inline std::vector<std::string> const& include_paths() const {
    return _config->include_paths;
  }
};

class Kernel_impl {
  friend class KernelLauncher_impl;
  friend class KernelInstantiation_impl;
  Program_impl _program;
  std::string _name;
  std::vector<std::string> _options;
  uint64_t _hash;

 public:
  inline Kernel_impl(Program_impl const& program, std::string name,
                     jitify::detail::vector<std::string> options = 0);
  inline Kernel_impl(Kernel_impl const&) = default;
  inline Kernel_impl(Kernel_impl&&) = default;
};

class KernelInstantiation_impl {
  friend class KernelLauncher_impl;
  Kernel_impl _kernel;
  uint64_t _hash;
  std::string _template_inst;
  std::vector<std::string> _options;
  detail::CUDAKernel* _cuda_kernel;
  inline void print() const;
  void build_kernel();

 public:
  inline KernelInstantiation_impl(
      Kernel_impl const& kernel, std::vector<std::string> const& template_args);
  inline KernelInstantiation_impl(KernelInstantiation_impl const&) = default;
  inline KernelInstantiation_impl(KernelInstantiation_impl&&) = default;
  detail::CUDAKernel const& cuda_kernel() const { return *_cuda_kernel; }
};

class KernelLauncher_impl {
  KernelInstantiation_impl _kernel_inst;
  dim3 _grid;
  dim3 _block;
  unsigned int _smem;
  cudaStream_t _stream;

 public:
  inline KernelLauncher_impl(KernelInstantiation_impl const& kernel_inst,
                             dim3 grid, dim3 block, unsigned int smem = 0,
                             cudaStream_t stream = 0)
      : _kernel_inst(kernel_inst),
        _grid(grid),
        _block(block),
        _smem(smem),
        _stream(stream) {}
  inline KernelLauncher_impl(KernelLauncher_impl const&) = default;
  inline KernelLauncher_impl(KernelLauncher_impl&&) = default;
  inline CUresult launch(
      jitify::detail::vector<void*> arg_ptrs,
      jitify::detail::vector<std::string> arg_types = 0) const;
  inline void safe_launch(
      jitify::detail::vector<void*> arg_ptrs,
      jitify::detail::vector<std::string> arg_types = 0) const;

 private:
  inline void pre_launch(
      jitify::detail::vector<std::string> arg_types = 0) const;
};

/*! An object representing a configured and instantiated kernel ready
 *    for launching.
 */
class KernelLauncher {
  std::unique_ptr<KernelLauncher_impl const> _impl;

 public:
  KernelLauncher() = default;
  inline KernelLauncher(KernelInstantiation const& kernel_inst, dim3 grid,
                        dim3 block, unsigned int smem = 0,
                        cudaStream_t stream = 0);

  // Note: It's important that there is no implicit conversion required
  //         for arg_ptrs, because otherwise the parameter pack version
  //         below gets called instead (probably resulting in a segfault).
  /*! Launch the kernel.
   *
   *  \param arg_ptrs  A vector of pointers to each function argument for the
   *    kernel.
   *  \param arg_types A vector of function argument types represented
   *    as code-strings. This parameter is optional and is only used to print
   *    out the function signature.
   */
  inline CUresult launch(
      std::vector<void*> arg_ptrs = std::vector<void*>(),
      jitify::detail::vector<std::string> arg_types = 0) const {
    return _impl->launch(arg_ptrs, arg_types);
  }

  /*! Launch the kernel and check for cuda errors.
   *
   *  \see launch
   */
  inline void safe_launch(
      std::vector<void*> arg_ptrs = std::vector<void*>(),
      jitify::detail::vector<std::string> arg_types = 0) const {
    _impl->safe_launch(arg_ptrs, arg_types);
  }

  // Regular function call syntax
  /*! Launch the kernel.
   *
   *  \see launch
   */
  template <typename... ArgTypes>
  inline CUresult operator()(const ArgTypes&... args) const {
    return this->launch(args...);
  }
  /*! Launch the kernel.
   *
   *  \param args Function arguments for the kernel.
   */
  template <typename... ArgTypes>
  inline CUresult launch(const ArgTypes&... args) const {
    return this->launch(std::vector<void*>({(void*)&args...}),
                        {reflection::reflect<ArgTypes>()...});
  }
  /*! Launch the kernel and check for cuda errors.
   *
   *  \param args Function arguments for the kernel.
   */
  template <typename... ArgTypes>
  inline void safe_launch(const ArgTypes&... args) const {
    this->safe_launch(std::vector<void*>({(void*)&args...}),
                      {reflection::reflect<ArgTypes>()...});
  }
};

/*! An object representing a kernel instantiation made up of a Kernel and
 *    template arguments.
 */
class KernelInstantiation {
  friend class KernelLauncher;
  std::unique_ptr<KernelInstantiation_impl const> _impl;

 public:
  KernelInstantiation() = default;
  inline KernelInstantiation(Kernel const& kernel,
                             std::vector<std::string> const& template_args);

  /*! Implicit conversion to the underlying CUfunction object.
   *
   * \note This allows use of CUDA APIs like
   *   cuOccupancyMaxActiveBlocksPerMultiprocessor.
   */
  inline operator CUfunction() const { return _impl->cuda_kernel(); }

  /*! Configure the kernel launch.
   *
   *  \see configure
   */
  inline KernelLauncher operator()(dim3 grid, dim3 block, unsigned int smem = 0,
                                   cudaStream_t stream = 0) const {
    return this->configure(grid, block, smem, stream);
  }
  /*! Configure the kernel launch.
   *
   *  \param grid   The thread grid dimensions for the launch.
   *  \param block  The thread block dimensions for the launch.
   *  \param smem   The amount of shared memory to dynamically allocate, in
   * bytes.
   *  \param stream The CUDA stream to launch the kernel in.
   */
  inline KernelLauncher configure(dim3 grid, dim3 block, unsigned int smem = 0,
                                  cudaStream_t stream = 0) const {
    return KernelLauncher(*this, grid, block, smem, stream);
  }
  /*! Configure the kernel launch with a 1-dimensional block and grid chosen
   *  automatically to maximise occupancy.
   *
   * \param max_block_size  The upper limit on the block size, or 0 for no
   * limit.
   * \param smem  The amount of shared memory to dynamically allocate, in bytes.
   * \param smem_callback  A function returning smem for a given block size (overrides \p smem).
   * \param stream The CUDA stream to launch the kernel in.
   * \param flags The flags to pass to cuOccupancyMaxPotentialBlockSizeWithFlags.
   */
  inline KernelLauncher configure_1d_max_occupancy(
      int max_block_size = 0, unsigned int smem = 0,
      CUoccupancyB2DSize smem_callback = 0, cudaStream_t stream = 0,
      unsigned int flags = 0) const {
    int grid;
    int block;
    CUfunction func = _impl->cuda_kernel();
    detail::get_1d_max_occupancy(func, smem_callback, &smem, max_block_size,
                                 flags, &grid, &block);
    return this->configure(grid, block, smem, stream);
  }

  /*
   * Returns the function attribute requested from the kernel
   */
  inline int get_func_attribute(CUfunction_attribute attribute) const {
    return _impl->cuda_kernel().get_func_attribute(attribute);
  }

  /*
   * Set the function attribute requested for the kernel
   */
  inline void set_func_attribute(CUfunction_attribute attribute,
                                 int value) const {
    _impl->cuda_kernel().set_func_attribute(attribute, value);
  }

  /*
   * \deprecated Use \p get_global_ptr instead.
   */
  inline CUdeviceptr get_constant_ptr(const char* name,
                                      size_t* size = nullptr) const {
    return get_global_ptr(name, size);
  }

  /*
   * Get a device pointer to a global __constant__ or __device__ variable using
   * its un-mangled name. If provided, *size is set to the size of the variable
   * in bytes.
   */
  inline CUdeviceptr get_global_ptr(const char* name,
                                    size_t* size = nullptr) const {
    return _impl->cuda_kernel().get_global_ptr(name, size);
  }

  /*
   * Copy data from a global __constant__ or __device__ array to the host using
   * its un-mangled name.
   */
  template <typename T>
  inline CUresult get_global_array(const char* name, T* data, size_t count,
                                   CUstream stream = 0) const {
    return _impl->cuda_kernel().get_global_data(name, data, count, stream);
  }

  /*
   * Copy a value from a global __constant__ or __device__ variable to the host
   * using its un-mangled name.
   */
  template <typename T>
  inline CUresult get_global_value(const char* name, T* value,
                                   CUstream stream = 0) const {
    return get_global_array(name, value, 1, stream);
  }

  /*
   * Copy data from the host to a global __constant__ or __device__ array using
   * its un-mangled name.
   */
  template <typename T>
  inline CUresult set_global_array(const char* name, const T* data,
                                   size_t count, CUstream stream = 0) const {
    return _impl->cuda_kernel().set_global_data(name, data, count, stream);
  }

  /*
   * Copy a value from the host to a global __constant__ or __device__ variable
   * using its un-mangled name.
   */
  template <typename T>
  inline CUresult set_global_value(const char* name, const T& value,
                                   CUstream stream = 0) const {
    return set_global_array(name, &value, 1, stream);
  }

  const std::string& mangled_name() const {
    return _impl->cuda_kernel().function_name();
  }

  const std::string& ptx() const { return _impl->cuda_kernel().ptx(); }

  const std::vector<std::string>& link_files() const {
    return _impl->cuda_kernel().link_files();
  }

  const std::vector<std::string>& link_paths() const {
    return _impl->cuda_kernel().link_paths();
  }
};

/*! An object representing a kernel made up of a Program, a name and options.
 */
class Kernel {
  friend class KernelInstantiation;
  std::unique_ptr<Kernel_impl const> _impl;

 public:
  Kernel() = default;
  Kernel(Program const& program, std::string name,
         jitify::detail::vector<std::string> options = 0);

  /*! Instantiate the kernel.
   *
   *  \param template_args A vector of template arguments represented as
   *    code-strings. These can be generated using
   *    \code{.cpp}jitify::reflection::reflect<type>()\endcode or
   *    \code{.cpp}jitify::reflection::reflect(value)\endcode
   *
   *  \note Template type deduction is not possible, so all types must be
   *    explicitly specified.
   */
  // inline KernelInstantiation instantiate(std::vector<std::string> const&
  // template_args) const {
  inline KernelInstantiation instantiate(
      std::vector<std::string> const& template_args =
          std::vector<std::string>()) const {
    return KernelInstantiation(*this, template_args);
  }

  // Regular template instantiation syntax (note limited flexibility)
  /*! Instantiate the kernel.
   *
   *  \note The template arguments specified on this function are
   *    used to instantiate the kernel. Non-type template arguments must
   *    be wrapped with
   *    \code{.cpp}jitify::reflection::NonType<type,value>\endcode
   *
   *  \note Template type deduction is not possible, so all types must be
   *    explicitly specified.
   */
  template <typename... TemplateArgs>
  inline KernelInstantiation instantiate() const {
    return this->instantiate(
        std::vector<std::string>({reflection::reflect<TemplateArgs>()...}));
  }
  // Template-like instantiation syntax
  //   E.g., instantiate(myvar,Type<MyType>())(grid,block)
  /*! Instantiate the kernel.
   *
   *  \param targs The template arguments for the kernel, represented as
   *    values. Types must be wrapped with
   *    \code{.cpp}jitify::reflection::Type<type>()\endcode or
   *    \code{.cpp}jitify::reflection::type_of(value)\endcode
   *
   *  \note Template type deduction is not possible, so all types must be
   *    explicitly specified.
   */
  template <typename... TemplateArgs>
  inline KernelInstantiation instantiate(TemplateArgs... targs) const {
    return this->instantiate(
        std::vector<std::string>({reflection::reflect(targs)...}));
  }
};

/*! An object representing a program made up of source code, headers
 *    and options.
 */
class Program {
  friend class Kernel;
 public:
  std::unique_ptr<Program_impl const> _impl;

  Program() = default;
  Program(JitCache& cache, std::string source,
          jitify::detail::vector<std::string> headers = 0,
          jitify::detail::vector<std::string> options = 0,
          file_callback_type file_callback = 0);

  /*! Select a kernel.
   *
   * \param name The name of the kernel (unmangled and without
   * template arguments).
   * \param options A vector of options to be passed to the NVRTC
   * compiler when compiling this kernel.
   */
  inline Kernel kernel(std::string name,
                       jitify::detail::vector<std::string> options = 0) const {
    return Kernel(*this, name, options);
  }
  /*! Select a kernel.
   *
   *  \see kernel
   */
  inline Kernel operator()(
      std::string name, jitify::detail::vector<std::string> options = 0) const {
    return this->kernel(name, options);
  }
};

/*! An object that manages a cache of JIT-compiled CUDA kernels.
 *
 */
class JitCache {
  friend class Program;
  std::unique_ptr<JitCache_impl> _impl;

 public:
  /*! JitCache constructor.
   *  \param cache_size The number of kernels to hold in the cache
   *    before overwriting the least-recently-used ones.
   */
  enum { DEFAULT_CACHE_SIZE = 128 };
  JitCache(size_t cache_size = DEFAULT_CACHE_SIZE)
      : _impl(new JitCache_impl(cache_size)) {}

  /*! Create a program.
   *
   *  \param source A string containing either the source filename or
   *    the source itself; in the latter case, the first line must be
   *    the name of the program.
   *  \param headers A vector of strings representing the source of
   *    each header file required by the program. Each entry can be
   *    either the header filename or the header source itself; in
   *    the latter case, the first line must be the name of the header
   *    (i.e., the name by which the header is #included).
   *  \param options A vector of options to be passed to the
   *    NVRTC compiler. Include paths specified with \p -I
   *    are added to the search paths used by Jitify. The environment
   *    variable JITIFY_OPTIONS can also be used to define additional
   *    options.
   *  \param file_callback A pointer to a callback function that is
   *    invoked whenever a source file needs to be loaded. Inside this
   *    function, the user can either load/specify the source themselves
   *    or defer to Jitify's file-loading mechanisms.
   *  \note Program or header source files referenced by filename are
   *  looked-up using the following mechanisms (in this order):
   *  \note 1) By calling file_callback.
   *  \note 2) By looking for the file embedded in the executable via the GCC
   * linker.
   *  \note 3) By looking for the file in the filesystem.
   *
   *  \note Jitify recursively scans all source files for \p #include
   *  directives and automatically adds them to the set of headers needed
   *  by the program.
   *  If a \p #include directive references a header that cannot be found,
   *  the directive is automatically removed from the source code to prevent
   *  immediate compilation failure. This may result in compilation errors
   *  if the header was required by the program.
   *
   *  \note Jitify automatically includes NVRTC-safe versions of some
   *  standard library headers.
   */
  inline Program program(std::string source,
                         jitify::detail::vector<std::string> headers = 0,
                         jitify::detail::vector<std::string> options = 0,
                         file_callback_type file_callback = 0) {
    return Program(*this, source, headers, options, file_callback);
  }
};

inline Program::Program(JitCache& cache, std::string source,
                        jitify::detail::vector<std::string> headers,
                        jitify::detail::vector<std::string> options,
                        file_callback_type file_callback)
    : _impl(new Program_impl(*cache._impl, source, headers, options,
                             file_callback)) {}

inline Kernel::Kernel(Program const& program, std::string name,
                      jitify::detail::vector<std::string> options)
    : _impl(new Kernel_impl(*program._impl, name, options)) {}

inline KernelInstantiation::KernelInstantiation(
    Kernel const& kernel, std::vector<std::string> const& template_args)
    : _impl(new KernelInstantiation_impl(*kernel._impl, template_args)) {}

inline KernelLauncher::KernelLauncher(KernelInstantiation const& kernel_inst,
                                      dim3 grid, dim3 block, unsigned int smem,
                                      cudaStream_t stream)
    : _impl(new KernelLauncher_impl(*kernel_inst._impl, grid, block, smem,
                                    stream)) {}

inline std::ostream& operator<<(std::ostream& stream, dim3 d) {
  if (d.y == 1 && d.z == 1) {
    stream << d.x;
  } else {
    stream << "(" << d.x << "," << d.y << "," << d.z << ")";
  }
  return stream;
}

inline void KernelLauncher_impl::pre_launch(
    jitify::detail::vector<std::string> arg_types) const {
  (void)arg_types;
#if JITIFY_PRINT_LAUNCH
  Kernel_impl const& kernel = _kernel_inst._kernel;
  std::string arg_types_string =
      (arg_types.empty() ? "..." : reflection::reflect_list(arg_types));
  std::cout << "Launching " << kernel._name << _kernel_inst._template_inst
            << "<<<" << _grid << "," << _block << "," << _smem << "," << _stream
            << ">>>"
            << "(" << arg_types_string << ")" << std::endl;
#endif
  if (!_kernel_inst._cuda_kernel) {
    throw std::runtime_error(
        "Kernel pointer is NULL; you may need to define JITIFY_THREAD_SAFE 1");
  }
}

inline CUresult KernelLauncher_impl::launch(
    jitify::detail::vector<void*> arg_ptrs,
    jitify::detail::vector<std::string> arg_types) const {
  pre_launch(arg_types);
  return _kernel_inst._cuda_kernel->launch(_grid, _block, _smem, _stream,
                                           arg_ptrs);
}

inline void KernelLauncher_impl::safe_launch(
    jitify::detail::vector<void*> arg_ptrs,
    jitify::detail::vector<std::string> arg_types) const {
  pre_launch(arg_types);
  _kernel_inst._cuda_kernel->safe_launch(_grid, _block, _smem, _stream,
                                         arg_ptrs);
}

inline KernelInstantiation_impl::KernelInstantiation_impl(
    Kernel_impl const& kernel, std::vector<std::string> const& template_args)
    : _kernel(kernel), _options(kernel._options) {
  _template_inst =
      (template_args.empty() ? ""
                             : reflection::reflect_template(template_args));
  using detail::hash_combine;
  using detail::hash_larson64;
  _hash = _kernel._hash;
  _hash = hash_combine(_hash, hash_larson64(_template_inst.c_str()));
  JitCache_impl& cache = _kernel._program._cache;
  uint64_t cache_key = _hash;
#if JITIFY_THREAD_SAFE
  std::lock_guard<std::mutex> lock(cache._kernel_cache_mutex);
#endif
  if (cache._kernel_cache.contains(cache_key)) {
#if JITIFY_PRINT_INSTANTIATION
    std::cout << "Found ";
    this->print();
#endif
    _cuda_kernel = &cache._kernel_cache.get(cache_key);
  } else {
#if JITIFY_PRINT_INSTANTIATION
    std::cout << "Building ";
    this->print();
#endif
    _cuda_kernel = &cache._kernel_cache.emplace(cache_key);
    this->build_kernel();
  }
}

inline void KernelInstantiation_impl::print() const {
  std::string options_string = reflection::reflect_list(_options);
  std::cout << _kernel._name << _template_inst << " [" << options_string << "]"
            << std::endl;
}

inline void KernelInstantiation_impl::build_kernel() {
  Program_impl const& program = _kernel._program;

  std::string instantiation = _kernel._name + _template_inst;

  std::string log, ptx, mangled_instantiation;
  std::vector<std::string> linker_files, linker_paths;
  detail::instantiate_kernel(program.name(), program.sources(), instantiation,
                             _options, &log, &ptx, &mangled_instantiation,
                             &linker_files, &linker_paths);

  _cuda_kernel->set(mangled_instantiation.c_str(), ptx.c_str(), linker_files,
                    linker_paths);
}

Kernel_impl::Kernel_impl(Program_impl const& program, std::string name,
                         jitify::detail::vector<std::string> options)
    : _program(program), _name(name), _options(options) {
  // Merge options from parent
  _options.insert(_options.end(), _program.options().begin(),
                  _program.options().end());
  detail::detect_and_add_cuda_arch(_options);
  detail::detect_and_add_cxx11_flag(_options);
  std::string options_string = reflection::reflect_list(_options);
  using detail::hash_combine;
  using detail::hash_larson64;
  _hash = _program._hash;
  _hash = hash_combine(_hash, hash_larson64(_name.c_str()));
  _hash = hash_combine(_hash, hash_larson64(options_string.c_str()));
}

Program_impl::Program_impl(JitCache_impl& cache, std::string source,
                           jitify::detail::vector<std::string> headers,
                           jitify::detail::vector<std::string> options,
                           file_callback_type file_callback)
    : _cache(cache) {
  // Compute hash of source, headers and options
  std::string options_string = reflection::reflect_list(options);
  using detail::hash_combine;
  using detail::hash_larson64;
  _hash = hash_combine(hash_larson64(source.c_str()),
                       hash_larson64(options_string.c_str()));
  for (size_t i = 0; i < headers.size(); ++i) {
    _hash = hash_combine(_hash, hash_larson64(headers[i].c_str()));
  }
  _hash = hash_combine(_hash, (uint64_t)file_callback);
  // Add pre-include built-in JIT-safe headers
  for (int i = 0; i < detail::preinclude_jitsafe_headers_count; ++i) {
    const char* hdr_name = detail::preinclude_jitsafe_header_names[i];
    const std::string& hdr_source =
        detail::get_jitsafe_headers_map().at(hdr_name);
    headers.push_back(std::string(hdr_name) + "\n" + hdr_source);
  }
  // Merge options from parent
  options.insert(options.end(), _cache._options.begin(), _cache._options.end());
  // Load sources
#if JITIFY_THREAD_SAFE
  std::lock_guard<std::mutex> lock(cache._program_cache_mutex);
#endif
  if (!cache._program_config_cache.contains(_hash)) {
    _config = &cache._program_config_cache.insert(_hash);
    this->load_sources(source, headers, options, file_callback);
  } else {
    _config = &cache._program_config_cache.get(_hash);
  }
}

inline void Program_impl::load_sources(std::string source,
                                       std::vector<std::string> headers,
                                       std::vector<std::string> options,
                                       file_callback_type file_callback) {
  _config->options = options;
  detail::load_program(source, headers, file_callback, &_config->include_paths,
                       &_config->sources, &_config->options, &_config->name);
}

enum Location { HOST, DEVICE };

/*! Specifies location and parameters for execution of an algorithm.
 *  \param stream        The CUDA stream on which to execute.
 *  \param headers       A vector of headers to include in the code.
 *  \param options       Options to pass to the NVRTC compiler.
 *  \param file_callback See jitify::Program.
 *  \param block_size    The size of the CUDA thread block with which to
 * execute.
 *  \param cache_size    The number of kernels to store in the cache
 * before overwriting the least-recently-used ones.
 */
struct ExecutionPolicy {
  /*! Location (HOST or DEVICE) on which to execute.*/
  Location location;
  /*! List of headers to include when compiling the algorithm.*/
  std::vector<std::string> headers;
  /*! List of compiler options.*/
  std::vector<std::string> options;
  /*! Optional callback for loading source files.*/
  file_callback_type file_callback;
  /*! CUDA stream on which to execute.*/
  cudaStream_t stream;
  /*! CUDA device on which to execute.*/
  int device;
  /*! CUDA block size with which to execute.*/
  int block_size;
  /*! The number of instantiations to store in the cache before overwriting
   *  the least-recently-used ones.*/
  size_t cache_size;
  ExecutionPolicy(Location location_ = DEVICE,
                  jitify::detail::vector<std::string> headers_ = 0,
                  jitify::detail::vector<std::string> options_ = 0,
                  file_callback_type file_callback_ = 0,
                  cudaStream_t stream_ = 0, int device_ = 0,
                  int block_size_ = 256,
                  size_t cache_size_ = JitCache::DEFAULT_CACHE_SIZE)
      : location(location_),
        headers(headers_),
        options(options_),
        file_callback(file_callback_),
        stream(stream_),
        device(device_),
        block_size(block_size_),
        cache_size(cache_size_) {}
};

template <class Func>
class Lambda;

/*! An object that captures a set of variables for use in a parallel_for
 *    expression. See JITIFY_CAPTURE().
 */
class Capture {
 public:
  std::vector<std::string> _arg_decls;
  std::vector<void*> _arg_ptrs;

 public:
  template <typename... Args>
  inline Capture(std::vector<std::string> arg_names, Args const&... args)
      : _arg_ptrs{(void*)&args...} {
    std::vector<std::string> arg_types = {reflection::reflect<Args>()...};
    _arg_decls.resize(arg_names.size());
    for (int i = 0; i < (int)arg_names.size(); ++i) {
      _arg_decls[i] = arg_types[i] + " " + arg_names[i];
    }
  }
};

/*! An object that captures the instantiated Lambda function for use
    in a parallel_for expression and the function string for NVRTC
    compilation
 */
template <class Func>
class Lambda {
 public:
  Capture _capture;
  std::string _func_string;
  Func _func;

 public:
  inline Lambda(Capture const& capture, std::string func_string, Func func)
      : _capture(capture), _func_string(func_string), _func(func) {}
};

template <typename T>
inline Lambda<T> make_Lambda(Capture const& capture, std::string func,
                             T lambda) {
  return Lambda<T>(capture, func, lambda);
}

#define JITIFY_CAPTURE(...)                                            \
  jitify::Capture(jitify::detail::split_string(#__VA_ARGS__, -1, ","), \
                  __VA_ARGS__)

#define JITIFY_MAKE_LAMBDA(capture, x, ...)               \
  jitify::make_Lambda(capture, std::string(#__VA_ARGS__), \
                      [x](int i) { __VA_ARGS__; })

#define JITIFY_ARGS(...) __VA_ARGS__

#define JITIFY_LAMBDA_(x, ...) \
  JITIFY_MAKE_LAMBDA(JITIFY_CAPTURE(x), JITIFY_ARGS(x), __VA_ARGS__)

// macro sequence to strip surrounding brackets
#define JITIFY_STRIP_PARENS(X) X
#define JITIFY_PASS_PARAMETERS(X) JITIFY_STRIP_PARENS(JITIFY_ARGS X)

/*! Creates a Lambda object with captured variables and a function
 *    definition.
 *  \param capture A bracket-enclosed list of variables to capture.
 *  \param ...     The function definition.
 *
 *  \code{.cpp}
 *  float* capture_me;
 *  int    capture_me_too;
 *  auto my_lambda = JITIFY_LAMBDA( (capture_me, capture_me_too),
 *                                  capture_me[i] = i*capture_me_too );
 *  \endcode
 */
#define JITIFY_LAMBDA(capture, ...)                            \
  JITIFY_LAMBDA_(JITIFY_ARGS(JITIFY_PASS_PARAMETERS(capture)), \
                 JITIFY_ARGS(__VA_ARGS__))

// TODO: Try to implement for_each that accepts iterators instead of indices
//       Add compile guard for NOCUDA compilation
/*! Call a function for a range of indices
 *
 *  \param policy Determines the location and device parameters for
 *  execution of the parallel_for.
 *  \param begin  The starting index.
 *  \param end    The ending index.
 *  \param lambda A Lambda object created using the JITIFY_LAMBDA() macro.
 *
 *  \code{.cpp}
 *  char const* in;
 *  float*      out;
 *  parallel_for(0, 100, JITIFY_LAMBDA( (in, out), {char x = in[i]; out[i] =
 * x*x; } ); \endcode
 */
template <typename IndexType, class Func>
CUresult parallel_for(ExecutionPolicy policy, IndexType begin, IndexType end,
                      Lambda<Func> const& lambda) {
  using namespace jitify;

  if (policy.location == HOST) {
#ifdef _OPENMP
#pragma omp parallel for
#endif
    for (IndexType i = begin; i < end; i++) {
      lambda._func(i);
    }
    return CUDA_SUCCESS;  // FIXME - replace with non-CUDA enum type?
  }

  thread_local static JitCache kernel_cache(policy.cache_size);

  std::vector<std::string> arg_decls;
  arg_decls.push_back("I begin, I end");
  arg_decls.insert(arg_decls.end(), lambda._capture._arg_decls.begin(),
                   lambda._capture._arg_decls.end());

  std::stringstream source_ss;
  source_ss << "parallel_for_program\n";
  for (auto const& header : policy.headers) {
    std::string header_name = header.substr(0, header.find("\n"));
    source_ss << "#include <" << header_name << ">\n";
  }
  source_ss << "template<typename I>\n"
               "__global__\n"
               "void parallel_for_kernel("
            << reflection::reflect_list(arg_decls)
            << ") {\n"
               "	I i0 = threadIdx.x + blockDim.x*blockIdx.x;\n"
               "	for( I i=i0+begin; i<end; i+=blockDim.x*gridDim.x ) {\n"
               "	"
            << "\t" << lambda._func_string << ";\n"
            << "	}\n"
               "}\n";

  Program program = kernel_cache.program(source_ss.str(), policy.headers,
                                         policy.options, policy.file_callback);

  std::vector<void*> arg_ptrs;
  arg_ptrs.push_back(&begin);
  arg_ptrs.push_back(&end);
  arg_ptrs.insert(arg_ptrs.end(), lambda._capture._arg_ptrs.begin(),
                  lambda._capture._arg_ptrs.end());

  size_t n = end - begin;
  dim3 block(policy.block_size);
  dim3 grid((unsigned int)std::min((n - 1) / block.x + 1, size_t(65535)));
  cudaSetDevice(policy.device);
  return program.kernel("parallel_for_kernel")
      .instantiate<IndexType>()
      .configure(grid, block, 0, policy.stream)
      .launch(arg_ptrs);
}

namespace experimental {

using jitify::file_callback_type;

namespace serialization {

namespace detail {

// This should be incremented whenever the serialization format changes in any
// incompatible way.
static constexpr const size_t kSerializationVersion = 2;

inline void serialize(std::ostream& stream, size_t u) {
  uint64_t u64 = u;
  char bytes[8];
  for (int i = 0; i < (int)sizeof(bytes); ++i) {
    // Convert to little-endian bytes.
    bytes[i] = (unsigned char)(u64 >> (i * CHAR_BIT));
  }
  stream.write(bytes, sizeof(bytes));
}

inline bool deserialize(std::istream& stream, size_t* size) {
  char bytes[8];
  stream.read(bytes, sizeof(bytes));
  uint64_t u64 = 0;
  for (int i = 0; i < (int)sizeof(bytes); ++i) {
    // Convert from little-endian bytes.
    u64 |= uint64_t((unsigned char)(bytes[i])) << (i * CHAR_BIT);
  }
  *size = u64;
  return stream.good();
}

inline void serialize(std::ostream& stream, std::string const& s) {
  serialize(stream, s.size());
  stream.write(s.data(), s.size());
}

inline bool deserialize(std::istream& stream, std::string* s) {
  size_t size;
  if (!deserialize(stream, &size)) return false;
  s->resize(size);
  if (s->size()) {
    stream.read(&(*s)[0], s->size());
  }
  return stream.good();
}

inline void serialize(std::ostream& stream, std::vector<std::string> const& v) {
  serialize(stream, v.size());
  for (auto const& s : v) {
    serialize(stream, s);
  }
}

inline bool deserialize(std::istream& stream, std::vector<std::string>* v) {
  size_t size;
  if (!deserialize(stream, &size)) return false;
  v->resize(size);
  for (auto& s : *v) {
    if (!deserialize(stream, &s)) return false;
  }
  return true;
}

inline void serialize(std::ostream& stream,
                      std::map<std::string, std::string> const& m) {
  serialize(stream, m.size());
  for (auto const& kv : m) {
    serialize(stream, kv.first);
    serialize(stream, kv.second);
  }
}

inline bool deserialize(std::istream& stream,
                        std::map<std::string, std::string>* m) {
  size_t size;
  if (!deserialize(stream, &size)) return false;
  for (size_t i = 0; i < size; ++i) {
    std::string key;
    if (!deserialize(stream, &key)) return false;
    if (!deserialize(stream, &(*m)[key])) return false;
  }
  return true;
}

template <typename T, typename... Rest>
inline void serialize(std::ostream& stream, T const& value, Rest... rest) {
  serialize(stream, value);
  serialize(stream, rest...);
}

template <typename T, typename... Rest>
inline bool deserialize(std::istream& stream, T* value, Rest... rest) {
  if (!deserialize(stream, value)) return false;
  return deserialize(stream, rest...);
}

inline void serialize_magic_number(std::ostream& stream) {
  stream.write("JTFY", 4);
  serialize(stream, kSerializationVersion);
}

inline bool deserialize_magic_number(std::istream& stream) {
  char magic_number[4] = {0, 0, 0, 0};
  stream.read(&magic_number[0], 4);
  if (!(magic_number[0] == 'J' && magic_number[1] == 'T' &&
        magic_number[2] == 'F' && magic_number[3] == 'Y')) {
    return false;
  }
  size_t serialization_version;
  if (!deserialize(stream, &serialization_version)) return false;
  return serialization_version == kSerializationVersion;
}

}  // namespace detail

template <typename... Values>
inline std::string serialize(Values const&... values) {
  std::ostringstream ss(std::stringstream::out | std::stringstream::binary);
  detail::serialize_magic_number(ss);
  detail::serialize(ss, values...);
  return ss.str();
}

template <typename... Values>
inline bool deserialize(std::string const& serialized, Values*... values) {
  std::istringstream ss(serialized,
                        std::stringstream::in | std::stringstream::binary);
  if (!detail::deserialize_magic_number(ss)) return false;
  return detail::deserialize(ss, values...);
}

}  // namespace serialization

class Program;
class Kernel;
class KernelInstantiation;
class KernelLauncher;

/*! An object representing a program made up of source code, headers
 *    and options.
 */
class Program {
 public:
  friend class KernelInstantiation;
  std::string _name;
  std::vector<std::string> _options;
  std::map<std::string, std::string> _sources;

private:
  // Private constructor used by deserialize()
  Program() {}

 public:
  /*! Create a program.
   *
   *  \param source A string containing either the source filename or
   *    the source itself; in the latter case, the first line must be
   *    the name of the program.
   *  \param headers A vector of strings representing the source of
   *    each header file required by the program. Each entry can be
   *    either the header filename or the header source itself; in
   *    the latter case, the first line must be the name of the header
   *    (i.e., the name by which the header is #included).
   *  \param options A vector of options to be passed to the
   *    NVRTC compiler. Include paths specified with \p -I
   *    are added to the search paths used by Jitify. The environment
   *    variable JITIFY_OPTIONS can also be used to define additional
   *    options.
   *  \param file_callback A pointer to a callback function that is
   *    invoked whenever a source file needs to be loaded. Inside this
   *    function, the user can either load/specify the source themselves
   *    or defer to Jitify's file-loading mechanisms.
   *  \note Program or header source files referenced by filename are
   *  looked-up using the following mechanisms (in this order):
   *  \note 1) By calling file_callback.
   *  \note 2) By looking for the file embedded in the executable via the GCC
   * linker.
   *  \note 3) By looking for the file in the filesystem.
   *
   *  \note Jitify recursively scans all source files for \p #include
   *  directives and automatically adds them to the set of headers needed
   *  by the program.
   *  If a \p #include directive references a header that cannot be found,
   *  the directive is automatically removed from the source code to prevent
   *  immediate compilation failure. This may result in compilation errors
   *  if the header was required by the program.
   *
   *  \note Jitify automatically includes NVRTC-safe versions of some
   *  standard library headers.
   */
  Program(std::string const& cuda_source,
          std::vector<std::string> const& given_headers = {},
          std::vector<std::string> const& given_options = {},
          file_callback_type file_callback = nullptr) {
    // Add pre-include built-in JIT-safe headers
    std::vector<std::string> headers = given_headers;
    for (int i = 0; i < detail::preinclude_jitsafe_headers_count; ++i) {
      const char* hdr_name = detail::preinclude_jitsafe_header_names[i];
      const std::string& hdr_source =
          detail::get_jitsafe_headers_map().at(hdr_name);
      headers.push_back(std::string(hdr_name) + "\n" + hdr_source);
    }

    _options = given_options;
    detail::add_options_from_env(_options);
    std::vector<std::string> include_paths;
    detail::load_program(cuda_source, headers, file_callback, &include_paths,
                         &_sources, &_options, &_name);
  }

  /*! Restore a serialized program.
   *
   * \param serialized_program The serialized program to restore.
   *
   * \see serialize
   */
  static Program deserialize(std::string const& serialized_program) {
    Program program;
    if (!serialization::deserialize(serialized_program, &program._name,
                                    &program._options, &program._sources)) {
      throw std::runtime_error("Failed to deserialize program");
    }
    return program;
  }

  /*! Save the program.
   *
   * \see deserialize
   */
  std::string serialize() const {
    // Note: Must update kSerializationVersion if this is changed.
    return serialization::serialize(_name, _options, _sources);
  };

  /*! Select a kernel.
   *
   * \param name The name of the kernel (unmangled and without
   * template arguments).
   * \param options A vector of options to be passed to the NVRTC
   * compiler when compiling this kernel.
   */
  Kernel kernel(std::string const& name,
                std::vector<std::string> const& options = {}) const;
};

class Kernel {
  friend class KernelInstantiation;
  Program const* _program;
  std::string _name;
  std::vector<std::string> _options;

 public:
  Kernel(Program const* program, std::string const& name,
         std::vector<std::string> const& options = {})
      : _program(program), _name(name), _options(options) {}

  /*! Instantiate the kernel.
   *
   *  \param template_args A vector of template arguments represented as
   *    code-strings. These can be generated using
   *    \code{.cpp}jitify::reflection::reflect<type>()\endcode or
   *    \code{.cpp}jitify::reflection::reflect(value)\endcode
   *
   *  \note Template type deduction is not possible, so all types must be
   *    explicitly specified.
   */
  KernelInstantiation instantiate(
      std::vector<std::string> const& template_args =
          std::vector<std::string>()) const;

  // Regular template instantiation syntax (note limited flexibility)
  /*! Instantiate the kernel.
   *
   *  \note The template arguments specified on this function are
   *    used to instantiate the kernel. Non-type template arguments must
   *    be wrapped with
   *    \code{.cpp}jitify::reflection::NonType<type,value>\endcode
   *
   *  \note Template type deduction is not possible, so all types must be
   *    explicitly specified.
   */
  template <typename... TemplateArgs>
  KernelInstantiation instantiate() const;

  // Template-like instantiation syntax
  //   E.g., instantiate(myvar,Type<MyType>())(grid,block)
  /*! Instantiate the kernel.
   *
   *  \param targs The template arguments for the kernel, represented as
   *    values. Types must be wrapped with
   *    \code{.cpp}jitify::reflection::Type<type>()\endcode or
   *    \code{.cpp}jitify::reflection::type_of(value)\endcode
   *
   *  \note Template type deduction is not possible, so all types must be
   *    explicitly specified.
   */
  template <typename... TemplateArgs>
  KernelInstantiation instantiate(TemplateArgs... targs) const;
};

class KernelInstantiation {
  friend class KernelLauncher;
  std::unique_ptr<detail::CUDAKernel> _cuda_kernel;

  // Private constructor used by deserialize()
  KernelInstantiation(std::string const& func_name, std::string const& ptx,
                      std::vector<std::string> const& link_files,
                      std::vector<std::string> const& link_paths)
      : _cuda_kernel(new detail::CUDAKernel(func_name.c_str(), ptx.c_str(),
                                            link_files, link_paths)) {}

 public:
  KernelInstantiation(Kernel const& kernel,
                      std::vector<std::string> const& template_args) {
    Program const* program = kernel._program;

    std::string template_inst =
        (template_args.empty() ? ""
                               : reflection::reflect_template(template_args));
    std::string instantiation = kernel._name + template_inst;

    std::vector<std::string> options;
    options.insert(options.begin(), program->_options.begin(),
                   program->_options.end());
    options.insert(options.begin(), kernel._options.begin(),
                   kernel._options.end());
    detail::detect_and_add_cuda_arch(options);
    detail::detect_and_add_cxx11_flag(options);

    std::string log, ptx, mangled_instantiation;
    std::vector<std::string> linker_files, linker_paths;
    detail::instantiate_kernel(program->_name, program->_sources, instantiation,
                               options, &log, &ptx, &mangled_instantiation,
                               &linker_files, &linker_paths);

    _cuda_kernel.reset(new detail::CUDAKernel(mangled_instantiation.c_str(),
                                              ptx.c_str(), linker_files,
                                              linker_paths));
  }

  /*! Implicit conversion to the underlying CUfunction object.
   *
   * \note This allows use of CUDA APIs like
   *   cuOccupancyMaxActiveBlocksPerMultiprocessor.
   */
  operator CUfunction() const { return *_cuda_kernel; }

  /*! Restore a serialized kernel instantiation.
   *
   * \param serialized_kernel_inst The serialized kernel instantiation to
   * restore.
   *
   * \see serialize
   */
  static KernelInstantiation deserialize(
      std::string const& serialized_kernel_inst) {
    std::string func_name, ptx;
    std::vector<std::string> link_files, link_paths;
    if (!serialization::deserialize(serialized_kernel_inst, &func_name, &ptx,
                                    &link_files, &link_paths)) {
      throw std::runtime_error("Failed to deserialize kernel instantiation");
    }
    return KernelInstantiation(func_name, ptx, link_files, link_paths);
  }

  /*! Save the program.
   *
   * \see deserialize
   */
  std::string serialize() const {
    // Note: Must update kSerializationVersion if this is changed.
    return serialization::serialize(
        _cuda_kernel->function_name(), _cuda_kernel->ptx(),
        _cuda_kernel->link_files(), _cuda_kernel->link_paths());
  }

  /*! Configure the kernel launch.
   *
   *  \param grid   The thread grid dimensions for the launch.
   *  \param block  The thread block dimensions for the launch.
   *  \param smem   The amount of shared memory to dynamically allocate, in
   * bytes.
   *  \param stream The CUDA stream to launch the kernel in.
   */
  KernelLauncher configure(dim3 grid, dim3 block, unsigned int smem = 0,
                           cudaStream_t stream = 0) const;

  /*! Configure the kernel launch with a 1-dimensional block and grid chosen
   *  automatically to maximise occupancy.
   *
   * \param max_block_size  The upper limit on the block size, or 0 for no
   * limit.
   * \param smem  The amount of shared memory to dynamically allocate, in bytes.
   * \param smem_callback  A function returning smem for a given block size
   * (overrides \p smem).
   * \param stream The CUDA stream to launch the kernel in.
   * \param flags The flags to pass to
   * cuOccupancyMaxPotentialBlockSizeWithFlags.
   */
  KernelLauncher configure_1d_max_occupancy(
      int max_block_size = 0, unsigned int smem = 0,
      CUoccupancyB2DSize smem_callback = 0, cudaStream_t stream = 0,
      unsigned int flags = 0) const;

  /*
   * Returns the function attribute requested from the kernel
   */
  inline int get_func_attribute(CUfunction_attribute attribute) const {
    return _cuda_kernel->get_func_attribute(attribute);
  }

  /*
   * Set the function attribute requested for the kernel
   */
  inline void set_func_attribute(CUfunction_attribute attribute,
                                 int value) const {
    _cuda_kernel->set_func_attribute(attribute, value);
  }

  /*
   * \deprecated Use \p get_global_ptr instead.
   */
  CUdeviceptr get_constant_ptr(const char* name, size_t* size = nullptr) const {
    return get_global_ptr(name, size);
  }

  /*
   * Get a device pointer to a global __constant__ or __device__ variable using
   * its un-mangled name. If provided, *size is set to the size of the variable
   * in bytes.
   */
  CUdeviceptr get_global_ptr(const char* name, size_t* size = nullptr) const {
    return _cuda_kernel->get_global_ptr(name, size);
  }

  /*
   * Copy data from a global __constant__ or __device__ array to the host using
   * its un-mangled name.
   */
  template <typename T>
  CUresult get_global_array(const char* name, T* data, size_t count,
                            CUstream stream = 0) const {
    return _cuda_kernel->get_global_data(name, data, count, stream);
  }

  /*
   * Copy a value from a global __constant__ or __device__ variable to the host
   * using its un-mangled name.
   */
  template <typename T>
  CUresult get_global_value(const char* name, T* value,
                            CUstream stream = 0) const {
    return get_global_array(name, value, 1, stream);
  }

  /*
   * Copy data from the host to a global __constant__ or __device__ array using
   * its un-mangled name.
   */
  template <typename T>
  CUresult set_global_array(const char* name, const T* data, size_t count,
                            CUstream stream = 0) const {
    return _cuda_kernel->set_global_data(name, data, count, stream);
  }

  /*
   * Copy a value from the host to a global __constant__ or __device__ variable
   * using its un-mangled name.
   */
  template <typename T>
  CUresult set_global_value(const char* name, const T& value,
                            CUstream stream = 0) const {
    return set_global_array(name, &value, 1, stream);
  }

  const std::string& mangled_name() const {
    return _cuda_kernel->function_name();
  }

  const std::string& ptx() const { return _cuda_kernel->ptx(); }

  const std::vector<std::string>& link_files() const {
    return _cuda_kernel->link_files();
  }

  const std::vector<std::string>& link_paths() const {
    return _cuda_kernel->link_paths();
  }
};

class KernelLauncher {
  KernelInstantiation const* _kernel_inst;
  dim3 _grid;
  dim3 _block;
  unsigned int _smem;
  cudaStream_t _stream;

 private:
  void pre_launch(std::vector<std::string> arg_types = {}) const {
    (void)arg_types;
#if JITIFY_PRINT_LAUNCH
    std::string arg_types_string =
        (arg_types.empty() ? "..." : reflection::reflect_list(arg_types));
    std::cout << "Launching " << _kernel_inst->_cuda_kernel->function_name()
              << "<<<" << _grid << "," << _block << "," << _smem << ","
              << _stream << ">>>"
              << "(" << arg_types_string << ")" << std::endl;
#endif
  }

 public:
  KernelLauncher(KernelInstantiation const* kernel_inst, dim3 grid, dim3 block,
                 unsigned int smem = 0, cudaStream_t stream = 0)
      : _kernel_inst(kernel_inst),
        _grid(grid),
        _block(block),
        _smem(smem),
        _stream(stream) {}

  // Note: It's important that there is no implicit conversion required
  //         for arg_ptrs, because otherwise the parameter pack version
  //         below gets called instead (probably resulting in a segfault).
  /*! Launch the kernel.
   *
   *  \param arg_ptrs  A vector of pointers to each function argument for the
   *    kernel.
   *  \param arg_types A vector of function argument types represented
   *    as code-strings. This parameter is optional and is only used to print
   *    out the function signature.
   */
  CUresult launch(std::vector<void*> arg_ptrs = {},
                  std::vector<std::string> arg_types = {}) const {
    pre_launch(arg_types);
    return _kernel_inst->_cuda_kernel->launch(_grid, _block, _smem, _stream,
                                              arg_ptrs);
  }

  void safe_launch(std::vector<void*> arg_ptrs = {},
                   std::vector<std::string> arg_types = {}) const {
    pre_launch(arg_types);
    _kernel_inst->_cuda_kernel->safe_launch(_grid, _block, _smem, _stream,
                                            arg_ptrs);
  }

  /*! Launch the kernel.
   *
   *  \param args Function arguments for the kernel.
   */
  template <typename... ArgTypes>
  CUresult launch(const ArgTypes&... args) const {
    return this->launch(std::vector<void*>({(void*)&args...}),
                        {reflection::reflect<ArgTypes>()...});
  }

  /*! Launch the kernel and check for cuda errors.
   *
   *  \param args Function arguments for the kernel.
   */
  template <typename... ArgTypes>
  void safe_launch(const ArgTypes&... args) const {
    return this->safe_launch(std::vector<void*>({(void*)&args...}),
                             {reflection::reflect<ArgTypes>()...});
  }
};

inline Kernel Program::kernel(std::string const& name,
                              std::vector<std::string> const& options) const {
  return Kernel(this, name, options);
}

inline KernelInstantiation Kernel::instantiate(
    std::vector<std::string> const& template_args) const {
  return KernelInstantiation(*this, template_args);
}

template <typename... TemplateArgs>
inline KernelInstantiation Kernel::instantiate() const {
  return this->instantiate(
      std::vector<std::string>({reflection::reflect<TemplateArgs>()...}));
}

template <typename... TemplateArgs>
inline KernelInstantiation Kernel::instantiate(TemplateArgs... targs) const {
  return this->instantiate(
      std::vector<std::string>({reflection::reflect(targs)...}));
}

inline KernelLauncher KernelInstantiation::configure(
    dim3 grid, dim3 block, unsigned int smem, cudaStream_t stream) const {
  return KernelLauncher(this, grid, block, smem, stream);
}

inline KernelLauncher KernelInstantiation::configure_1d_max_occupancy(
    int max_block_size, unsigned int smem, CUoccupancyB2DSize smem_callback,
    cudaStream_t stream, unsigned int flags) const {
  int grid;
  int block;
  CUfunction func = *_cuda_kernel;
  detail::get_1d_max_occupancy(func, smem_callback, &smem, max_block_size,
                               flags, &grid, &block);
  return this->configure(grid, block, smem, stream);
}

}  // namespace experimental

}  // namespace jitify

#if defined(_WIN32) || defined(_WIN64)
#pragma pop_macro("max")
#pragma pop_macro("min")
#pragma pop_macro("strtok_r")
#endif
