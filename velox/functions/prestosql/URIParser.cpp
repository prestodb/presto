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

#include "velox/functions/prestosql/URIParser.h"
#include "velox/external/utf8proc/utf8procImpl.h"
#include "velox/functions/lib/Utf8Utils.h"

namespace facebook::velox::functions {

namespace {
using Mask = std::bitset<128>;

Mask createMask(size_t low, size_t high) {
  VELOX_DCHECK_LE(low, high);
  VELOX_DCHECK_LE(high, 128);

  Mask mask = 0;

  for (size_t i = low; i <= high; i++) {
    mask.set(i);
  }

  return mask;
}

Mask createMask(const std::vector<size_t>& values) {
  Mask mask = 0;

  for (const auto& value : values) {
    VELOX_DCHECK_LE(value, 128);
    mask.set(value);
  }

  return mask;
}

bool test(const Mask& mask, char value) {
  return value < mask.size() && mask.test(value);
}

// a-z or A-Z.
const Mask kAlpha = createMask('a', 'z') | createMask('A', 'Z');
// 0-9.
const Mask kNum = createMask('0', '9');
// 0-9, a-f, or A-F.
const Mask kHex = kNum | createMask('a', 'f') | createMask('A', 'F');
// sub-delims    = "!" / "$" / "&" / "'" / "(" / ")"
//               / "*" / "+" / "," / ";" / "="
const Mask kSubDelims =
    createMask({'!', '$', '&', '\'', '(', ')', '*', '+', ',', ';', '='});
// unreserved    = ALPHA / DIGIT / "-" / "." / "_" / "~"
const Mask kUnreserved = kAlpha | kNum | createMask({'-', '.', '_', '~'});
// pchar         = unreserved / pct-encoded / sub-delims / ":" / "@"
const Mask kPChar = kUnreserved | kSubDelims | createMask({':', '@'});
// query         = *( pchar / "/" / "?" )
// fragment      = *( pchar / "/" / "?" )
//
// DEVIATION FROM RFC 3986!!!
// We support the characters '[' and ']' appearing in the query and fragment
// strings.  This was done in RFC 2732, but undone in RFC 3986. We add support
// for it here so we support a superset of the URLs supported by Presto Java.
const Mask kQueryOrFragment = kPChar | createMask({'/', '?', '[', ']'});
// path          = path-abempty    ; begins with "/" or is empty
//                / path-absolute   ; begins with "/" but not "//"
//                / path-noscheme   ; begins with a non-colon segment
//                / path-rootless   ; begins with a segment
//                / path-empty      ; zero characters
//
//  path-abempty  = *( "/" segment )
//  path-absolute = "/" [ segment-nz *( "/" segment ) ]
//  path-noscheme = segment-nz-nc *( "/" segment )
//  path-rootless = segment-nz *( "/" segment )
//  path-empty    = 0<pchar>
//
//  segment       = *pchar
//  segment-nz    = 1*pchar
//  segment-nz-nc = 1*( unreserved / pct-encoded / sub-delims / "@" )
//                ; non-zero-length segment without any colon ":"
const Mask kPath = kPChar | createMask({'/'});
// segment-nz-nc  = 1*( unreserved / pct-encoded / sub-delims / "@" )
//                ; non-zero-length segment without any colon ":"
const Mask kPathNoColonPrefix = kPChar & createMask({':'}).flip();
// reg-name      = *( unreserved / pct-encoded / sub-delims )
const Mask kRegName = kUnreserved | kSubDelims;
// IPvFuture     = "v" 1*HEXDIG "." 1*( unreserved / sub-delims / ":" )
// userinfo      = *( unreserved / pct-encoded / sub-delims / ":" )
const Mask kIPVFutureSuffixOrUserInfo =
    kUnreserved | kSubDelims | createMask({':'});
// scheme        = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
const Mask kScheme = kAlpha | kNum | createMask({'+', '-', '.'});
// Not explicitly called out in the spec, but these are the only characters that
// can legally follow a host name in a URI (as part of a port, query, fragment,
// or path respectively). Used to differentiate an IPv4 address from a reg-name
// that has an IPv4 address as its prefix.
const Mask kFollowingHost = createMask({':', '?', '#', '/'});

// The functions below follow the general conventions:
//
// They are named consumeX or tryConsumeX where X is a part of the URI string
// that generally corresponds to a rule in the ABNF grammar for URIs defined in
// Appendix A of RFC 3986.
//
// Functions will take 3 or 4 arguments, `str` the URI string we are parsing,
// `len` the length of the URI string we are parsing, and `pos` our current
// position in the string at the time this function was called. Some functions
// all take a fourth `uri` argument, the URI struct that is to be populated with
// extracted sections of the URI string as part of parsing. If a function
// successfully parses the string, pos is updated to be the first character
// after the substring that was successfully parsed. In addition, if the
// function takes `uri` it will be populated with the relevant section of the
// URI.
//
// Functions named consumeX will always succeed because they can accept an empty
// string.
//
// Functions named tryConsumeX may not successfully parse a substring, they will
// return a bool, with `true` indicating that it was successful and `false`
// indicating it was not. If it was not successful `pos` and `uri` will not be
// modified.
//
// All rules are greedy and will consume as much of the string as possible
// starting from `pos`.
//
// Naturally these conventions do not apply to helper functions.

// pct-encoded   = "%" HEXDIG HEXDIG
bool tryConsumePercentEncoded(const char* str, const size_t len, int32_t& pos) {
  if (len - pos < 3) {
    return false;
  }

  if (str[pos] != '%' || !test(kHex, str[pos + 1]) ||
      !test(kHex, str[pos + 2])) {
    return false;
  }

  pos += 3;

  return true;
}

// Helper function that consumes as much of `str` from `pos` as possible where a
// character passes mask, is part of a percent encoded character, or is an
// allowed UTF-8 character.
//
// `pos` is updated to the first character in `str` that was not consumed and
// `hasEncoded` is set to true if any percent encoded characters were
// encountered.
void consume(
    const Mask& mask,
    const char* str,
    const size_t len,
    int32_t& pos,
    bool& hasEncoded) {
  while (pos < len) {
    if (test(mask, str[pos])) {
      pos++;
      continue;
    }

    if (tryConsumePercentEncoded(str, len, pos)) {
      hasEncoded = true;
      continue;
    }

    // Masks cover all ASCII characters, check if this is an allowed UTF-8
    // character.
    if ((unsigned char)str[pos] > 127) {
      // Get the UTF-8 code point.
      int32_t codePoint;
      auto valid = tryGetUtf8CharLength(str + pos, len - pos, codePoint);

      // Check if it's a valid UTF-8 character.
      // The range after ASCII characters up to 159 covers control characters
      // which are not allowed.
      if (valid > 0 && codePoint > 159) {
        const auto category = utf8proc_get_property(codePoint)->category;
        // White space characters are also not allowed. The range of categories
        // excluded here are categories of white space.
        if (category < UTF8PROC_CATEGORY_ZS ||
            category > UTF8PROC_CATEGORY_ZP) {
          // Increment over the whole (potentially multi-byte) character.
          pos += valid;
          continue;
        }
      }
    }

    break;
  }
}

// path          = path-abempty    ; begins with "/" or is empty
//                / path-absolute   ; begins with "/" but not "//"
//                / path-noscheme   ; begins with a non-colon segment
//                / path-rootless   ; begins with a segment
//                / path-empty      ; zero characters
//
//  path-abempty  = *( "/" segment )
//  path-absolute = "/" [ segment-nz *( "/" segment ) ]
//  path-noscheme = segment-nz-nc *( "/" segment )
//  path-rootless = segment-nz *( "/" segment )
//  path-empty    = 0<pchar>
//
//  segment       = *pchar
//  segment-nz    = 1*pchar
//  segment-nz-nc = 1*( unreserved / pct-encoded / sub-delims / "@" )
//                ; non-zero-length segment without any colon ":"
//
// For our purposes this is just a complicated way of saying a possibly empty
// string of characters that match the kPath Mask or are percent encoded.
//
// The only case in Velox for distinguishing the different types of paths is in
// the relative-part of a relative-ref which only supports path-noscheme, but
// not path-rootless. Without this, it can be impossible to distinguish the ":"
// between the scheme and hier-part of a URL and a colon appearining in the
// relative-part of a relative-ref.
//
// To handle this we add the template bool restrictColonInPrefix which when set
// just means we don't allow any ":" to appear before the first "/".
//
// As an example "a:b/c/d" would be allowed when restrictColonInPrefix is false,
// but not when it's true. While "/a:b/c/d" would be allowed in either case.
template <bool restrictColonInPrefix>
void consumePath(const char* str, const size_t len, int32_t& pos, URI& uri) {
  int32_t posInPath = pos;

  if constexpr (restrictColonInPrefix) {
    // Consume a prefix without ':' or '/'.
    consume(kPathNoColonPrefix, str, len, posInPath, uri.pathHasEncoded);
    // The path continues only if the next character is a '/'.
    if (posInPath != len && str[posInPath] == '/') {
      consume(kPath, str, len, posInPath, uri.pathHasEncoded);
    }
  } else {
    consume(kPath, str, len, posInPath, uri.pathHasEncoded);
  }

  uri.path = StringView(str + pos, posInPath - pos);
  pos = posInPath;
}

// Returns whether `probe` is in the range[`low`, `high`].
FOLLY_ALWAYS_INLINE bool inRange(char probe, char low, char high) {
  return probe >= low && probe <= high;
}

// IPv4address   = dec-octet "." dec-octet "." dec-octet "." dec-octet
// dec-octet     = DIGIT                 ; 0-9
//               / %x31-39 DIGIT         ; 10-99
//               / "1" 2DIGIT            ; 100-199
//               / "2" %x30-34 DIGIT     ; 200-249
//               / "25" %x30-35          ; 250-255
bool tryConsumeIPV4Address(const char* str, const size_t len, int32_t& pos) {
  int32_t posInAddress = pos;

  for (int i = 0; i < 4; i++) {
    if (posInAddress == len) {
      return false;
    }

    if (str[posInAddress] == '2' && posInAddress < len - 2 &&
        str[posInAddress + 1] == '5' &&
        inRange(str[posInAddress + 2], '0', '5')) {
      // 250-255
      posInAddress += 3;
    } else if (
        str[posInAddress] == '2' && posInAddress < len - 2 &&
        inRange(str[posInAddress + 1], '0', '4') &&
        inRange(str[posInAddress + 2], '0', '9')) {
      // 200-249
      posInAddress += 3;
    } else if (
        str[posInAddress] == '1' && posInAddress < len - 2 &&
        inRange(str[posInAddress + 1], '0', '9') &&
        inRange(str[posInAddress + 2], '0', '9')) {
      // 100-199
      posInAddress += 3;
    } else if (inRange(str[posInAddress], '0', '9')) {
      if (posInAddress < len - 1 && inRange(str[posInAddress + 1], '0', '9')) {
        // 10-99
        posInAddress += 2;
      } else {
        // 0-9
        posInAddress += 1;
      }
    } else {
      return false;
    }

    if (i < 3) {
      // An IPv4 address must have exactly 4 parts.
      if (posInAddress == len || str[posInAddress] != '.') {
        return false;
      }

      // Consume '.'.
      posInAddress++;
    }
  }

  pos = posInAddress;
  return true;
}

// Returns true if the substring starting from `pos` is '::'.
bool isAtCompression(const char* str, const size_t len, const int32_t pos) {
  return pos < len - 1 && str[pos] == ':' && str[pos + 1] == ':';
}

// IPvFuture     = "v" 1*HEXDIG "." 1*( unreserved / sub-delims / ":" )
bool tryConsumeIPVFuture(const char* str, const size_t len, int32_t& pos) {
  int32_t posInAddress = pos;

  if (posInAddress == len || str[posInAddress] != 'v') {
    return false;
  }

  // Consume 'v'.
  posInAddress++;

  // Consume a string of hex digits.
  int32_t posInHex = posInAddress;
  while (posInHex < len) {
    if (test(kHex, str[posInHex])) {
      posInHex++;
    } else {
      break;
    }
  }

  // The string of hex digits has to be non-empty.
  if (posInHex == posInAddress) {
    return false;
  }

  posInAddress = posInHex;

  // The string of hex digits must be followed by a '.'.
  if (posInAddress == len || str[posInAddress] != '.') {
    return false;
  }

  // Consume '.'.
  posInAddress++;

  int32_t posInSuffix = posInAddress;
  while (posInSuffix < len) {
    if (test(kIPVFutureSuffixOrUserInfo, str[posInSuffix])) {
      posInSuffix++;
    } else {
      break;
    }
  }

  // The suffix must be non-empty.
  if (posInSuffix == posInAddress) {
    return false;
  }

  pos = posInSuffix;
  return true;
}

// IP-literal    = "[" ( IPv6address / IPvFuture  ) "]"
bool tryConsumeIPLiteral(const char* str, const size_t len, int32_t& pos) {
  int32_t posInAddress = pos;

  // The IP Literal must start with '['.
  if (posInAddress == len || str[posInAddress] != '[') {
    return false;
  }

  // Consume '['.
  posInAddress++;

  // The contents must be an IPv6 address or an IPvFuture.
  if (!tryConsumeIPV6Address(str, len, posInAddress) &&
      !tryConsumeIPVFuture(str, len, posInAddress)) {
    return false;
  }

  // The IP literal must end with ']'.
  if (posInAddress == len || str[posInAddress] != ']') {
    return false;
  }

  // Consume ']'.
  posInAddress++;
  pos = posInAddress;

  return true;
}

// port          = *DIGIT
void consumePort(const char* str, const size_t len, int32_t& pos, URI& uri) {
  int32_t posInPort = pos;

  while (posInPort < len) {
    if (test(kNum, str[posInPort])) {
      posInPort++;
      continue;
    }

    break;
  }

  uri.port = StringView(str + pos, posInPort - pos);
  pos = posInPort;
}

// host          = IP-literal / IPv4address / reg-name
// reg-name      = *( unreserved / pct-encoded / sub-delims )
void consumeHost(const char* str, const size_t len, int32_t& pos, URI& uri) {
  int32_t posInHost = pos;

  if (!tryConsumeIPLiteral(str, len, posInHost)) {
    int32_t posInIPV4Address = posInHost;
    if (tryConsumeIPV4Address(str, len, posInIPV4Address) &&
        (posInIPV4Address == len ||
         test(kFollowingHost, str[posInIPV4Address]))) {
      // reg-name and IPv4 addresses are hard to distinguish, a reg-name could
      // have a valid IPv4 address as a prefix, but treating that prefix as an
      // IPv4 address would make this URI invalid. We make sure that if we
      // detect an IPv4 address it either goes to the end of the string, or is
      // followed by one of the characters that can appear after a host name
      // (and importantly can't appear in a reg-name).
      posInHost = posInIPV4Address;
    } else {
      consume(kRegName, str, len, posInHost, uri.hostHasEncoded);
    }
  }

  uri.host = StringView(str + pos, posInHost - pos);
  pos = posInHost;
}

// authority     = [ userinfo "@" ] host [ ":" port ]
void consumeAuthority(
    const char* str,
    const size_t len,
    int32_t& pos,
    URI& uri) {
  int32_t posInAuthority = pos;

  // Dummy variable to pass in as reference.
  bool authorityHasEncoded;
  consume(
      kIPVFutureSuffixOrUserInfo,
      str,
      len,
      posInAuthority,
      authorityHasEncoded);

  // The user info must be followed by '@'.
  if (posInAuthority != len && str[posInAuthority] == '@') {
    // Consume '@'.
    posInAuthority++;
  } else {
    posInAuthority = pos;
  }

  consumeHost(str, len, posInAuthority, uri);

  // The port must be preceded by a ':'.
  if (posInAuthority < len && str[posInAuthority] == ':') {
    // Consume ':'.
    posInAuthority++;
    consumePort(str, len, posInAuthority, uri);
  }

  pos = posInAuthority;
}

// scheme        = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
bool tryConsumeScheme(
    const char* str,
    const size_t len,
    int32_t& pos,
    URI& uri) {
  int32_t posInScheme = pos;

  // The scheme must start with a letter.
  if (posInScheme == len || !test(kAlpha, str[posInScheme])) {
    return false;
  }

  // Consume the first letter.
  posInScheme++;

  while (posInScheme < len && test(kScheme, str[posInScheme])) {
    posInScheme++;
  }

  uri.scheme = StringView(str + pos, posInScheme - pos);
  pos = posInScheme;
  return true;
}

// relative-part = "//" authority path-abempty
//               / path-absolute
//               / path-noscheme
//               / path-empty
//
// hier-part     = "//" authority path-abempty
//               / path-absolute
//               / path-rootless
//               / path-empty
//
// Since we don't distinguish between path types these are functionally the same
// thing.
template <bool isRelativePart>
void consumeRelativePartOrHierPart(
    const char* str,
    const size_t len,
    int32_t& pos,
    URI& uri) {
  if (pos < len - 1 && str[pos] == '/' && str[pos + 1] == '/') {
    // Consume '//'.
    pos += 2;
    consumeAuthority(str, len, pos, uri);

    if (pos < len && str[pos] == '/') {
      consumePath<false>(str, len, pos, uri);
    }

    return;
  }

  if constexpr (isRelativePart) {
    // In the relative part, there's a restriction that there cannot be any ':'
    // until the first '/'.
    consumePath<true>(str, len, pos, uri);
  } else {
    consumePath<false>(str, len, pos, uri);
  }
}

// query         = *( pchar / "/" / "?" )
// fragment      = *( pchar / "/" / "?" )
void consumeQueryAndFragment(
    const char* str,
    const size_t len,
    int32_t& pos,
    URI& uri) {
  if (pos < len && str[pos] == '?') {
    int32_t posInQuery = pos;
    // Consume '?'.
    posInQuery++;
    // Consume query.
    consume(kQueryOrFragment, str, len, posInQuery, uri.queryHasEncoded);

    // Don't include the '?'.
    uri.query = StringView(str + pos + 1, posInQuery - pos - 1);
    pos = posInQuery;
  }

  if (pos < len && str[pos] == '#') {
    int32_t posInFragment = pos;
    // Consume '#'.
    posInFragment++;
    // Consume fragment.
    consume(kQueryOrFragment, str, len, posInFragment, uri.fragmentHasEncoded);

    // Don't include the '#'.
    uri.fragment = StringView(str + pos + 1, posInFragment - pos - 1);
    pos = posInFragment;
  }
}

// relative-ref  = relative-part [ "?" query ] [ "#" fragment ]
void consumeRelativeRef(
    const char* str,
    const size_t len,
    int32_t& pos,
    URI& uri) {
  consumeRelativePartOrHierPart<true>(str, len, pos, uri);

  consumeQueryAndFragment(str, len, pos, uri);
}

// URI           = scheme ":" hier-part [ "?" query ] [ "#" fragment ]
bool tryConsumeUri(const char* str, const size_t len, int32_t& pos, URI& uri) {
  URI result;
  int32_t posInUri = pos;

  if (!tryConsumeScheme(str, len, posInUri, result)) {
    return false;
  }

  // Scheme is always followed by ':'.
  if (posInUri == len || str[posInUri] != ':') {
    return false;
  }

  // Consume ':'.
  posInUri++;

  consumeRelativePartOrHierPart<false>(str, len, posInUri, result);
  consumeQueryAndFragment(str, len, posInUri, result);

  pos = posInUri;
  uri = result;
  return true;
}

} // namespace

// IPv6address   =                            6( h16 ":" ) ls32
//               /                       "::" 5( h16 ":" ) ls32
//               / [               h16 ] "::" 4( h16 ":" ) ls32
//               / [ *1( h16 ":" ) h16 ] "::" 3( h16 ":" ) ls32
//               / [ *2( h16 ":" ) h16 ] "::" 2( h16 ":" ) ls32
//               / [ *3( h16 ":" ) h16 ] "::"    h16 ":"   ls32
//               / [ *4( h16 ":" ) h16 ] "::"              ls32
//               / [ *5( h16 ":" ) h16 ] "::"              h16
//               / [ *6( h16 ":" ) h16 ] "::"
// h16           = 1*4HEXDIG
// ls32          = ( h16 ":" h16 ) / IPv4address
bool tryConsumeIPV6Address(const char* str, const size_t len, int32_t& pos) {
  bool hasCompression = false;
  uint8_t numBytes = 0;
  int32_t posInAddress = pos;

  if (isAtCompression(str, len, posInAddress)) {
    hasCompression = true;
    // Consume the compression '::'.
    posInAddress += 2;
  }

  while (posInAddress < len && numBytes < 16) {
    int32_t posInHex = posInAddress;
    for (int i = 0; i < 4; i++) {
      if (posInHex == len || !test(kHex, str[posInHex])) {
        break;
      }

      posInHex++;
    }

    if (posInHex == posInAddress) {
      // We need to be able to consume at least one hex digit.
      break;
    }

    if (posInHex < len) {
      if (str[posInHex] == '.') {
        // We may be in the IPV4 Address.
        if (tryConsumeIPV4Address(str, len, posInAddress)) {
          numBytes += 4;
          break;
        } else {
          // A '.' can't appear anywhere except in a valid IPV4 address.
          return false;
        }
      }
      if (str[posInHex] == ':') {
        if (isAtCompression(str, len, posInHex)) {
          if (hasCompression) {
            // We can't have two compressions.
            return false;
          } else {
            // We found a 2 byte hex value followed by a compression.
            numBytes += 2;
            hasCompression = true;
            // Consume the hex block and the compression '::'.
            posInAddress = posInHex + 2;

            continue;
          }
        } else {
          if (posInHex == len || !test(kHex, str[posInHex + 1])) {
            // Peak ahead, we can't end on a single ':'.
            return false;
          }
          // We found a 2 byte hex value followed by a single ':'.
          numBytes += 2;
          // Consume the hex block and the ':'.
          posInAddress = posInHex + 1;

          continue;
        }
      } else {
        // We found a 2 byte hex value at the end of the string.
        numBytes += 2;
        posInAddress = posInHex;
        break;
      }
    }

    break;
  }

  // A valid IPv6 address must have exactly 16 bytes, or a compression.
  if ((numBytes == 16 && !hasCompression) ||
      (hasCompression && numBytes <= 14 && numBytes % 2 == 0)) {
    pos = posInAddress;
    return true;
  } else {
    return false;
  }
}

// URI-reference = URI / relative-ref
bool parseUri(const StringView& uriStr, URI& uri) {
  int32_t pos = 0;
  if (tryConsumeUri(uriStr.data(), uriStr.size(), pos, uri) &&
      pos == uriStr.size()) {
    return true;
  }

  pos = 0;
  consumeRelativeRef(uriStr.data(), uriStr.size(), pos, uri);

  return pos == uriStr.size();
}
} // namespace facebook::velox::functions
