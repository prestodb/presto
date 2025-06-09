%{
#include <vector>
#include <memory>

#include "velox/expression/signature_parser/SignatureParser.yy.h"  // @manual
#include "velox/expression/signature_parser/Scanner.h"
#define YY_DECL int facebook::velox::exec::Scanner::lex(facebook::velox::exec::Parser::semantic_type *yylval)

std::string unescape_doublequote(const char* yytext) {
    size_t len = strlen(yytext);
    std::string output;
    output.resize(len);

    int i = 0;
    int j = 0;

    while (i < len - 1) {
        if (yytext[i] == '"' && yytext[i+1] == '"') {
            output[j++] = '"';
            i += 2;
        } else {
            output[j++] = yytext[i++];
        }
    }
    // Check if the last character needs to be added.
    if (i < len) {
       output[j++] = yytext[i++];
    }
    output.resize(j);
    return output;
}
%}

%option c++ noyywrap noyylineno nodefault caseless

A   [A|a]
B   [B|b]
C   [C|c]
D   [D|d]
E   [E|e]
F   [F|f]
G   [G|g]
H   [H|h]
I   [I|i]
J   [J|j]
K   [K|k]
L   [L|l]
M   [M|m]
O   [O|o]
P   [P|p]
R   [R|r]
S   [S|s]
T   [T|t]
U   [U|u]
W   [W|w]
X   [X|x]
Y   [Y|y]
Z   [Z|z]

WORD              ([[:alnum:]_]*)
QUOTED_ID         (\"([^\"\n]|\"\")*\")
ROW               (ROW|STRUCT)

%%

"("                return Parser::token::LPAREN;
")"                return Parser::token::RPAREN;
","                return Parser::token::COMMA;
(ARRAY)            return Parser::token::ARRAY;
(MAP)              return Parser::token::MAP;
(FUNCTION)         return Parser::token::FUNCTION;
(DECIMAL)          yylval->build<std::string>(YYText()); return Parser::token::DECIMAL;
{ROW}              return Parser::token::ROW;
{WORD}             yylval->build<std::string>(YYText()); return Parser::token::WORD;
{QUOTED_ID}        {auto val = unescape_doublequote(YYText()); yylval->build<std::string>(val.c_str()); return Parser::token::QUOTED_ID;}
<<EOF>>            return Parser::token::YYEOF;
.               /* no action on unmatched input */

%%

int yyFlexLexer::yylex() {
    throw std::runtime_error("Bad call to yyFlexLexer::yylex()");
}

#include "velox/expression/signature_parser/SignatureParser.h"

facebook::velox::exec::TypeSignature facebook::velox::exec::parseTypeSignature(
    const std::string& signatureText) {
  std::istringstream is(signatureText);
  std::ostringstream os;
  facebook::velox::exec::TypeSignaturePtr signature;
  facebook::velox::exec::Scanner scanner{is, os, signature, signatureText};
  facebook::velox::exec::Parser parser{&scanner};
  parser.parse();
  VELOX_CHECK(signature, "Failed to parse signature [{}]", signatureText);
  return std::move(*signature);
}
