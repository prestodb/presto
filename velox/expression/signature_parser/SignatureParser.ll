%{
#include <vector>
#include <memory>

#include "velox/expression/signature_parser/SignatureParser.yy.h"  // @manual
#include "velox/expression/signature_parser/Scanner.h"
#define YY_DECL int facebook::velox::exec::Scanner::lex(facebook::velox::exec::Parser::semantic_type *yylval)
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
QUOTED_ID         (['"'][[:alnum:][:space:]_]*['"'])
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
{QUOTED_ID}        yylval->build<std::string>(YYText()); return Parser::token::QUOTED_ID;
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
