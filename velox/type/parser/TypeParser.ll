%{
#include <vector>
#include <memory>
#include <map>

#include "velox/type/parser/TypeParser.yy.h"  // @manual
#include "velox/type/parser/Scanner.h"

#define YY_DECL int facebook::velox::type::Scanner::lex(facebook::velox::type::Parser::semantic_type *yylval)
%}

%option c++ noyywrap noyylineno nodefault caseless ecs

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

WORD              ([[:alpha:][:alnum:]_]*)
QUOTED_ID         (['"']([^"\n]|"")*['"'])
NUMBER            ([[:digit:]]+)
SIGNED_INT        (-?[[:digit:]]+)
VARIABLE          (VARCHAR|VARBINARY)
WORD_WITH_PERIODS ([[:alpha:]_][[:alnum:]_]*)(\.([[:alpha:]_][[:alnum:]_]*))*

%%

"("                return Parser::token::LPAREN;
")"                return Parser::token::RPAREN;
"{"                return Parser::token::LBRACE;
"}"                return Parser::token::RBRACE;
","                return Parser::token::COMMA;
"."                return Parser::token::PERIOD;
":"                return Parser::token::COLON;
(ARRAY)            return Parser::token::ARRAY;
(MAP)              return Parser::token::MAP;
(FUNCTION)         return Parser::token::FUNCTION;
(DECIMAL)          return Parser::token::DECIMAL;
(ROW)              return Parser::token::ROW;
{VARIABLE}         yylval->build<std::string>(YYText()); return Parser::token::VARIABLE;
{NUMBER}           yylval->build<long long>(folly::to<int>(YYText())); return Parser::token::NUMBER;
{SIGNED_INT}       yylval->build<long long>(folly::to<int>(YYText())); return Parser::token::SIGNED_INT;
{WORD}             yylval->build<std::string>(YYText()); return Parser::token::WORD;
{WORD_WITH_PERIODS}             yylval->build<std::string>(YYText()); return Parser::token::WORD_WITH_PERIODS;
{QUOTED_ID}        yylval->build<std::string>(YYText()); return Parser::token::QUOTED_ID;
<<EOF>>            return Parser::token::YYEOF;
.               /* no action on unmatched input */

%%

int yyFlexLexer::yylex() {
    throw std::runtime_error("Bad call to yyFlexLexer::yylex()");
}

#include "velox/type/parser/TypeParser.h"

facebook::velox::TypePtr facebook::velox::parseType(const std::string& typeText)
 {
    std::istringstream is(typeText);
    std::ostringstream os;
    facebook::velox::TypePtr type;
    facebook::velox::type::Scanner scanner{is, os, type, typeText};
    facebook::velox::type::Parser parser{ &scanner };
    parser.parse();
    VELOX_CHECK(type, "Failed to parse type [{}]", typeText);
    return type;
}
