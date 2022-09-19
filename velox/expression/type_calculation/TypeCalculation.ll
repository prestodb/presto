%{
#include "velox/expression/type_calculation/TypeCalculation.yy.h"  // @manual
#include "velox/expression/type_calculation/Scanner.h"
#define YY_DECL int facebook::velox::expression::calculate::Scanner::lex(facebook::velox::expression::calculate::Parser::semantic_type *yylval)
%}

%option c++ noyywrap noyylineno nodefault

integer         ([[:digit:]]+)
var             ([[:alpha:]][[:alnum:]_]*)

%%

"+"             return Parser::token::PLUS;
"-"             return Parser::token::MINUS;
"*"             return Parser::token::MULTIPLY;
"/"             return Parser::token::DIVIDE;
"%"             return Parser::token::MODULO;
"("             return Parser::token::LPAREN;
")"             return Parser::token::RPAREN;
","             return Parser::token::COMMA;
"="             return Parser::token::ASSIGN;
"min"           return Parser::token::MIN;
"max"           return Parser::token::MAX;
{integer}       yylval->build<long long>(strtoll(YYText(), nullptr, 10)); return Parser::token::INT;
{var}           yylval->build<std::string>(YYText()); return Parser::token::VAR;
<<EOF>>         return Parser::token::YYEOF;
.               /* no action on unmatched input */

%%

int yyFlexLexer::yylex() {
    throw std::runtime_error("Bad call to yyFlexLexer::yylex()");
}

#include "velox/expression/type_calculation/TypeCalculation.h"

void facebook::velox::expression::calculation::evaluate(const std::string& calculation, std::unordered_map<std::string, std::optional<int>>& variables) {
    std::istringstream is(calculation);
    facebook::velox::expression::calculate::Scanner scanner{ is, std::cerr, variables};
    facebook::velox::expression::calculate::Parser parser{ &scanner };
    parser.parse();
}
