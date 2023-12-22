%{
#include <FlexLexer.h>
#include "velox/common/base/Exceptions.h"
#include "velox/expression/TypeSignature.h"
#include "velox/expression/signature_parser/ParseUtil.h"
%}
%require "3.0.4"
%language "C++"

%define parser_class_name {Parser}
%define api.namespace {facebook::velox::exec}
%define api.value.type variant
%parse-param {Scanner* scanner}
%define parse.error verbose

%code requires
{
    namespace facebook::velox::exec {
        class Scanner;
        class TypeSignature;
    } // namespace facebook::velox::exec
} // %code requires

%code
{
    #include <velox/expression/signature_parser/Scanner.h>
    #define yylex(x) scanner->lex(x)
}

%token               LPAREN RPAREN COMMA ARRAY MAP ROW FUNCTION
%token <std::string> WORD VARIABLE QUOTED_ID DECIMAL
%token YYEOF         0

%nterm <std::shared_ptr<exec::TypeSignature>> special_type function_type decimal_type row_type array_type map_type
%nterm <std::shared_ptr<exec::TypeSignature>> type named_type
%nterm <std::vector<exec::TypeSignature>> type_list type_list_opt_names
%nterm <std::vector<std::string>> type_with_spaces

%%

type_spec : type                 { scanner->setTypeSignature($1); }
          | type_with_spaces     { scanner->setTypeSignature(inferTypeWithSpaces($1)); }
          | error                { yyerrok; }
          ;

type : special_type   { $$ = $1; }
     | WORD           { $$ = std::make_shared<exec::TypeSignature>(exec::TypeSignature($1, {})); }
     ;

special_type : array_type                  { $$ = $1; }
             | map_type                    { $$ = $1; }
             | row_type                    { $$ = $1; }
             | function_type               { $$ = $1; }
             | decimal_type                { $$ = $1; }
             ;

named_type : QUOTED_ID type          { $1.erase(0, 1); $1.pop_back(); $$ = std::make_shared<exec::TypeSignature>(exec::TypeSignature($2->baseName(), $2->parameters(), $1)); }  // Remove the quotes.
           | WORD special_type       { $$ = std::make_shared<exec::TypeSignature>(exec::TypeSignature($2->baseName(), $2->parameters(), $1)); }
           | type_with_spaces        { $$ = inferTypeWithSpaces($1, true); }
           ;

type_with_spaces : type_with_spaces WORD { $1.push_back($2); $$ = std::move($1); }
                 | WORD WORD             { $$.push_back($1); $$.push_back($2); }
                 ;

decimal_type : DECIMAL LPAREN WORD COMMA WORD RPAREN { $$ = std::make_shared<exec::TypeSignature>(exec::TypeSignature($1, { exec::TypeSignature($3, {}), exec::TypeSignature($5, {}) })); }
             ;

type_list : type                   { $$.push_back(*($1)); }
          | type_list COMMA type   { $1.push_back(*($3)); $$ = std::move($1); }
          ;

type_list_opt_names : named_type                           { $$.push_back(*($1)); }
                    | type_list_opt_names COMMA named_type { $1.push_back(*($3)); $$ = std::move($1); }
                    | type                                 { $$.push_back(*($1)); }
                    | type_list_opt_names COMMA type       { $1.push_back(*($3)); $$ = std::move($1); }
                    ;

row_type : ROW LPAREN type_list_opt_names RPAREN  { $$ = std::make_shared<exec::TypeSignature>("row", $3); }
         ;

array_type : ARRAY LPAREN type RPAREN             { $$ = std::make_shared<exec::TypeSignature>(exec::TypeSignature("array", { *($3) })); }
           | ARRAY LPAREN type_with_spaces RPAREN { $$ = std::make_shared<exec::TypeSignature>(exec::TypeSignature("array", { *inferTypeWithSpaces($3) })); }
           ;

map_type : MAP LPAREN type COMMA type RPAREN                         { $$ = std::make_shared<exec::TypeSignature>(exec::TypeSignature("map", {*($3), *($5)})); }
         | MAP LPAREN type COMMA type_with_spaces RPAREN             { $$ = std::make_shared<exec::TypeSignature>(exec::TypeSignature("map", {*($3), *inferTypeWithSpaces($5)})); }
         | MAP LPAREN type_with_spaces COMMA type RPAREN             { $$ = std::make_shared<exec::TypeSignature>(exec::TypeSignature("map", {*inferTypeWithSpaces($3), *($5)})); }
         | MAP LPAREN type_with_spaces COMMA type_with_spaces RPAREN { $$ = std::make_shared<exec::TypeSignature>(exec::TypeSignature("map", {*inferTypeWithSpaces($3), *inferTypeWithSpaces($5)})); }
         ;

function_type : FUNCTION LPAREN type_list RPAREN { $$ = std::make_shared<exec::TypeSignature>(exec::TypeSignature("function", {$3})); }

%%

void facebook::velox::exec::Parser::error(const std::string& msg) {
    VELOX_FAIL("Failed to parse type signature [{}]: {}", scanner->input(), msg);
}
