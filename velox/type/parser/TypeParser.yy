%{
#include <FlexLexer.h>
#include "velox/common/base/Exceptions.h"
#include "velox/type/Type.h"
#include "velox/type/parser/ParserUtil.h"
%}
%require "3.0.4"
%language "C++"

%define parser_class_name {Parser}
%define api.namespace {facebook::velox::type}
%define api.value.type variant
%parse-param {Scanner* scanner}
%define parse.error verbose

%code requires
{
    namespace facebook::velox::type {
        class Scanner;
    } // namespace facebook::velox::type
    namespace facebook::velox {
        class Type;
    } // namespace facebook::velox
    struct RowArguments {
       std::vector<std::string> names;
       std::vector<std::shared_ptr<const facebook::velox::Type>> types;
    };
} // %code requires

%code
{
    #include <velox/type/parser/Scanner.h>
    #define yylex(x) scanner->lex(x)
}

%token               LPAREN RPAREN COMMA ARRAY MAP ROW FUNCTION DECIMAL
%token <std::string> WORD VARIABLE QUOTED_ID
%token <long long>   NUMBER
%token YYEOF         0

%nterm <std::shared_ptr<const Type>> special_type function_type decimal_type row_type array_type map_type variable_type
%nterm <std::shared_ptr<const Type>> type single_word_type
%nterm <RowArguments> type_list_opt_names
%nterm <std::vector<std::shared_ptr<const Type>>> type_list
%nterm <std::pair<std::string, std::shared_ptr<const Type>>> named_type
%nterm <std::vector<std::string>> type_with_spaces

%%

type_spec : named_type           { scanner->setType($1.second); }
          | type                 { scanner->setType($1); }
          | error                { yyerrok; }
          ;

named_type : QUOTED_ID type             { $1.erase(0, 1); $1.pop_back(); $$ = std::make_pair($1, $2); }  // Remove the quotes.
           | QUOTED_ID type_with_spaces { $1.erase(0, 1); $1.pop_back(); auto type = inferTypeWithSpaces($2, true); $$ = std::make_pair($1, type.second); }  // Remove the quotes.
           | WORD special_type          { $$ = std::make_pair($1, $2); }
           | WORD variable_type         { $$ = std::make_pair($1, $2); }
           | WORD decimal_type          { $$ = std::make_pair($1, $2); }
           | type_with_spaces           { $$ = inferTypeWithSpaces($1); }
           ;

special_type : array_type                  { $$ = $1; }
             | map_type                    { $$ = $1; }
             | row_type                    { $$ = $1; }
             | function_type               { $$ = $1; }
             ;

single_word_type : WORD          { $$ = typeFromString($1); }
                 | variable_type { $$ = $1; }
                 | decimal_type                { $$ = $1; }

type : special_type     { $$ = $1; }
     | single_word_type { $$ = $1; }
     ;

type_with_spaces : type_with_spaces WORD { $1.push_back($2); $$ = std::move($1); }
                 | WORD WORD             { $$.push_back($1); $$.push_back($2); }
                 ;

variable_type : VARIABLE LPAREN NUMBER RPAREN  { $$ = typeFromString($1); }
              | VARIABLE                       { $$ = typeFromString($1); }
              ;

decimal_type : DECIMAL LPAREN NUMBER COMMA NUMBER RPAREN { $$ = DECIMAL($3, $5); }
             ;

type_list : type                   { $$.push_back($1); }
          | type_list COMMA type   { $1.push_back($3); $$ = std::move($1); }
          ;

type_list_opt_names : type_list_opt_names COMMA named_type { $1.names.push_back($3.first); $1.types.push_back($3.second);
                                                             $$.names = std::move($1.names); $$.types = std::move($1.types); }
                    | named_type                           { $$.names.push_back($1.first); $$.types.push_back($1.second); }
                    | type_list_opt_names COMMA type       { $1.names.push_back(""); $1.types.push_back($3);
                                                             $$.names = std::move($1.names); $$.types = std::move($1.types); }
                    | type                                 { $$.names.push_back(""); $$.types.push_back($1); }
                    ;

row_type : ROW LPAREN type_list_opt_names RPAREN  { $$ = ROW(std::move($3.names), std::move($3.types)); }
         ;

array_type : ARRAY LPAREN type RPAREN             { $$ = ARRAY($3); }
           | ARRAY LPAREN type_with_spaces RPAREN { $$ = ARRAY(inferTypeWithSpaces($3, true).second); }
           ;

map_type : MAP LPAREN single_word_type COMMA type RPAREN             { $$ = MAP($3, $5); }
         | MAP LPAREN single_word_type COMMA type_with_spaces RPAREN { $$ = MAP($3, inferTypeWithSpaces($5, true).second); }
         | MAP LPAREN type_with_spaces COMMA type RPAREN             { $$ = MAP(inferTypeWithSpaces($3, true).second, $5); }
         | MAP LPAREN type_with_spaces COMMA type_with_spaces RPAREN { $$ = MAP(inferTypeWithSpaces($3, true).second,
                                                                                inferTypeWithSpaces($5, true).second); }
         ;

function_type : FUNCTION LPAREN type_list RPAREN { auto returnType = $3.back(); $3.pop_back();
                                                   $$ = FUNCTION(std::move($3), returnType); }

%%

void facebook::velox::type::Parser::error(const std::string& msg) {
    VELOX_FAIL("Failed to parse type [{}]. {}", scanner->input(), msg);
}
