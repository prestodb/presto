%{
#include <FlexLexer.h>
#include "velox/common/base/Exceptions.h"
#include "velox/type/Type.h"
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
    using namespace facebook::velox;
    TypePtr typeFromString(const std::string& type) {
        auto upper = type;
        std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
        if (upper == "INT") {
            upper = "INTEGER";
        } else if (upper == "DOUBLE PRECISION") {
            upper = "DOUBLE";
        }
        auto inferredType = getType(upper, {});
        VELOX_CHECK(inferredType, "Failed to parse type [{}]. Type not registered.", type);
        return inferredType;
    }
}

%token               LPAREN RPAREN COMMA ARRAY MAP ROW FUNCTION DECIMAL
%token <std::string> WORD VARIABLE QUOTED_ID TYPE_WITH_SPACES
%token <long long>   NUMBER
%token YYEOF         0

%nterm <std::shared_ptr<const Type>> type array_type map_type variable_type
%nterm <std::pair<std::string, std::shared_ptr<const Type>>> named_type
%nterm <std::shared_ptr<const Type>> row_type function_type decimal_type simple_type
%nterm <std::string> identifier
%nterm <std::vector<std::shared_ptr<const Type>>> type_list
%nterm <RowArguments> type_list_opt_names

%%

type_spec : named_type           { scanner->setType($1.second); }
          | type                 { scanner->setType($1); }
          | error                { yyerrok; }
          ;

named_type :  identifier type       { $$ = std::make_pair($1, $2); }
           ;

type : array_type                  { $$ = $1; }
     | map_type                    { $$ = $1; }
     | row_type                    { $$ = $1; }
     | simple_type                 { $$ = $1; }
     | function_type               { $$ = $1; }
     | variable_type               { $$ = $1; }
     | decimal_type                { $$ = $1; }
     ;

simple_type : WORD                { $$ = typeFromString($1); }
            | TYPE_WITH_SPACES    { $$ = typeFromString($1); }
            ;

variable_type : VARIABLE LPAREN NUMBER RPAREN  { $$ = typeFromString($1); }
              | VARIABLE                       { $$ = typeFromString($1); }
              ;

array_type : ARRAY LPAREN type RPAREN { $$ = ARRAY($3); }
           ;

decimal_type : DECIMAL LPAREN NUMBER COMMA NUMBER RPAREN { $$ = DECIMAL($3, $5); }
             ;

type_list : type                   { $$.push_back($1); }
          | type_list COMMA type   { $1.push_back($3); $$ = std::move($1); }
          ;

type_list_opt_names : type                                 { $$.names.push_back(""); $$.types.push_back($1); }
                    | named_type                           { $$.names.push_back($1.first); $$.types.push_back($1.second); }
                    | type_list_opt_names COMMA type       { $1.names.push_back(""); $1.types.push_back($3);
                                                             $$.names = std::move($1.names); $$.types = std::move($1.types); }
                    | type_list_opt_names COMMA named_type { $1.names.push_back($3.first); $1.types.push_back($3.second);
                                                             $$.names = std::move($1.names); $$.types = std::move($1.types); }
                    ;

row_type : ROW LPAREN type_list_opt_names RPAREN  { $$ = ROW(std::move($3.names), std::move($3.types)); }
         ;

map_type : MAP LPAREN type COMMA type RPAREN { $$ = MAP($3, $5); }
         ;

function_type : FUNCTION LPAREN type_list RPAREN { auto returnType = $3.back(); $3.pop_back();
                                                   $$ = FUNCTION(std::move($3), returnType); }

identifier : QUOTED_ID { $1.erase(0, 1); $1.pop_back(); $$ = $1; } // Remove the quotes.
           | WORD     { $$ = $1; }
           ;

%%

void facebook::velox::type::Parser::error(const std::string& msg) {
    VELOX_FAIL("Failed to parse type [{}]", scanner->input());
}
