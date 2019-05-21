/*-
 *
 *  This file is part of Oracle NoSQL Database
 *  Copyright (C) 2011, 2018 Oracle and/or its affiliates.  All rights reserved.
 *
 *  Oracle NoSQL Database is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU Affero General Public License
 *  as published by the Free Software Foundation, version 3.
 *
 *  Oracle NoSQL Database is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public
 *  License in the LICENSE file along with Oracle NoSQL Database.  If not,
 *  see <http://www.gnu.org/licenses/>.
 *
 *  An active Oracle commercial licensing agreement for this product
 *  supercedes this license.
 *
 *  For more information please contact:
 *
 *  Vice President Legal, Development
 *  Oracle America, Inc.
 *  5OP-10
 *  500 Oracle Parkway
 *  Redwood Shores, CA 94065
 *
 *  or
 *
 *  berkeleydb-info_us@oracle.com
 *
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  EOF
 *
 */

/*
 * IMPORTANT: the code generation by Antlr using this grammar is not
 * automatically part of the build.  It has its own target:
 *   ant generate-ddl
 * If this file is modified that target must be run.  Once all relevant
 * testing is done the resulting files must be modified to avoid warnings and
 * errors in Eclipse because of unused imports.  At that point the new files can
 * be check in.
 *
 * This file describes the syntax of the Oracle NoSQL Table DDL.  In order to
 * make the syntax as familiar as possible, the following hierarchy of existing
 * languages is used as the model for each operation:
 * 1.  SQL standard
 * 2.  Oracle SQL
 * 3.  MySQL
 * 4.  SQLite or other SQL
 * 5.  Any comparable declarative language
 * 6.  New syntax, specific to Oracle NoSQL.
 *
 * The major commands in this grammar include
 * o create/drop table
 * o alter table
 * o create/drop index
 * o describe
 * o show
 *
 * Grammar notes:
 *  Antlr resolves ambiguity in token recognition by using order of
 *  declaration, so order matters when ambiguity is possible.  This is most
 *  typical with different types of identifiers that have some overlap.
 *
 *  This grammar uses some extra syntax and Antlr actions to generate more
 *  useful error messages (see use of "notifyErrorListeners").  This is Java-
 *  specific code.  In the future it may be useful to parse in other languages,
 *  which is supported by Antlr4.  If done, these errors may have to be handled
 *  elsewhere or we'd have to handle multiple versions of the grammar with minor
 *  changes for language-specific constructs.
 */

/*
 * Parser rules (start with lowercase).
 */

grammar KVQL;

/*
 * This is the starting rule for the parse.  It accepts one of the number of
 * top-level statements.  The EOF indicates that only a single statement can
 * be accepted and any extraneous input after the statement will generate an
 * error.  Allow a semicolon to terminate statements.  In the future a semicolon
 * may act as a statement separator.
 */
parse : statement EOF;

statement :
    (
    query
  |  update_statement
  | create_table_statement
  | create_index_statement
  | create_user_statement
  | create_role_statement
  | drop_index_statement
  | create_text_index_statement
  | drop_role_statement
  | drop_user_statement
  | alter_table_statement
  | alter_user_statement
  | drop_table_statement
  | grant_statement
  | revoke_statement
  | describe_statement
  | show_statement) ;

/******************************************************************************
 *
 * Query expressions
 *
 ******************************************************************************/

query : prolog? sfw_expr ;

prolog : DECLARE var_decl SEMI (var_decl SEMI)*;

var_decl : var_name type_def;

var_name : DOLLAR id ;

/*
 * TODO : add sfw_expr here when subqueries are supported
 */
expr : or_expr ;


sfw_expr :
    select_clause
    from_clause
    where_clause?
    groupby_clause?
    orderby_clause?
    limit_clause?
    offset_clause? ;

from_clause : 
    FROM (from_table | nested_tables)
         (COMMA expr (AS? var_name))*;

nested_tables : 
    NESTED TABLES
    LP
    from_table
    (ANCESTORS LP ancestor_tables RP) ?
    (DESCENDANTS LP descendant_tables RP) ?
    RP ;

ancestor_tables : from_table (COMMA from_table)* ;

descendant_tables : from_table (COMMA from_table)* ;

/*
 * The ON clause is not allowed for the target table. The grammar allows it,
 * but the translator throws error if present. It is allowed in the grammar
 * for implementation convenience.
 */
from_table : aliased_table_name (ON or_expr)? ;

aliased_table_name : (table_name | SYSTEM_TABLE_NAME) (AS? tab_alias)? ;

tab_alias : DOLLAR? id ;

where_clause : WHERE expr ;

select_clause : SELECT select_list ;

select_list : hints? ( STAR | (expr col_alias (COMMA expr col_alias)*) ) ;

hints : '/*+' hint* '*/' ;

hint : ( (PREFER_INDEXES LP table_name index_name* RP) |
         (FORCE_INDEX    LP table_name index_name  RP) |
         (PREFER_PRIMARY_INDEX LP table_name RP)       |
         (FORCE_PRIMARY_INDEX  LP table_name RP) ) STRING?;

col_alias : (AS id)? ;

orderby_clause : ORDER BY expr sort_spec (COMMA expr sort_spec)* ;

sort_spec : (ASC | DESC)? (NULLS (FIRST | LAST))? ;

groupby_clause : GROUP BY expr (COMMA expr)* ;

limit_clause : LIMIT add_expr;

offset_clause : OFFSET add_expr ;

or_expr : and_expr | or_expr OR and_expr ;

and_expr : not_expr | and_expr AND not_expr ;

not_expr : NOT? is_null_expr ;

is_null_expr : cond_expr (IS NOT? NULL)? ;

cond_expr : comp_expr | exists_expr | is_of_type_expr ;

exists_expr : EXISTS add_expr ;

is_of_type_expr : add_expr IS NOT? OF TYPE?
        LP ONLY? quantified_type_def ( COMMA ONLY? quantified_type_def )* RP;

comp_expr : (add_expr ((comp_op | any_op) add_expr)?) ;

comp_op : EQ | NEQ | GT | GTE | LT | LTE ;

any_op : (EQ_ANY) | (NEQ_ANY) | (GT_ANY) | (GTE_ANY) | (LT_ANY) | (LTE_ANY);

add_expr : multiply_expr ((PLUS | MINUS) multiply_expr)* ;

multiply_expr : unary_expr ((STAR | DIV) unary_expr)* ;

unary_expr : path_expr | (PLUS | MINUS) unary_expr ;

path_expr : primary_expr (map_step | array_step)* ;

/*
 * It's important that filtering_field_step appears before simple_field_step,
 * because a filtering_field_step looks like a function call, which is also
 * part of simple_field_step. By putting filtering_field_step first, it takes
 * priority over the func_call production.
 */
map_step : DOT ( map_filter_step | map_field_step );

map_field_step : ( id | string | var_ref | parenthesized_expr | func_call );

map_filter_step : (KEYS | VALUES) LP expr? RP ;

array_step : array_filter_step | array_slice_step;

array_slice_step : LBRACK expr? COLON expr? RBRACK ;

array_filter_step : LBRACK expr? RBRACK ;

primary_expr :
    const_expr |
    column_ref |
    var_ref |
    array_constructor |
    map_constructor |
    transform_expr |
    func_call |
    count_star |
    case_expr |
    cast_expr |
    parenthesized_expr |
    extract_expr ;

/*
 * If there are 2 ids, the first one refers to a table name/alias and the
 * second to a column in that table. A single id refers to a column in some
 * of the table in the FROM clause. If more than one table has a column of
 * that name, an error is thrown. In this case, the user has to rewrite the
 * query to use table aliases to resolve the ambiguity.
 */
column_ref : id (DOT id)? ;

/*
 * INT/FLOAT literals are translated to Long/Double values.
 */
const_expr : number | string | TRUE | FALSE | NULL;

var_ref : DOLLAR id? ;

array_constructor : LBRACK expr? (COMMA expr)* RBRACK ;

map_constructor :
    (LBRACE expr COLON expr (COMMA expr COLON expr)* RBRACE) |
    (LBRACE RBRACE) ;

transform_expr : SEQ_TRANSFORM LP transform_input_expr COMMA expr RP;

transform_input_expr : expr ;

func_call : id LP (expr (COMMA expr)*)? RP ;

count_star : COUNT LP STAR RP ;

case_expr : CASE WHEN expr THEN expr (WHEN expr THEN expr)* (ELSE expr)? END;

cast_expr : CAST LP expr AS quantified_type_def RP ;

parenthesized_expr : LP expr RP;

extract_expr : EXTRACT LP id FROM expr RP ;

/******************************************************************************
 *
 * updates
 *
 ******************************************************************************/

update_statement : 
    prolog?
    UPDATE table_name AS? tab_alias? update_clause (COMMA update_clause)*
    WHERE expr
    returning_clause? ; 

returning_clause : RETURNING select_list ;

update_clause : 
    (SET set_clause (COMMA (update_clause | set_clause))*) |
    (ADD add_clause (COMMA (update_clause | add_clause))*) |
    (PUT put_clause (COMMA (update_clause | put_clause))*) |
    (REMOVE remove_clause (COMMA remove_clause)*) |
    (SET TTL ttl_clause (COMMA update_clause)*) ;

set_clause : target_expr EQ expr ;

add_clause : target_expr pos_expr? expr ;

put_clause : target_expr expr ;

remove_clause : target_expr ;

ttl_clause : (add_expr (HOURS | DAYS)) | (USING TABLE DEFAULT) ;

target_expr : path_expr ;

pos_expr : add_expr ;


/******************************************************************************
 *
 * Types
 *
 ******************************************************************************/

quantified_type_def : type_def ( STAR | PLUS | QUESTION_MARK)? ;

/*
 * All supported type definitions. The # labels on each line cause Antlr to
 * generate events specific to that type, which allows the parser code to more
 * simply discriminate among types.
 */
type_def :
    binary_def         # Binary
  | array_def          # Array
  | boolean_def        # Boolean
  | enum_def           # Enum
  | float_def          # Float        // float, double, number
  | integer_def        # Int          // int, long
  | json_def           # JSON
  | map_def            # Map
  | record_def         # Record
  | string_def         # StringT
  | timestamp_def      # Timestamp
  | any_def            # Any           // not allowed in DDL
  | anyAtomic_def      # AnyAtomic     // not allowed in DDL
  | anyJsonAtomic_def  # AnyJsonAtomic // not allowed in DDL
  | anyRecord_def      # AnyRecord     // not allowed in DDL
  ;

/*
 * A record contains one or more field definitions.
 */
record_def : RECORD_T LP field_def (COMMA field_def)* RP ;

/*
 * A definition of a named, typed field within a record.
 * The field name must be an identifier, rather than the more general
 * field_name syntax. This restriction is placed by AVRO!
 */
field_def : id type_def default_def? comment? ;

/*
 * The translator checks that the value conforms to the associated type.
 * Binary fields have no default value and as a result they are always nullable.
 * This is enforced in code.
 */
default_def : (default_value not_null?) | (not_null default_value?) ;

/*
 * The id alternative is used for enum defaults. The translator
 * checks that the type of the defualt value conforms with the type of
 * the field.
 */
default_value : DEFAULT (number | string | TRUE | FALSE | id) ;

not_null : NOT NULL ;

map_def : MAP_T LP type_def RP ;

array_def : ARRAY_T LP type_def RP ;

integer_def : (INTEGER_T | LONG_T) ;

json_def : JSON ;

float_def : (FLOAT_T | DOUBLE_T | NUMBER_T) ;

string_def : STRING_T ;

/*
 * Enumeration is defined by a list of ID values.
 *   enum (val1, val2, ...)
 */
enum_def : (ENUM_T LP id_list RP) |
           (ENUM_T LP id_list { notifyErrorListeners("Missing closing ')'"); }) ;

boolean_def : BOOLEAN_T ;

binary_def : BINARY_T (LP INT RP)? ;

timestamp_def: TIMESTAMP_T (LP INT RP)? ;

any_def : ANY_T ;

anyAtomic_def : ANYATOMIC_T;

anyJsonAtomic_def : ANYJSONATOMIC_T;

anyRecord_def : ANYRECORD_T;

/******************************************************************************
 *
 * DDL statements
 *
 ******************************************************************************/

/*
 * id_path is used for table and index names. name_path is used for field paths.
 * Both of these may have multiple components. Table names may reference child 
 * tables using dot notation and similarly, field paths may reference nested 
 * fields using dot notation as well.
 */
id_path : id (DOT id)* ;

name_path : field_name (DOT field_name)* ;

field_name : id | DSTRING ;

/*
 * CREATE TABLE.
 */
create_table_statement :
    CREATE TABLE (IF NOT EXISTS)? table_name comment? LP table_def RP ttl_def? ;

table_name : id_path ;

table_def : (field_def | key_def) (COMMA (field_def | key_def))* ;

key_def : PRIMARY KEY LP (shard_key_def COMMA?)? id_list_with_size? RP ;

shard_key_def :
    (SHARD LP id_list_with_size RP) |
    (LP id_list_with_size { notifyErrorListeners("Missing closing ')'"); }) ;

id_list_with_size : id_with_size (COMMA id_with_size)* ;

id_with_size : id storage_size? ;

storage_size : LP INT RP ;

ttl_def : USING TTL duration ;

/*
 * ALTER TABLE
 */
alter_table_statement : ALTER TABLE table_name alter_def ;

alter_def : alter_field_statements | ttl_def;

/*
 * Table modification -- add, drop, modify fields in an existing table.
 * This definition allows multiple changes to be contained in a single
 * alter table statement.
 */
alter_field_statements :
    LP
    (add_field_statement | drop_field_statement | modify_field_statement)
    (COMMA (add_field_statement | drop_field_statement | modify_field_statement))*
    RP ;

add_field_statement : ADD schema_path type_def default_def? comment? ;

drop_field_statement : DROP schema_path ;

/* not actually implemented */
modify_field_statement :
    MODIFY schema_path type_def default_def? comment? ;

schema_path : init_schema_path_step (DOT schema_path_step)*;

init_schema_path_step : id (LBRACK RBRACK)* ;

schema_path_step : id (LBRACK RBRACK)* | VALUES LP RP ;

/*
 * DROP TABLE
 */
drop_table_statement : DROP TABLE (IF EXISTS)? table_name ;

/*
 * CREATE INDEX
 */
create_index_statement :
    CREATE INDEX (IF NOT EXISTS)? index_name ON table_name
    ((LP index_path_list RP) |
     (LP index_path_list { notifyErrorListeners("Missing closing ')'"); }) )
    comment?;

index_name : id ;

/*
 * A comma-separated list of field paths that may or may not reference nested
 * fields. This is used to reference fields in an index or a describe statement.
 */
index_path_list : index_path (COMMA index_path)* ;

/*
 * index_path handles a basic name_path but adds .keys(), .values(), and []
 * stepss to handle addressing in maps and arrays.
 * NOTE: if the syntax of .keys(), .values(), or [] changes the source should
 * be checked for code that reproduces these constants.
 */
index_path :
    name_path path_type? |
    keys_expr |
    values_expr path_type? ;

keys_expr :
    name_path DOT KEYS LP RP |
    KEYOF LP name_path RP |
    KEYS LP name_path RP ;

values_expr :
    ((name_path DOT VALUES LP RP) |
     (name_path LBRACK RBRACK) |
     (ELEMENTOF LP name_path RP)) ('.' name_path)? ;

path_type : 
    AS (INTEGER_T | LONG_T | DOUBLE_T | STRING_T | BOOLEAN_T | NUMBER_T); 

/*
 * CREATE FULLTEXT INDEX [if not exists] name ON name_path (field [ mapping ], ...)
 */
create_text_index_statement :
    CREATE FULLTEXT INDEX (IF NOT EXISTS)?
    index_name ON table_name fts_field_list es_properties? OVERRIDE? comment?;

/*
 * A list of field names, as above, which may or may not include
 * a text-search mapping specification per field.
 */
fts_field_list :
    LP fts_path_list RP |
    LP fts_path_list {notifyErrorListeners("Missing closing ')'");}
    ;

/*
 * A comma-separated list of paths to field names with optional mapping specs.
 */
fts_path_list : fts_path (COMMA fts_path)* ;

/*
 * A field name with optional mapping spec.
 */
fts_path : index_path jsobject? ;

es_properties: es_property_assignment es_property_assignment* ;

es_property_assignment: ES_SHARDS EQ INT | ES_REPLICAS EQ INT ;

/*
 * DROP INDEX [if exists] index_name ON table_name
 */
drop_index_statement : DROP INDEX (IF EXISTS)? index_name ON table_name
                       OVERRIDE? ;

/*
 * DESC[RIBE] TABLE table_name [field_path[,field_path]]
 * DESC[RIBE] INDEX index_name ON table_name
 */
describe_statement :
    (DESCRIBE | DESC) (AS JSON)?
    (TABLE table_name (
             (LP schema_path_list RP) |
             (LP schema_path_list { notifyErrorListeners("Missing closing ')'")
             ; })
           )? |
     INDEX index_name ON table_name) ;

schema_path_list : schema_path (COMMA schema_path)* ;

/*
 * SHOW TABLES
 * SHOW INDEXES ON table_name
 * SHOW TABLE table_name -- lists hierarchy of the table
 */
show_statement: SHOW (AS JSON)?
        (TABLES
        | USERS
        | ROLES
        | USER identifier_or_string
        | ROLE id
        | INDEXES ON table_name
        | TABLE table_name)
  ;


/******************************************************************************
 *
 * Parse rules of security commands.
 *
 ******************************************************************************/

/*
 * CREATE USER user (IDENTIFIED BY password [PASSWORD EXPIRE]
 * [PASSWORD LIFETIME duration] | IDENTIFIED EXTERNALLY)
 * [ACCOUNT LOCK|UNLOCK] [ADMIN]
 */
create_user_statement :
    CREATE USER create_user_identified_clause account_lock? ADMIN? ;

/*
 * CREATE ROLE role
 */
create_role_statement : CREATE ROLE id ;

/*
 * ALTER USER user [IDENTIFIED BY password [RETAIN CURRENT PASSWORD]]
 *       [CLEAR RETAINED PASSWORD] [PASSWORD EXPIRE]
 *       [PASSWORD LIFETIME duration] [ACCOUNT UNLOCK|LOCK]
 */
alter_user_statement : ALTER USER identifier_or_string
    reset_password_clause? (CLEAR_RETAINED_PASSWORD)? (PASSWORD_EXPIRE)?
        password_lifetime? account_lock? ;

/*
 * DROP USER user [CASCADE]
 */
drop_user_statement : DROP USER identifier_or_string (CASCADE)?;

/*
 * DROP ROLE role_name
 */
drop_role_statement : DROP ROLE id ;

/*
 * GRANT (grant_roles|grant_system_privileges|grant_object_privileges)
 *     grant_roles ::= role [, role]... TO { USER user | ROLE role }
 *     grant_system_privileges ::=
 *         {system_privilege | ALL PRIVILEGES}
 *             [,{system_privilege | ALL PRIVILEGES}]...
 *         TO role
 *     grant_object_privileges ::=
 *         {object_privileges| ALL [PRIVILEGES]}
 *             [,{object_privileges| ALL [PRIVILEGES]}]...
 *         ON table TO role
 */
grant_statement : GRANT
        (grant_roles
        | grant_system_privileges
        | grant_object_privileges)
    ;

/*
 * REVOKE (revoke_roles | revoke_system_privileges | revoke_object_privileges)
 *     revoke_roles ::= role [, role]... FROM { user | role }
 *     revoke_system_privileges ::=
 *         {system_privilege | ALL PRIVILEGES}
 *             [, {system_privilege | ALL PRIVILEGES}]...
 *         FROM role
 *     revoke_object_privileges ::=
 *         {object_privileges| ALL [PRIVILEGES]}
 *             [, { object_privileges | ALL [PRIVILEGES] }]...
 *         ON object FROM role
 */
revoke_statement : REVOKE
        (revoke_roles
        | revoke_system_privileges
        | revoke_object_privileges)
    ;

/*
 * An identifier or a string
 */
identifier_or_string : (id | string);

/*
 * Identified clause, indicates the authentication method of user.
 */
identified_clause : IDENTIFIED by_password ;

/*
 * Identified clause for create user command, indicates the authentication
 * method of user. If the user is an internal user, we use the extended_id
 * for the user name. If the user is an external user, we use STRING for
 * the user name.
 */
create_user_identified_clause :
    id identified_clause (PASSWORD_EXPIRE)? password_lifetime? |
    string IDENTIFIED_EXTERNALLY ;

/*
 * Rule of authentication by password.
 */
by_password : BY string;

/*
 * Rule of password lifetime definition.
 */
password_lifetime : PASSWORD LIFETIME duration;

/*
 * Rule of defining the reset password clause in the alter user statement.
 */
reset_password_clause : identified_clause RETAIN_CURRENT_PASSWORD? ;

account_lock : ACCOUNT (LOCK | UNLOCK) ;

/*
 * Subrule of granting roles to a user or a role.
 */
grant_roles : id_list TO principal ;

/*
 * Subrule of granting system privileges to a role.
 */
grant_system_privileges : sys_priv_list TO id ;

/*
 * Subrule of granting object privileges to a role.
 */
grant_object_privileges : obj_priv_list ON object TO id ;

/*
 * Subrule of revoking roles from a user or a role.
 */
revoke_roles : id_list FROM principal ;

/*
 * Subrule of revoking system privileges from a role.
 */
revoke_system_privileges : sys_priv_list FROM id ;

/*
 * Subrule of revoking object privileges from a role.
 */
revoke_object_privileges : obj_priv_list ON object FROM id  ;

/*
 * Parsing a principal of user or role.
 */
principal : (USER identifier_or_string | ROLE id) ;

sys_priv_list : priv_item (COMMA priv_item)* ;

priv_item : (id | ALL_PRIVILEGES) ;

obj_priv_list : (priv_item | ALL) (COMMA (priv_item | ALL))* ;

/*
 * Subrule of parsing the operated object. For now, only table object is
 * available.
 */
object : table_name ;


/******************************************************************************
 *
 * Literals and identifiers
 *
 ******************************************************************************/

/*
 * Simple JSON parser, derived from example in Terence Parr's book,
 * _The Definitive Antlr 4 Reference_.
 */
json_text : jsobject | jsarray ;

jsobject
    :   LBRACE jspair (',' jspair)* RBRACE    # JsonObject
    |   LBRACE RBRACE                         # EmptyJsonObject ;

jsarray
    :   LBRACK jsvalue (',' jsvalue)* RBRACK  # ArrayOfJsonValues
    |   LBRACK RBRACK                         # EmptyJsonArray ;

jspair :   DSTRING ':' jsvalue                 # JsonPair ;

jsvalue
    :   jsobject  	# JsonObjectValue
    |   jsarray  	# JsonArrayValue
    |   DSTRING		# JsonAtom
    |   number      # JsonAtom
    |   TRUE		# JsonAtom
    |   FALSE		# JsonAtom
    |   NULL		# JsonAtom ;


comment : COMMENT string ;

duration : INT time_unit ;

time_unit : (SECONDS | MINUTES | HOURS | DAYS) ;

/* this is a parser rule to allow space between '-' and digits */
number : '-'? (INT | FLOAT | NUMBER);

string : STRING | DSTRING ;

/*
 * Identifiers
 */

id_list : id (COMMA id)* ;

id :
    (ACCOUNT | ADD | ADMIN | ALL | ALTER | ANCESTORS | AND | ANY_T |
     ANYATOMIC_T | ANYJSONATOMIC_T | ANYRECORD_T | AS | ASC |
     BY | CASE | CAST | COMMENT | COUNT | CREATE |
     DAYS | DECLARE | DEFAULT | DESC | DESCENDANTS | DESCRIBE | DROP |
     ELEMENTOF | ELSE | END | ES_SHARDS | ES_REPLICAS | EXISTS | EXTRACT |
     FIRST | FROM | FULLTEXT | GRANT | GROUP | HOURS |
     IDENTIFIED | IF | INDEX | INDEXES | IS | JSON |
     KEY | KEYOF | KEYS |
     LIFETIME | LAST | LIMIT | LOCK | MINUTES | MODIFY |
     NESTED | NOT | NULLS |
     OF | OFFSET | ON | OR | ORDER | OVERRIDE |
     PASSWORD | PRIMARY | PUT | REMOVE | RETURNING | ROLE | ROLES | REVOKE |
     SECONDS | SELECT | SEQ_TRANSFORM | SET | SHARD | SHOW |
     TABLE | TABLES | THEN | TO | TTL | TYPE |
     UNLOCK | UPDATE | USER | USERS | USING | VALUES | WHEN | WHERE |
     ARRAY_T |  BINARY_T | BOOLEAN_T | DOUBLE_T | ENUM_T | FLOAT_T |
     LONG_T | INTEGER_T | MAP_T | NUMBER_T | RECORD_T | STRING_T | TIMESTAMP_T |
     SCALAR_T |
     ID) |
     BAD_ID
     {
        notifyErrorListeners("Identifiers must start with a letter: " + $text);
     }
  ;


/******************************************************************************
 * Lexical rules (start with uppercase)
 *
 * Keywords need to be case-insensitive, which makes their lexical rules a bit
 * more complicated than simple strings.
 ******************************************************************************/

/*
 * Keywords
 */

ACCOUNT : [Aa][Cc][Cc][Oo][Uu][Nn][Tt] ;

ADD : [Aa][Dd][Dd] ;

ADMIN : [Aa][Dd][Mm][Ii][Nn] ;

ALL : [Aa][Ll][Ll] ;

ALTER : [Aa][Ll][Tt][Ee][Rr] ;

ANCESTORS : [Aa][Nn][Cc][Ee][Ss][Tt][Oo][Rr][Ss] ;

AND : [Aa][Nn][Dd] ;

AS : [Aa][Ss] ;

ASC : [Aa][Ss][Cc];

BY : [Bb][Yy] ;

CASE : [Cc][Aa][Ss][Ee] ;

CASCADE : [Cc][Aa][Ss][Cc][Aa][Dd][Ee] ;

CAST : [Cc][Aa][Ss][Tt] ;

COMMENT : [Cc][Oo][Mm][Mm][Ee][Nn][Tt] ;

COUNT : 'count' ;

CREATE : [Cc][Rr][Ee][Aa][Tt][Ee] ;

DAYS : ([Dd] | [Dd][Aa][Yy][Ss]) ;

DECLARE : [Dd][Ee][Cc][Ll][Aa][Rr][Ee] ;

DEFAULT : [Dd][Ee][Ff][Aa][Uu][Ll][Tt] ;

DESC : [Dd][Ee][Ss][Cc] ;

DESCENDANTS : [Dd][Ee][Ss][Cc][Ee][Nn][Dd][Aa][Nn][Tt][Ss] ;

DESCRIBE : [Dd][Ee][Ss][Cc][Rr][Ii][Bb][Ee] ;

DROP : [Dd][Rr][Oo][Pp] ;

ELEMENTOF : [Ee][Ll][Ee][Mm][Ee][Nn][Tt][Oo][Ff] ;

ELSE : [Ee][Ll][Ss][Ee] ;

END : [Ee][Nn][Dd] ;

ES_SHARDS : [Ee][Ss] UNDER [Ss][Hh][Aa][Rr][Dd][Ss] ;

ES_REPLICAS : [Ee][Ss] UNDER [Rr][Ee][Pp][Ll][Ii][Cc][Aa][Ss] ;

EXISTS : [Ee][Xx][Ii][Ss][Tt][Ss] ;

EXTRACT: [Ee][Xx][Tt][Rr][Aa][Cc][Tt] ;

FIRST : [Ff][Ii][Rr][Ss][Tt] ;

FORCE_INDEX : FORCE UNDER INDEX;

FORCE_PRIMARY_INDEX : FORCE UNDER PRIMARY UNDER INDEX;

FROM : [Ff][Rr][Oo][Mm] ;

FULLTEXT : [Ff][Uu][Ll][Ll][Tt][Ee][Xx][Tt] ;

GRANT : [Gg][Rr][Aa][Nn][Tt] ;

GROUP : [Gg][Rr][Oo][Uu][Pp] ;

HOURS : ([Hh] | [Hh][Oo][Uu][Rr][Ss]) ;

IDENTIFIED : [Ii][Dd][Ee][Nn][Tt][Ii][Ff][Ii][Ee][Dd] ;

IF : [Ii][Ff] ;

INDEX : [Ii][Nn][Dd][Ee][Xx] ;

INDEXES : [Ii][Nn][Dd][Ee][Xx][Ee][Ss] ;

IS : [Ii][Ss];

JSON : [Jj][Ss][Oo][Nn] ;

KEY : [Kk][Ee][Yy] ;

KEYOF : [Kk][Ee][Yy][Oo][Ff] ;

KEYS : [Kk][Ee][Yy][Ss] ;

LAST : [Ll][Aa][Ss][Tt] ;

LIFETIME : [Ll][Ii][Ff][Ee][Tt][Ii][Mm][Ee] ;

LIMIT : [Ll][Ii][Mm][Ii][Tt] ;

LOCK : [Ll][Oo][Cc][Kk] ;

MINUTES : ([Mm] | [Mm][Ii][Nn][Uu][Tt][Ee][Ss]) ;

MODIFY : [Mm][Oo][Dd][Ii][Ff][Yy] ;

NESTED : [Nn][Ee][Ss][Tt][Ee][Dd] ;

NOT : [Nn][Oo][Tt] ;

NULLS : [Nn][Uu][Ll][Ll][Ss] ;

OFFSET : [Oo][Ff][Ff][Ss][Ee][Tt] ;

OF : [Oo][Ff] ;

ON : [Oo][Nn] ;

ONLY : [Oo][Nn][Ll][Yy] ;

OR : [Oo][Rr] ;

ORDER : [Oo][Rr][Dd][Ee][Rr];

OVERRIDE : [Oo][Vv][Ee][Rr][Rr][Ii][Dd][Ee] ;

PASSWORD : [Pp][Aa][Ss][Ss][Ww][Oo][Rr][Dd] ;

PREFER_INDEXES: PREFER UNDER INDEXES;

PREFER_PRIMARY_INDEX : PREFER UNDER PRIMARY UNDER INDEX;

PRIMARY : [Pp][Rr][Ii][Mm][Aa][Rr][Yy] ;

PUT : [Pp][Uu][Tt] ;

REMOVE : [Rr][Ee][Mm][Oo][Vv][Ee] ;

RETURNING : [Rr][Ee][Tt][Uu][Rr][Nn][Ii][Nn][Gg] ;

REVOKE : [Rr][Ee][Vv][Oo][Kk][Ee] ;

ROLE : [Rr][Oo][Ll][Ee] ;

ROLES : [Rr][Oo][Ll][Ee][Ss] ;

SECONDS : ([Ss] | [Ss][Ee][Cc][Oo][Nn][Dd][Ss]) ;

SELECT : [Ss][Ee][Ll][Ee][Cc][Tt] ;

SEQ_TRANSFORM : 'seq_transform' ;

SET : [Ss][Ee][Tt] ;

SHARD : [Ss][Hh][Aa][Rr][Dd] ;

SHOW : [Ss][Hh][Oo][Ww] ;

TABLE : [Tt][Aa][Bb][Ll][Ee] ;

TABLES : [Tt][Aa][Bb][Ll][Ee][Ss] ;

THEN : [Tt][Hh][Ee][Nn] ;

TO : [Tt][Oo] ;

TTL : [Tt][Tt][Ll];

TYPE : [Tt][Yy][Pp][Ee] ;

UNLOCK : [Uu][Nn][Ll][Oo][Cc][Kk] ;

UPDATE : [Uu][Pp][Dd][Aa][Tt][Ee] ;

USER : [Uu][Ss][Ee][Rr] ;

USERS : [Uu][Ss][Ee][Rr][Ss] ;

USING: [Uu][Ss][Ii][Nn][Gg];

VALUES : [Vv][Aa][Ll][Uu][Ee][Ss] ;

WHEN : [Ww][Hh][Ee][Nn] ;

WHERE : [Ww][Hh][Ee][Rr][Ee] ;


/* multi-word tokens */

ALL_PRIVILEGES : ALL WS+ PRIVILEGES ;

IDENTIFIED_EXTERNALLY : IDENTIFIED WS+ EXTERNALLY ;

PASSWORD_EXPIRE : PASSWORD WS+ EXPIRE ;

RETAIN_CURRENT_PASSWORD : RETAIN WS+ CURRENT WS+ PASSWORD ;

CLEAR_RETAINED_PASSWORD : CLEAR WS+ RETAINED WS+ PASSWORD;

/* types */
ARRAY_T : [Aa][Rr][Rr][Aa][Yy] ;

BINARY_T : [Bb][Ii][Nn][Aa][Rr][Yy] ;

BOOLEAN_T : [Bb][Oo][Oo][Ll][Ee][Aa][Nn] ;

DOUBLE_T : [Dd][Oo][Uu][Bb][Ll][Ee] ;

ENUM_T : [Ee][Nn][Uu][Mm] ;

FLOAT_T : [Ff][Ll][Oo][Aa][Tt] ;

INTEGER_T : [Ii][Nn][Tt][Ee][Gg][Ee][Rr] ;

LONG_T : [Ll][Oo][Nn][Gg] ;

MAP_T : [Mm][Aa][Pp] ;

NUMBER_T : [Nn][Uu][Mm][Bb][Ee][Rr] ;

RECORD_T : [Rr][Ee][Cc][Oo][Rr][Dd] ;

STRING_T : [Ss][Tt][Rr][Ii][Nn][Gg] ;

TIMESTAMP_T : [Tt][Ii][Mm][Ee][Ss][Tt][Aa][Mm][Pp] ;

ANY_T : [Aa][Nn][Yy] ;

ANYATOMIC_T : [Aa][Nn][Yy][Aa][Tt][Oo][Mm][Ii][Cc] ;

ANYJSONATOMIC_T : [Aa][Nn][Yy][Jj][Ss][Oo][Nn][Aa][Tt][Oo][Mm][Ii][Cc] ;

ANYRECORD_T : [Aa][Nn][Yy][Rr][Ee][Cc][Oo][Rr][Dd] ;


/* used to define a scalar (atomic type) index */
SCALAR_T : [Ss][Cc][Aa][Ll][Aa][Rr] ;

/*
 * Punctuation marks
 */
SEMI : ';' ;
COMMA : ',' ;
COLON : ':' ;
LP : '(' ;
RP : ')' ;
LBRACK : '[' ;
RBRACK : ']' ;
LBRACE : '{' ;
RBRACE : '}' ;
STAR : '*' ;
DOT : '.' ;
DOLLAR : '$' ;
QUESTION_MARK : '?' ;

/*
 * Operators
 */
LT : '<' ;
LTE : '<=' ;
GT : '>' ;
GTE : '>=' ;
EQ : '=' ;
NEQ : '!=' ;

LT_ANY : '<'[Aa][Nn][Yy] ;
LTE_ANY : '<='[Aa][Nn][Yy] ;
GT_ANY : '>'[Aa][Nn][Yy] ;
GTE_ANY : '>='[Aa][Nn][Yy] ;
EQ_ANY : '='[Aa][Nn][Yy] ;
NEQ_ANY : '!='[Aa][Nn][Yy] ;

PLUS : '+' ;
MINUS : '-' ;
//MULT : '*' ; STAR already defined
DIV : '/' ;

/*
 * LITERALS
 */

NULL : [Nn][Uu][Ll][Ll] ;

FALSE : [Ff][Aa][Ll][Ss][Ee] ;

TRUE : [Tt][Rr][Uu][Ee] ;

INT : DIGIT+ ; // translated to Integer or Long item

FLOAT : ( DIGIT* '.' DIGIT+ ([Ee] [+-]? DIGIT+)? ) |
        ( DIGIT+ [Ee] [+-]? DIGIT+ ) ;

// For number use number parser rule instead of this lexer rule.
NUMBER : (INT | FLOAT) ('n'|'N');

DSTRING : '"' (DSTR_ESC | .)*? '"' ;

STRING : '\'' (ESC | .)*? '\'' ;

SYSTEM_TABLE_NAME : [S][Y][S][$]ID ;

/*
 * Identifiers (MUST come after all the keywords and literals defined above)
 */

ID : ALPHA (ALPHA | DIGIT | UNDER)* ;

/* A special token to catch badly-formed identifiers. */
BAD_ID : (DIGIT | UNDER) (ALPHA | DIGIT | UNDER)* ;


/*
 * Skip whitespace, don't pass to parser.
 */
WS : (' ' | '\t' | '\r' | '\n')+ -> skip ;

/*
 * Comments.  3 styles.
 */
C_COMMENT : '/*' ~[+] .*? '*/' -> skip ;

LINE_COMMENT : '//' ~[\r\n]* -> skip ;

LINE_COMMENT1 : '#' ~[\r\n]* -> skip ;

/*
 * Add a token that will match anything.  The resulting error will be
 * more usable this way.
 */
UnrecognizedToken : . ;

/*
 * fragments can only be used in other lexical rules and are not tokens
 */

fragment ALPHA : 'a'..'z'|'A'..'Z' ;

fragment DIGIT : '0'..'9' ;

fragment DSTR_ESC : '\\' (["\\/bfnrt] | UNICODE) ; /* " */

fragment ESC : '\\' (['\\/bfnrt] | UNICODE) ;

fragment HEX : [0-9a-fA-F] ;

fragment UNDER : '_';

fragment UNICODE : 'u' HEX HEX HEX HEX ;

fragment CLEAR : [Cc][Ll][Ee][Aa][Rr] ;

fragment CURRENT : [Cc][Uu][Rr][Rr][Ee][Nn][Tt] ;

fragment EXPIRE : [Ee][Xx][Pp][Ii][Rr][Ee] ;

fragment EXTERNALLY : [Ee][Xx][Tt][Ee][Rr][Nn][Aa][Ll][Ll][Yy] ;

fragment FORCE : [Ff][Oo][Rr][Cc][Ee] ;

fragment PREFER : [Pp][Rr][Ee][Ff][Ee][Rr] ;

fragment PRIVILEGES : [Pp][Rr][Ii][Vv][Ii][Ll][Ee][Gg][Ee][Ss] ;

fragment RETAIN : [Rr][Ee][Tt][Aa][Ii][Nn] ;

fragment RETAINED : [Rr][Ee][Tt][Aa][Ii][Nn][Ee][Dd] ;
