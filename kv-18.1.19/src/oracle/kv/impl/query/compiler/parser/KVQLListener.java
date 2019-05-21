// Generated from /home/markos/KVS/joins/kvstore/src/oracle/kv/impl/query/compiler/parser/KVQL.g4 by ANTLR 4.7.1
package oracle.kv.impl.query.compiler.parser;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link KVQLParser}.
 */
public interface KVQLListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link KVQLParser#parse}.
	 * @param ctx the parse tree
	 */
	void enterParse(KVQLParser.ParseContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#parse}.
	 * @param ctx the parse tree
	 */
	void exitParse(KVQLParser.ParseContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatement(KVQLParser.StatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatement(KVQLParser.StatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#query}.
	 * @param ctx the parse tree
	 */
	void enterQuery(KVQLParser.QueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#query}.
	 * @param ctx the parse tree
	 */
	void exitQuery(KVQLParser.QueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#prolog}.
	 * @param ctx the parse tree
	 */
	void enterProlog(KVQLParser.PrologContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#prolog}.
	 * @param ctx the parse tree
	 */
	void exitProlog(KVQLParser.PrologContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#var_decl}.
	 * @param ctx the parse tree
	 */
	void enterVar_decl(KVQLParser.Var_declContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#var_decl}.
	 * @param ctx the parse tree
	 */
	void exitVar_decl(KVQLParser.Var_declContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#var_name}.
	 * @param ctx the parse tree
	 */
	void enterVar_name(KVQLParser.Var_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#var_name}.
	 * @param ctx the parse tree
	 */
	void exitVar_name(KVQLParser.Var_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterExpr(KVQLParser.ExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitExpr(KVQLParser.ExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#sfw_expr}.
	 * @param ctx the parse tree
	 */
	void enterSfw_expr(KVQLParser.Sfw_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#sfw_expr}.
	 * @param ctx the parse tree
	 */
	void exitSfw_expr(KVQLParser.Sfw_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#from_clause}.
	 * @param ctx the parse tree
	 */
	void enterFrom_clause(KVQLParser.From_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#from_clause}.
	 * @param ctx the parse tree
	 */
	void exitFrom_clause(KVQLParser.From_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#nested_tables}.
	 * @param ctx the parse tree
	 */
	void enterNested_tables(KVQLParser.Nested_tablesContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#nested_tables}.
	 * @param ctx the parse tree
	 */
	void exitNested_tables(KVQLParser.Nested_tablesContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#ancestor_tables}.
	 * @param ctx the parse tree
	 */
	void enterAncestor_tables(KVQLParser.Ancestor_tablesContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#ancestor_tables}.
	 * @param ctx the parse tree
	 */
	void exitAncestor_tables(KVQLParser.Ancestor_tablesContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#descendant_tables}.
	 * @param ctx the parse tree
	 */
	void enterDescendant_tables(KVQLParser.Descendant_tablesContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#descendant_tables}.
	 * @param ctx the parse tree
	 */
	void exitDescendant_tables(KVQLParser.Descendant_tablesContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#from_table}.
	 * @param ctx the parse tree
	 */
	void enterFrom_table(KVQLParser.From_tableContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#from_table}.
	 * @param ctx the parse tree
	 */
	void exitFrom_table(KVQLParser.From_tableContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#aliased_table_name}.
	 * @param ctx the parse tree
	 */
	void enterAliased_table_name(KVQLParser.Aliased_table_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#aliased_table_name}.
	 * @param ctx the parse tree
	 */
	void exitAliased_table_name(KVQLParser.Aliased_table_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#tab_alias}.
	 * @param ctx the parse tree
	 */
	void enterTab_alias(KVQLParser.Tab_aliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#tab_alias}.
	 * @param ctx the parse tree
	 */
	void exitTab_alias(KVQLParser.Tab_aliasContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#where_clause}.
	 * @param ctx the parse tree
	 */
	void enterWhere_clause(KVQLParser.Where_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#where_clause}.
	 * @param ctx the parse tree
	 */
	void exitWhere_clause(KVQLParser.Where_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#select_clause}.
	 * @param ctx the parse tree
	 */
	void enterSelect_clause(KVQLParser.Select_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#select_clause}.
	 * @param ctx the parse tree
	 */
	void exitSelect_clause(KVQLParser.Select_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#select_list}.
	 * @param ctx the parse tree
	 */
	void enterSelect_list(KVQLParser.Select_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#select_list}.
	 * @param ctx the parse tree
	 */
	void exitSelect_list(KVQLParser.Select_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#hints}.
	 * @param ctx the parse tree
	 */
	void enterHints(KVQLParser.HintsContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#hints}.
	 * @param ctx the parse tree
	 */
	void exitHints(KVQLParser.HintsContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#hint}.
	 * @param ctx the parse tree
	 */
	void enterHint(KVQLParser.HintContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#hint}.
	 * @param ctx the parse tree
	 */
	void exitHint(KVQLParser.HintContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#col_alias}.
	 * @param ctx the parse tree
	 */
	void enterCol_alias(KVQLParser.Col_aliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#col_alias}.
	 * @param ctx the parse tree
	 */
	void exitCol_alias(KVQLParser.Col_aliasContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#orderby_clause}.
	 * @param ctx the parse tree
	 */
	void enterOrderby_clause(KVQLParser.Orderby_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#orderby_clause}.
	 * @param ctx the parse tree
	 */
	void exitOrderby_clause(KVQLParser.Orderby_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#sort_spec}.
	 * @param ctx the parse tree
	 */
	void enterSort_spec(KVQLParser.Sort_specContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#sort_spec}.
	 * @param ctx the parse tree
	 */
	void exitSort_spec(KVQLParser.Sort_specContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#groupby_clause}.
	 * @param ctx the parse tree
	 */
	void enterGroupby_clause(KVQLParser.Groupby_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#groupby_clause}.
	 * @param ctx the parse tree
	 */
	void exitGroupby_clause(KVQLParser.Groupby_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#limit_clause}.
	 * @param ctx the parse tree
	 */
	void enterLimit_clause(KVQLParser.Limit_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#limit_clause}.
	 * @param ctx the parse tree
	 */
	void exitLimit_clause(KVQLParser.Limit_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#offset_clause}.
	 * @param ctx the parse tree
	 */
	void enterOffset_clause(KVQLParser.Offset_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#offset_clause}.
	 * @param ctx the parse tree
	 */
	void exitOffset_clause(KVQLParser.Offset_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#or_expr}.
	 * @param ctx the parse tree
	 */
	void enterOr_expr(KVQLParser.Or_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#or_expr}.
	 * @param ctx the parse tree
	 */
	void exitOr_expr(KVQLParser.Or_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#and_expr}.
	 * @param ctx the parse tree
	 */
	void enterAnd_expr(KVQLParser.And_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#and_expr}.
	 * @param ctx the parse tree
	 */
	void exitAnd_expr(KVQLParser.And_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#not_expr}.
	 * @param ctx the parse tree
	 */
	void enterNot_expr(KVQLParser.Not_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#not_expr}.
	 * @param ctx the parse tree
	 */
	void exitNot_expr(KVQLParser.Not_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#is_null_expr}.
	 * @param ctx the parse tree
	 */
	void enterIs_null_expr(KVQLParser.Is_null_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#is_null_expr}.
	 * @param ctx the parse tree
	 */
	void exitIs_null_expr(KVQLParser.Is_null_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#cond_expr}.
	 * @param ctx the parse tree
	 */
	void enterCond_expr(KVQLParser.Cond_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#cond_expr}.
	 * @param ctx the parse tree
	 */
	void exitCond_expr(KVQLParser.Cond_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#exists_expr}.
	 * @param ctx the parse tree
	 */
	void enterExists_expr(KVQLParser.Exists_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#exists_expr}.
	 * @param ctx the parse tree
	 */
	void exitExists_expr(KVQLParser.Exists_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#is_of_type_expr}.
	 * @param ctx the parse tree
	 */
	void enterIs_of_type_expr(KVQLParser.Is_of_type_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#is_of_type_expr}.
	 * @param ctx the parse tree
	 */
	void exitIs_of_type_expr(KVQLParser.Is_of_type_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#comp_expr}.
	 * @param ctx the parse tree
	 */
	void enterComp_expr(KVQLParser.Comp_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#comp_expr}.
	 * @param ctx the parse tree
	 */
	void exitComp_expr(KVQLParser.Comp_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#comp_op}.
	 * @param ctx the parse tree
	 */
	void enterComp_op(KVQLParser.Comp_opContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#comp_op}.
	 * @param ctx the parse tree
	 */
	void exitComp_op(KVQLParser.Comp_opContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#any_op}.
	 * @param ctx the parse tree
	 */
	void enterAny_op(KVQLParser.Any_opContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#any_op}.
	 * @param ctx the parse tree
	 */
	void exitAny_op(KVQLParser.Any_opContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#add_expr}.
	 * @param ctx the parse tree
	 */
	void enterAdd_expr(KVQLParser.Add_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#add_expr}.
	 * @param ctx the parse tree
	 */
	void exitAdd_expr(KVQLParser.Add_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#multiply_expr}.
	 * @param ctx the parse tree
	 */
	void enterMultiply_expr(KVQLParser.Multiply_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#multiply_expr}.
	 * @param ctx the parse tree
	 */
	void exitMultiply_expr(KVQLParser.Multiply_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#unary_expr}.
	 * @param ctx the parse tree
	 */
	void enterUnary_expr(KVQLParser.Unary_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#unary_expr}.
	 * @param ctx the parse tree
	 */
	void exitUnary_expr(KVQLParser.Unary_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#path_expr}.
	 * @param ctx the parse tree
	 */
	void enterPath_expr(KVQLParser.Path_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#path_expr}.
	 * @param ctx the parse tree
	 */
	void exitPath_expr(KVQLParser.Path_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#map_step}.
	 * @param ctx the parse tree
	 */
	void enterMap_step(KVQLParser.Map_stepContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#map_step}.
	 * @param ctx the parse tree
	 */
	void exitMap_step(KVQLParser.Map_stepContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#map_field_step}.
	 * @param ctx the parse tree
	 */
	void enterMap_field_step(KVQLParser.Map_field_stepContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#map_field_step}.
	 * @param ctx the parse tree
	 */
	void exitMap_field_step(KVQLParser.Map_field_stepContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#map_filter_step}.
	 * @param ctx the parse tree
	 */
	void enterMap_filter_step(KVQLParser.Map_filter_stepContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#map_filter_step}.
	 * @param ctx the parse tree
	 */
	void exitMap_filter_step(KVQLParser.Map_filter_stepContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#array_step}.
	 * @param ctx the parse tree
	 */
	void enterArray_step(KVQLParser.Array_stepContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#array_step}.
	 * @param ctx the parse tree
	 */
	void exitArray_step(KVQLParser.Array_stepContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#array_slice_step}.
	 * @param ctx the parse tree
	 */
	void enterArray_slice_step(KVQLParser.Array_slice_stepContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#array_slice_step}.
	 * @param ctx the parse tree
	 */
	void exitArray_slice_step(KVQLParser.Array_slice_stepContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#array_filter_step}.
	 * @param ctx the parse tree
	 */
	void enterArray_filter_step(KVQLParser.Array_filter_stepContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#array_filter_step}.
	 * @param ctx the parse tree
	 */
	void exitArray_filter_step(KVQLParser.Array_filter_stepContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#primary_expr}.
	 * @param ctx the parse tree
	 */
	void enterPrimary_expr(KVQLParser.Primary_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#primary_expr}.
	 * @param ctx the parse tree
	 */
	void exitPrimary_expr(KVQLParser.Primary_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#column_ref}.
	 * @param ctx the parse tree
	 */
	void enterColumn_ref(KVQLParser.Column_refContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#column_ref}.
	 * @param ctx the parse tree
	 */
	void exitColumn_ref(KVQLParser.Column_refContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#const_expr}.
	 * @param ctx the parse tree
	 */
	void enterConst_expr(KVQLParser.Const_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#const_expr}.
	 * @param ctx the parse tree
	 */
	void exitConst_expr(KVQLParser.Const_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#var_ref}.
	 * @param ctx the parse tree
	 */
	void enterVar_ref(KVQLParser.Var_refContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#var_ref}.
	 * @param ctx the parse tree
	 */
	void exitVar_ref(KVQLParser.Var_refContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#array_constructor}.
	 * @param ctx the parse tree
	 */
	void enterArray_constructor(KVQLParser.Array_constructorContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#array_constructor}.
	 * @param ctx the parse tree
	 */
	void exitArray_constructor(KVQLParser.Array_constructorContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#map_constructor}.
	 * @param ctx the parse tree
	 */
	void enterMap_constructor(KVQLParser.Map_constructorContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#map_constructor}.
	 * @param ctx the parse tree
	 */
	void exitMap_constructor(KVQLParser.Map_constructorContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#transform_expr}.
	 * @param ctx the parse tree
	 */
	void enterTransform_expr(KVQLParser.Transform_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#transform_expr}.
	 * @param ctx the parse tree
	 */
	void exitTransform_expr(KVQLParser.Transform_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#transform_input_expr}.
	 * @param ctx the parse tree
	 */
	void enterTransform_input_expr(KVQLParser.Transform_input_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#transform_input_expr}.
	 * @param ctx the parse tree
	 */
	void exitTransform_input_expr(KVQLParser.Transform_input_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#func_call}.
	 * @param ctx the parse tree
	 */
	void enterFunc_call(KVQLParser.Func_callContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#func_call}.
	 * @param ctx the parse tree
	 */
	void exitFunc_call(KVQLParser.Func_callContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#count_star}.
	 * @param ctx the parse tree
	 */
	void enterCount_star(KVQLParser.Count_starContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#count_star}.
	 * @param ctx the parse tree
	 */
	void exitCount_star(KVQLParser.Count_starContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#case_expr}.
	 * @param ctx the parse tree
	 */
	void enterCase_expr(KVQLParser.Case_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#case_expr}.
	 * @param ctx the parse tree
	 */
	void exitCase_expr(KVQLParser.Case_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#cast_expr}.
	 * @param ctx the parse tree
	 */
	void enterCast_expr(KVQLParser.Cast_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#cast_expr}.
	 * @param ctx the parse tree
	 */
	void exitCast_expr(KVQLParser.Cast_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#parenthesized_expr}.
	 * @param ctx the parse tree
	 */
	void enterParenthesized_expr(KVQLParser.Parenthesized_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#parenthesized_expr}.
	 * @param ctx the parse tree
	 */
	void exitParenthesized_expr(KVQLParser.Parenthesized_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#extract_expr}.
	 * @param ctx the parse tree
	 */
	void enterExtract_expr(KVQLParser.Extract_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#extract_expr}.
	 * @param ctx the parse tree
	 */
	void exitExtract_expr(KVQLParser.Extract_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#update_statement}.
	 * @param ctx the parse tree
	 */
	void enterUpdate_statement(KVQLParser.Update_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#update_statement}.
	 * @param ctx the parse tree
	 */
	void exitUpdate_statement(KVQLParser.Update_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#returning_clause}.
	 * @param ctx the parse tree
	 */
	void enterReturning_clause(KVQLParser.Returning_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#returning_clause}.
	 * @param ctx the parse tree
	 */
	void exitReturning_clause(KVQLParser.Returning_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#update_clause}.
	 * @param ctx the parse tree
	 */
	void enterUpdate_clause(KVQLParser.Update_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#update_clause}.
	 * @param ctx the parse tree
	 */
	void exitUpdate_clause(KVQLParser.Update_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#set_clause}.
	 * @param ctx the parse tree
	 */
	void enterSet_clause(KVQLParser.Set_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#set_clause}.
	 * @param ctx the parse tree
	 */
	void exitSet_clause(KVQLParser.Set_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#add_clause}.
	 * @param ctx the parse tree
	 */
	void enterAdd_clause(KVQLParser.Add_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#add_clause}.
	 * @param ctx the parse tree
	 */
	void exitAdd_clause(KVQLParser.Add_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#put_clause}.
	 * @param ctx the parse tree
	 */
	void enterPut_clause(KVQLParser.Put_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#put_clause}.
	 * @param ctx the parse tree
	 */
	void exitPut_clause(KVQLParser.Put_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#remove_clause}.
	 * @param ctx the parse tree
	 */
	void enterRemove_clause(KVQLParser.Remove_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#remove_clause}.
	 * @param ctx the parse tree
	 */
	void exitRemove_clause(KVQLParser.Remove_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#ttl_clause}.
	 * @param ctx the parse tree
	 */
	void enterTtl_clause(KVQLParser.Ttl_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#ttl_clause}.
	 * @param ctx the parse tree
	 */
	void exitTtl_clause(KVQLParser.Ttl_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#target_expr}.
	 * @param ctx the parse tree
	 */
	void enterTarget_expr(KVQLParser.Target_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#target_expr}.
	 * @param ctx the parse tree
	 */
	void exitTarget_expr(KVQLParser.Target_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#pos_expr}.
	 * @param ctx the parse tree
	 */
	void enterPos_expr(KVQLParser.Pos_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#pos_expr}.
	 * @param ctx the parse tree
	 */
	void exitPos_expr(KVQLParser.Pos_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#quantified_type_def}.
	 * @param ctx the parse tree
	 */
	void enterQuantified_type_def(KVQLParser.Quantified_type_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#quantified_type_def}.
	 * @param ctx the parse tree
	 */
	void exitQuantified_type_def(KVQLParser.Quantified_type_defContext ctx);
	/**
	 * Enter a parse tree produced by the {@code Binary}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void enterBinary(KVQLParser.BinaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code Binary}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void exitBinary(KVQLParser.BinaryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code Array}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void enterArray(KVQLParser.ArrayContext ctx);
	/**
	 * Exit a parse tree produced by the {@code Array}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void exitArray(KVQLParser.ArrayContext ctx);
	/**
	 * Enter a parse tree produced by the {@code Boolean}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void enterBoolean(KVQLParser.BooleanContext ctx);
	/**
	 * Exit a parse tree produced by the {@code Boolean}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void exitBoolean(KVQLParser.BooleanContext ctx);
	/**
	 * Enter a parse tree produced by the {@code Enum}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void enterEnum(KVQLParser.EnumContext ctx);
	/**
	 * Exit a parse tree produced by the {@code Enum}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void exitEnum(KVQLParser.EnumContext ctx);
	/**
	 * Enter a parse tree produced by the {@code Float}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void enterFloat(KVQLParser.FloatContext ctx);
	/**
	 * Exit a parse tree produced by the {@code Float}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void exitFloat(KVQLParser.FloatContext ctx);
	/**
	 * Enter a parse tree produced by the {@code Int}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void enterInt(KVQLParser.IntContext ctx);
	/**
	 * Exit a parse tree produced by the {@code Int}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void exitInt(KVQLParser.IntContext ctx);
	/**
	 * Enter a parse tree produced by the {@code JSON}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void enterJSON(KVQLParser.JSONContext ctx);
	/**
	 * Exit a parse tree produced by the {@code JSON}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void exitJSON(KVQLParser.JSONContext ctx);
	/**
	 * Enter a parse tree produced by the {@code Map}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void enterMap(KVQLParser.MapContext ctx);
	/**
	 * Exit a parse tree produced by the {@code Map}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void exitMap(KVQLParser.MapContext ctx);
	/**
	 * Enter a parse tree produced by the {@code Record}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void enterRecord(KVQLParser.RecordContext ctx);
	/**
	 * Exit a parse tree produced by the {@code Record}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void exitRecord(KVQLParser.RecordContext ctx);
	/**
	 * Enter a parse tree produced by the {@code StringT}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void enterStringT(KVQLParser.StringTContext ctx);
	/**
	 * Exit a parse tree produced by the {@code StringT}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void exitStringT(KVQLParser.StringTContext ctx);
	/**
	 * Enter a parse tree produced by the {@code Timestamp}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void enterTimestamp(KVQLParser.TimestampContext ctx);
	/**
	 * Exit a parse tree produced by the {@code Timestamp}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void exitTimestamp(KVQLParser.TimestampContext ctx);
	/**
	 * Enter a parse tree produced by the {@code Any}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void enterAny(KVQLParser.AnyContext ctx);
	/**
	 * Exit a parse tree produced by the {@code Any}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void exitAny(KVQLParser.AnyContext ctx);
	/**
	 * Enter a parse tree produced by the {@code AnyAtomic}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void enterAnyAtomic(KVQLParser.AnyAtomicContext ctx);
	/**
	 * Exit a parse tree produced by the {@code AnyAtomic}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void exitAnyAtomic(KVQLParser.AnyAtomicContext ctx);
	/**
	 * Enter a parse tree produced by the {@code AnyJsonAtomic}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void enterAnyJsonAtomic(KVQLParser.AnyJsonAtomicContext ctx);
	/**
	 * Exit a parse tree produced by the {@code AnyJsonAtomic}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void exitAnyJsonAtomic(KVQLParser.AnyJsonAtomicContext ctx);
	/**
	 * Enter a parse tree produced by the {@code AnyRecord}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void enterAnyRecord(KVQLParser.AnyRecordContext ctx);
	/**
	 * Exit a parse tree produced by the {@code AnyRecord}
	 * labeled alternative in {@link KVQLParser#type_def}.
	 * @param ctx the parse tree
	 */
	void exitAnyRecord(KVQLParser.AnyRecordContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#record_def}.
	 * @param ctx the parse tree
	 */
	void enterRecord_def(KVQLParser.Record_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#record_def}.
	 * @param ctx the parse tree
	 */
	void exitRecord_def(KVQLParser.Record_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#field_def}.
	 * @param ctx the parse tree
	 */
	void enterField_def(KVQLParser.Field_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#field_def}.
	 * @param ctx the parse tree
	 */
	void exitField_def(KVQLParser.Field_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#default_def}.
	 * @param ctx the parse tree
	 */
	void enterDefault_def(KVQLParser.Default_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#default_def}.
	 * @param ctx the parse tree
	 */
	void exitDefault_def(KVQLParser.Default_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#default_value}.
	 * @param ctx the parse tree
	 */
	void enterDefault_value(KVQLParser.Default_valueContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#default_value}.
	 * @param ctx the parse tree
	 */
	void exitDefault_value(KVQLParser.Default_valueContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#not_null}.
	 * @param ctx the parse tree
	 */
	void enterNot_null(KVQLParser.Not_nullContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#not_null}.
	 * @param ctx the parse tree
	 */
	void exitNot_null(KVQLParser.Not_nullContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#map_def}.
	 * @param ctx the parse tree
	 */
	void enterMap_def(KVQLParser.Map_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#map_def}.
	 * @param ctx the parse tree
	 */
	void exitMap_def(KVQLParser.Map_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#array_def}.
	 * @param ctx the parse tree
	 */
	void enterArray_def(KVQLParser.Array_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#array_def}.
	 * @param ctx the parse tree
	 */
	void exitArray_def(KVQLParser.Array_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#integer_def}.
	 * @param ctx the parse tree
	 */
	void enterInteger_def(KVQLParser.Integer_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#integer_def}.
	 * @param ctx the parse tree
	 */
	void exitInteger_def(KVQLParser.Integer_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#json_def}.
	 * @param ctx the parse tree
	 */
	void enterJson_def(KVQLParser.Json_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#json_def}.
	 * @param ctx the parse tree
	 */
	void exitJson_def(KVQLParser.Json_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#float_def}.
	 * @param ctx the parse tree
	 */
	void enterFloat_def(KVQLParser.Float_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#float_def}.
	 * @param ctx the parse tree
	 */
	void exitFloat_def(KVQLParser.Float_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#string_def}.
	 * @param ctx the parse tree
	 */
	void enterString_def(KVQLParser.String_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#string_def}.
	 * @param ctx the parse tree
	 */
	void exitString_def(KVQLParser.String_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#enum_def}.
	 * @param ctx the parse tree
	 */
	void enterEnum_def(KVQLParser.Enum_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#enum_def}.
	 * @param ctx the parse tree
	 */
	void exitEnum_def(KVQLParser.Enum_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#boolean_def}.
	 * @param ctx the parse tree
	 */
	void enterBoolean_def(KVQLParser.Boolean_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#boolean_def}.
	 * @param ctx the parse tree
	 */
	void exitBoolean_def(KVQLParser.Boolean_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#binary_def}.
	 * @param ctx the parse tree
	 */
	void enterBinary_def(KVQLParser.Binary_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#binary_def}.
	 * @param ctx the parse tree
	 */
	void exitBinary_def(KVQLParser.Binary_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#timestamp_def}.
	 * @param ctx the parse tree
	 */
	void enterTimestamp_def(KVQLParser.Timestamp_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#timestamp_def}.
	 * @param ctx the parse tree
	 */
	void exitTimestamp_def(KVQLParser.Timestamp_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#any_def}.
	 * @param ctx the parse tree
	 */
	void enterAny_def(KVQLParser.Any_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#any_def}.
	 * @param ctx the parse tree
	 */
	void exitAny_def(KVQLParser.Any_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#anyAtomic_def}.
	 * @param ctx the parse tree
	 */
	void enterAnyAtomic_def(KVQLParser.AnyAtomic_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#anyAtomic_def}.
	 * @param ctx the parse tree
	 */
	void exitAnyAtomic_def(KVQLParser.AnyAtomic_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#anyJsonAtomic_def}.
	 * @param ctx the parse tree
	 */
	void enterAnyJsonAtomic_def(KVQLParser.AnyJsonAtomic_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#anyJsonAtomic_def}.
	 * @param ctx the parse tree
	 */
	void exitAnyJsonAtomic_def(KVQLParser.AnyJsonAtomic_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#anyRecord_def}.
	 * @param ctx the parse tree
	 */
	void enterAnyRecord_def(KVQLParser.AnyRecord_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#anyRecord_def}.
	 * @param ctx the parse tree
	 */
	void exitAnyRecord_def(KVQLParser.AnyRecord_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#id_path}.
	 * @param ctx the parse tree
	 */
	void enterId_path(KVQLParser.Id_pathContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#id_path}.
	 * @param ctx the parse tree
	 */
	void exitId_path(KVQLParser.Id_pathContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#name_path}.
	 * @param ctx the parse tree
	 */
	void enterName_path(KVQLParser.Name_pathContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#name_path}.
	 * @param ctx the parse tree
	 */
	void exitName_path(KVQLParser.Name_pathContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#field_name}.
	 * @param ctx the parse tree
	 */
	void enterField_name(KVQLParser.Field_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#field_name}.
	 * @param ctx the parse tree
	 */
	void exitField_name(KVQLParser.Field_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#create_table_statement}.
	 * @param ctx the parse tree
	 */
	void enterCreate_table_statement(KVQLParser.Create_table_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#create_table_statement}.
	 * @param ctx the parse tree
	 */
	void exitCreate_table_statement(KVQLParser.Create_table_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#table_name}.
	 * @param ctx the parse tree
	 */
	void enterTable_name(KVQLParser.Table_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#table_name}.
	 * @param ctx the parse tree
	 */
	void exitTable_name(KVQLParser.Table_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#table_def}.
	 * @param ctx the parse tree
	 */
	void enterTable_def(KVQLParser.Table_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#table_def}.
	 * @param ctx the parse tree
	 */
	void exitTable_def(KVQLParser.Table_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#key_def}.
	 * @param ctx the parse tree
	 */
	void enterKey_def(KVQLParser.Key_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#key_def}.
	 * @param ctx the parse tree
	 */
	void exitKey_def(KVQLParser.Key_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#shard_key_def}.
	 * @param ctx the parse tree
	 */
	void enterShard_key_def(KVQLParser.Shard_key_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#shard_key_def}.
	 * @param ctx the parse tree
	 */
	void exitShard_key_def(KVQLParser.Shard_key_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#id_list_with_size}.
	 * @param ctx the parse tree
	 */
	void enterId_list_with_size(KVQLParser.Id_list_with_sizeContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#id_list_with_size}.
	 * @param ctx the parse tree
	 */
	void exitId_list_with_size(KVQLParser.Id_list_with_sizeContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#id_with_size}.
	 * @param ctx the parse tree
	 */
	void enterId_with_size(KVQLParser.Id_with_sizeContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#id_with_size}.
	 * @param ctx the parse tree
	 */
	void exitId_with_size(KVQLParser.Id_with_sizeContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#storage_size}.
	 * @param ctx the parse tree
	 */
	void enterStorage_size(KVQLParser.Storage_sizeContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#storage_size}.
	 * @param ctx the parse tree
	 */
	void exitStorage_size(KVQLParser.Storage_sizeContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#ttl_def}.
	 * @param ctx the parse tree
	 */
	void enterTtl_def(KVQLParser.Ttl_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#ttl_def}.
	 * @param ctx the parse tree
	 */
	void exitTtl_def(KVQLParser.Ttl_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#alter_table_statement}.
	 * @param ctx the parse tree
	 */
	void enterAlter_table_statement(KVQLParser.Alter_table_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#alter_table_statement}.
	 * @param ctx the parse tree
	 */
	void exitAlter_table_statement(KVQLParser.Alter_table_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#alter_def}.
	 * @param ctx the parse tree
	 */
	void enterAlter_def(KVQLParser.Alter_defContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#alter_def}.
	 * @param ctx the parse tree
	 */
	void exitAlter_def(KVQLParser.Alter_defContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#alter_field_statements}.
	 * @param ctx the parse tree
	 */
	void enterAlter_field_statements(KVQLParser.Alter_field_statementsContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#alter_field_statements}.
	 * @param ctx the parse tree
	 */
	void exitAlter_field_statements(KVQLParser.Alter_field_statementsContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#add_field_statement}.
	 * @param ctx the parse tree
	 */
	void enterAdd_field_statement(KVQLParser.Add_field_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#add_field_statement}.
	 * @param ctx the parse tree
	 */
	void exitAdd_field_statement(KVQLParser.Add_field_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#drop_field_statement}.
	 * @param ctx the parse tree
	 */
	void enterDrop_field_statement(KVQLParser.Drop_field_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#drop_field_statement}.
	 * @param ctx the parse tree
	 */
	void exitDrop_field_statement(KVQLParser.Drop_field_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#modify_field_statement}.
	 * @param ctx the parse tree
	 */
	void enterModify_field_statement(KVQLParser.Modify_field_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#modify_field_statement}.
	 * @param ctx the parse tree
	 */
	void exitModify_field_statement(KVQLParser.Modify_field_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#schema_path}.
	 * @param ctx the parse tree
	 */
	void enterSchema_path(KVQLParser.Schema_pathContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#schema_path}.
	 * @param ctx the parse tree
	 */
	void exitSchema_path(KVQLParser.Schema_pathContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#init_schema_path_step}.
	 * @param ctx the parse tree
	 */
	void enterInit_schema_path_step(KVQLParser.Init_schema_path_stepContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#init_schema_path_step}.
	 * @param ctx the parse tree
	 */
	void exitInit_schema_path_step(KVQLParser.Init_schema_path_stepContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#schema_path_step}.
	 * @param ctx the parse tree
	 */
	void enterSchema_path_step(KVQLParser.Schema_path_stepContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#schema_path_step}.
	 * @param ctx the parse tree
	 */
	void exitSchema_path_step(KVQLParser.Schema_path_stepContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#drop_table_statement}.
	 * @param ctx the parse tree
	 */
	void enterDrop_table_statement(KVQLParser.Drop_table_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#drop_table_statement}.
	 * @param ctx the parse tree
	 */
	void exitDrop_table_statement(KVQLParser.Drop_table_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#create_index_statement}.
	 * @param ctx the parse tree
	 */
	void enterCreate_index_statement(KVQLParser.Create_index_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#create_index_statement}.
	 * @param ctx the parse tree
	 */
	void exitCreate_index_statement(KVQLParser.Create_index_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#index_name}.
	 * @param ctx the parse tree
	 */
	void enterIndex_name(KVQLParser.Index_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#index_name}.
	 * @param ctx the parse tree
	 */
	void exitIndex_name(KVQLParser.Index_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#index_path_list}.
	 * @param ctx the parse tree
	 */
	void enterIndex_path_list(KVQLParser.Index_path_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#index_path_list}.
	 * @param ctx the parse tree
	 */
	void exitIndex_path_list(KVQLParser.Index_path_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#index_path}.
	 * @param ctx the parse tree
	 */
	void enterIndex_path(KVQLParser.Index_pathContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#index_path}.
	 * @param ctx the parse tree
	 */
	void exitIndex_path(KVQLParser.Index_pathContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#keys_expr}.
	 * @param ctx the parse tree
	 */
	void enterKeys_expr(KVQLParser.Keys_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#keys_expr}.
	 * @param ctx the parse tree
	 */
	void exitKeys_expr(KVQLParser.Keys_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#values_expr}.
	 * @param ctx the parse tree
	 */
	void enterValues_expr(KVQLParser.Values_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#values_expr}.
	 * @param ctx the parse tree
	 */
	void exitValues_expr(KVQLParser.Values_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#path_type}.
	 * @param ctx the parse tree
	 */
	void enterPath_type(KVQLParser.Path_typeContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#path_type}.
	 * @param ctx the parse tree
	 */
	void exitPath_type(KVQLParser.Path_typeContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#create_text_index_statement}.
	 * @param ctx the parse tree
	 */
	void enterCreate_text_index_statement(KVQLParser.Create_text_index_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#create_text_index_statement}.
	 * @param ctx the parse tree
	 */
	void exitCreate_text_index_statement(KVQLParser.Create_text_index_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#fts_field_list}.
	 * @param ctx the parse tree
	 */
	void enterFts_field_list(KVQLParser.Fts_field_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#fts_field_list}.
	 * @param ctx the parse tree
	 */
	void exitFts_field_list(KVQLParser.Fts_field_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#fts_path_list}.
	 * @param ctx the parse tree
	 */
	void enterFts_path_list(KVQLParser.Fts_path_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#fts_path_list}.
	 * @param ctx the parse tree
	 */
	void exitFts_path_list(KVQLParser.Fts_path_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#fts_path}.
	 * @param ctx the parse tree
	 */
	void enterFts_path(KVQLParser.Fts_pathContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#fts_path}.
	 * @param ctx the parse tree
	 */
	void exitFts_path(KVQLParser.Fts_pathContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#es_properties}.
	 * @param ctx the parse tree
	 */
	void enterEs_properties(KVQLParser.Es_propertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#es_properties}.
	 * @param ctx the parse tree
	 */
	void exitEs_properties(KVQLParser.Es_propertiesContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#es_property_assignment}.
	 * @param ctx the parse tree
	 */
	void enterEs_property_assignment(KVQLParser.Es_property_assignmentContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#es_property_assignment}.
	 * @param ctx the parse tree
	 */
	void exitEs_property_assignment(KVQLParser.Es_property_assignmentContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#drop_index_statement}.
	 * @param ctx the parse tree
	 */
	void enterDrop_index_statement(KVQLParser.Drop_index_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#drop_index_statement}.
	 * @param ctx the parse tree
	 */
	void exitDrop_index_statement(KVQLParser.Drop_index_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#describe_statement}.
	 * @param ctx the parse tree
	 */
	void enterDescribe_statement(KVQLParser.Describe_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#describe_statement}.
	 * @param ctx the parse tree
	 */
	void exitDescribe_statement(KVQLParser.Describe_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#schema_path_list}.
	 * @param ctx the parse tree
	 */
	void enterSchema_path_list(KVQLParser.Schema_path_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#schema_path_list}.
	 * @param ctx the parse tree
	 */
	void exitSchema_path_list(KVQLParser.Schema_path_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#show_statement}.
	 * @param ctx the parse tree
	 */
	void enterShow_statement(KVQLParser.Show_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#show_statement}.
	 * @param ctx the parse tree
	 */
	void exitShow_statement(KVQLParser.Show_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#create_user_statement}.
	 * @param ctx the parse tree
	 */
	void enterCreate_user_statement(KVQLParser.Create_user_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#create_user_statement}.
	 * @param ctx the parse tree
	 */
	void exitCreate_user_statement(KVQLParser.Create_user_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#create_role_statement}.
	 * @param ctx the parse tree
	 */
	void enterCreate_role_statement(KVQLParser.Create_role_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#create_role_statement}.
	 * @param ctx the parse tree
	 */
	void exitCreate_role_statement(KVQLParser.Create_role_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#alter_user_statement}.
	 * @param ctx the parse tree
	 */
	void enterAlter_user_statement(KVQLParser.Alter_user_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#alter_user_statement}.
	 * @param ctx the parse tree
	 */
	void exitAlter_user_statement(KVQLParser.Alter_user_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#drop_user_statement}.
	 * @param ctx the parse tree
	 */
	void enterDrop_user_statement(KVQLParser.Drop_user_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#drop_user_statement}.
	 * @param ctx the parse tree
	 */
	void exitDrop_user_statement(KVQLParser.Drop_user_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#drop_role_statement}.
	 * @param ctx the parse tree
	 */
	void enterDrop_role_statement(KVQLParser.Drop_role_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#drop_role_statement}.
	 * @param ctx the parse tree
	 */
	void exitDrop_role_statement(KVQLParser.Drop_role_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#grant_statement}.
	 * @param ctx the parse tree
	 */
	void enterGrant_statement(KVQLParser.Grant_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#grant_statement}.
	 * @param ctx the parse tree
	 */
	void exitGrant_statement(KVQLParser.Grant_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#revoke_statement}.
	 * @param ctx the parse tree
	 */
	void enterRevoke_statement(KVQLParser.Revoke_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#revoke_statement}.
	 * @param ctx the parse tree
	 */
	void exitRevoke_statement(KVQLParser.Revoke_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#identifier_or_string}.
	 * @param ctx the parse tree
	 */
	void enterIdentifier_or_string(KVQLParser.Identifier_or_stringContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#identifier_or_string}.
	 * @param ctx the parse tree
	 */
	void exitIdentifier_or_string(KVQLParser.Identifier_or_stringContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#identified_clause}.
	 * @param ctx the parse tree
	 */
	void enterIdentified_clause(KVQLParser.Identified_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#identified_clause}.
	 * @param ctx the parse tree
	 */
	void exitIdentified_clause(KVQLParser.Identified_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#create_user_identified_clause}.
	 * @param ctx the parse tree
	 */
	void enterCreate_user_identified_clause(KVQLParser.Create_user_identified_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#create_user_identified_clause}.
	 * @param ctx the parse tree
	 */
	void exitCreate_user_identified_clause(KVQLParser.Create_user_identified_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#by_password}.
	 * @param ctx the parse tree
	 */
	void enterBy_password(KVQLParser.By_passwordContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#by_password}.
	 * @param ctx the parse tree
	 */
	void exitBy_password(KVQLParser.By_passwordContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#password_lifetime}.
	 * @param ctx the parse tree
	 */
	void enterPassword_lifetime(KVQLParser.Password_lifetimeContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#password_lifetime}.
	 * @param ctx the parse tree
	 */
	void exitPassword_lifetime(KVQLParser.Password_lifetimeContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#reset_password_clause}.
	 * @param ctx the parse tree
	 */
	void enterReset_password_clause(KVQLParser.Reset_password_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#reset_password_clause}.
	 * @param ctx the parse tree
	 */
	void exitReset_password_clause(KVQLParser.Reset_password_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#account_lock}.
	 * @param ctx the parse tree
	 */
	void enterAccount_lock(KVQLParser.Account_lockContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#account_lock}.
	 * @param ctx the parse tree
	 */
	void exitAccount_lock(KVQLParser.Account_lockContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#grant_roles}.
	 * @param ctx the parse tree
	 */
	void enterGrant_roles(KVQLParser.Grant_rolesContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#grant_roles}.
	 * @param ctx the parse tree
	 */
	void exitGrant_roles(KVQLParser.Grant_rolesContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#grant_system_privileges}.
	 * @param ctx the parse tree
	 */
	void enterGrant_system_privileges(KVQLParser.Grant_system_privilegesContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#grant_system_privileges}.
	 * @param ctx the parse tree
	 */
	void exitGrant_system_privileges(KVQLParser.Grant_system_privilegesContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#grant_object_privileges}.
	 * @param ctx the parse tree
	 */
	void enterGrant_object_privileges(KVQLParser.Grant_object_privilegesContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#grant_object_privileges}.
	 * @param ctx the parse tree
	 */
	void exitGrant_object_privileges(KVQLParser.Grant_object_privilegesContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#revoke_roles}.
	 * @param ctx the parse tree
	 */
	void enterRevoke_roles(KVQLParser.Revoke_rolesContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#revoke_roles}.
	 * @param ctx the parse tree
	 */
	void exitRevoke_roles(KVQLParser.Revoke_rolesContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#revoke_system_privileges}.
	 * @param ctx the parse tree
	 */
	void enterRevoke_system_privileges(KVQLParser.Revoke_system_privilegesContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#revoke_system_privileges}.
	 * @param ctx the parse tree
	 */
	void exitRevoke_system_privileges(KVQLParser.Revoke_system_privilegesContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#revoke_object_privileges}.
	 * @param ctx the parse tree
	 */
	void enterRevoke_object_privileges(KVQLParser.Revoke_object_privilegesContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#revoke_object_privileges}.
	 * @param ctx the parse tree
	 */
	void exitRevoke_object_privileges(KVQLParser.Revoke_object_privilegesContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#principal}.
	 * @param ctx the parse tree
	 */
	void enterPrincipal(KVQLParser.PrincipalContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#principal}.
	 * @param ctx the parse tree
	 */
	void exitPrincipal(KVQLParser.PrincipalContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#sys_priv_list}.
	 * @param ctx the parse tree
	 */
	void enterSys_priv_list(KVQLParser.Sys_priv_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#sys_priv_list}.
	 * @param ctx the parse tree
	 */
	void exitSys_priv_list(KVQLParser.Sys_priv_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#priv_item}.
	 * @param ctx the parse tree
	 */
	void enterPriv_item(KVQLParser.Priv_itemContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#priv_item}.
	 * @param ctx the parse tree
	 */
	void exitPriv_item(KVQLParser.Priv_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#obj_priv_list}.
	 * @param ctx the parse tree
	 */
	void enterObj_priv_list(KVQLParser.Obj_priv_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#obj_priv_list}.
	 * @param ctx the parse tree
	 */
	void exitObj_priv_list(KVQLParser.Obj_priv_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#object}.
	 * @param ctx the parse tree
	 */
	void enterObject(KVQLParser.ObjectContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#object}.
	 * @param ctx the parse tree
	 */
	void exitObject(KVQLParser.ObjectContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#json_text}.
	 * @param ctx the parse tree
	 */
	void enterJson_text(KVQLParser.Json_textContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#json_text}.
	 * @param ctx the parse tree
	 */
	void exitJson_text(KVQLParser.Json_textContext ctx);
	/**
	 * Enter a parse tree produced by the {@code JsonObject}
	 * labeled alternative in {@link KVQLParser#jsobject}.
	 * @param ctx the parse tree
	 */
	void enterJsonObject(KVQLParser.JsonObjectContext ctx);
	/**
	 * Exit a parse tree produced by the {@code JsonObject}
	 * labeled alternative in {@link KVQLParser#jsobject}.
	 * @param ctx the parse tree
	 */
	void exitJsonObject(KVQLParser.JsonObjectContext ctx);
	/**
	 * Enter a parse tree produced by the {@code EmptyJsonObject}
	 * labeled alternative in {@link KVQLParser#jsobject}.
	 * @param ctx the parse tree
	 */
	void enterEmptyJsonObject(KVQLParser.EmptyJsonObjectContext ctx);
	/**
	 * Exit a parse tree produced by the {@code EmptyJsonObject}
	 * labeled alternative in {@link KVQLParser#jsobject}.
	 * @param ctx the parse tree
	 */
	void exitEmptyJsonObject(KVQLParser.EmptyJsonObjectContext ctx);
	/**
	 * Enter a parse tree produced by the {@code ArrayOfJsonValues}
	 * labeled alternative in {@link KVQLParser#jsarray}.
	 * @param ctx the parse tree
	 */
	void enterArrayOfJsonValues(KVQLParser.ArrayOfJsonValuesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code ArrayOfJsonValues}
	 * labeled alternative in {@link KVQLParser#jsarray}.
	 * @param ctx the parse tree
	 */
	void exitArrayOfJsonValues(KVQLParser.ArrayOfJsonValuesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code EmptyJsonArray}
	 * labeled alternative in {@link KVQLParser#jsarray}.
	 * @param ctx the parse tree
	 */
	void enterEmptyJsonArray(KVQLParser.EmptyJsonArrayContext ctx);
	/**
	 * Exit a parse tree produced by the {@code EmptyJsonArray}
	 * labeled alternative in {@link KVQLParser#jsarray}.
	 * @param ctx the parse tree
	 */
	void exitEmptyJsonArray(KVQLParser.EmptyJsonArrayContext ctx);
	/**
	 * Enter a parse tree produced by the {@code JsonPair}
	 * labeled alternative in {@link KVQLParser#jspair}.
	 * @param ctx the parse tree
	 */
	void enterJsonPair(KVQLParser.JsonPairContext ctx);
	/**
	 * Exit a parse tree produced by the {@code JsonPair}
	 * labeled alternative in {@link KVQLParser#jspair}.
	 * @param ctx the parse tree
	 */
	void exitJsonPair(KVQLParser.JsonPairContext ctx);
	/**
	 * Enter a parse tree produced by the {@code JsonObjectValue}
	 * labeled alternative in {@link KVQLParser#jsvalue}.
	 * @param ctx the parse tree
	 */
	void enterJsonObjectValue(KVQLParser.JsonObjectValueContext ctx);
	/**
	 * Exit a parse tree produced by the {@code JsonObjectValue}
	 * labeled alternative in {@link KVQLParser#jsvalue}.
	 * @param ctx the parse tree
	 */
	void exitJsonObjectValue(KVQLParser.JsonObjectValueContext ctx);
	/**
	 * Enter a parse tree produced by the {@code JsonArrayValue}
	 * labeled alternative in {@link KVQLParser#jsvalue}.
	 * @param ctx the parse tree
	 */
	void enterJsonArrayValue(KVQLParser.JsonArrayValueContext ctx);
	/**
	 * Exit a parse tree produced by the {@code JsonArrayValue}
	 * labeled alternative in {@link KVQLParser#jsvalue}.
	 * @param ctx the parse tree
	 */
	void exitJsonArrayValue(KVQLParser.JsonArrayValueContext ctx);
	/**
	 * Enter a parse tree produced by the {@code JsonAtom}
	 * labeled alternative in {@link KVQLParser#jsvalue}.
	 * @param ctx the parse tree
	 */
	void enterJsonAtom(KVQLParser.JsonAtomContext ctx);
	/**
	 * Exit a parse tree produced by the {@code JsonAtom}
	 * labeled alternative in {@link KVQLParser#jsvalue}.
	 * @param ctx the parse tree
	 */
	void exitJsonAtom(KVQLParser.JsonAtomContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#comment}.
	 * @param ctx the parse tree
	 */
	void enterComment(KVQLParser.CommentContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#comment}.
	 * @param ctx the parse tree
	 */
	void exitComment(KVQLParser.CommentContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#duration}.
	 * @param ctx the parse tree
	 */
	void enterDuration(KVQLParser.DurationContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#duration}.
	 * @param ctx the parse tree
	 */
	void exitDuration(KVQLParser.DurationContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#time_unit}.
	 * @param ctx the parse tree
	 */
	void enterTime_unit(KVQLParser.Time_unitContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#time_unit}.
	 * @param ctx the parse tree
	 */
	void exitTime_unit(KVQLParser.Time_unitContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#number}.
	 * @param ctx the parse tree
	 */
	void enterNumber(KVQLParser.NumberContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#number}.
	 * @param ctx the parse tree
	 */
	void exitNumber(KVQLParser.NumberContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#string}.
	 * @param ctx the parse tree
	 */
	void enterString(KVQLParser.StringContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#string}.
	 * @param ctx the parse tree
	 */
	void exitString(KVQLParser.StringContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#id_list}.
	 * @param ctx the parse tree
	 */
	void enterId_list(KVQLParser.Id_listContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#id_list}.
	 * @param ctx the parse tree
	 */
	void exitId_list(KVQLParser.Id_listContext ctx);
	/**
	 * Enter a parse tree produced by {@link KVQLParser#id}.
	 * @param ctx the parse tree
	 */
	void enterId(KVQLParser.IdContext ctx);
	/**
	 * Exit a parse tree produced by {@link KVQLParser#id}.
	 * @param ctx the parse tree
	 */
	void exitId(KVQLParser.IdContext ctx);
}
