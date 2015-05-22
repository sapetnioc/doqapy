from parsimonious.grammar import Grammar

grammar = Grammar('''
query = _? ( select_where / select / where ) _?
select_where = select _ where
_ = ~"[ \\n\\t]+"
select = ~"select"i _ select_item (_? "," _? select_item)*
select_item = (collection_field (_ "as" _ identifier)?) / collection_path
identifier = ~"[a-zA-Z_][a-zA-Z0-9_]*"
collection_path = identifier ("/" identifier)*
collection_field = (collection_path)? "." identifier

where = ~"where"i _ boolean_expression

condition = operator_condition / in_operator
operator_condition = operand _? operator _? operand
operand = collection_field / collection_path / literal / external_data
operator = "=" / "!=" / ">=" / "<=" / ">" / "<"
literal = string / number
string = ~"\\\"[^\\\"]*\\\""
number = ~"[0-9]+"
external_data = "?"

in_operator = operand _ ~"in"i _ operand

boolean_expression = (parenthesis_bool / condition) operator_bool?
parenthesis_bool = "(" _? boolean_expression _? ")"
operator_bool = and_bool / or_bool
and_bool = _ ~"and"i _ boolean_expression
or_bool = _ ~"or"i _ boolean_expression
''')
