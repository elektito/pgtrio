from parsimonious import Grammar, ParseError
from parsimonious.nodes import NodeVisitor, Node


grammar = Grammar(r'''
array = open values close
values = value (comma value)*
value = quoted / unquoted / array
unquoted = ~r'[^,{}]+'
quoted = ~r'"(?:[^"\\]|\\.)*"'

open = "{"
close = "}"
comma = ","
''')


class ArrayVisitor(NodeVisitor):
    def visit_array(self, node, visited_children):
        _, values, _ = visited_children
        return values

    def visit_values(self, node, visited_children):
        output = []
        v1, rest = visited_children
        output.append(v1[0])
        for comma, value in rest:
            output.append(value[0])
        return output

    def visit_quoted(self, node, visited_children):
        s = node.text[1:-1]
        ret = ''
        i = 0
        while i < len(s):
            if s[i] == '\\':
                ret += s[i+1]
                i += 2
            else:
                ret += s[i]
                i += 1

        return ret

    def visit_unquoted(self, node, visited_children):
        return node.text

    def generic_visit(self, node, visited_children):
        return visited_children or node


class ArrayParseError(Exception):
    pass


def parse_text_array(value):
    assert isinstance(value, str)

    # the grammar does not handle empty arrays (it's written that way
    # because empty arrays are not allowed inside arrays)
    if value == '{}':
        return []

    try:
        tree = grammar.parse(value)
    except ParseError:
        raise ArrayParseError
    visitor = ArrayVisitor()
    return visitor.visit(tree)
