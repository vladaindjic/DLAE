from parglare import Grammar, Parser
from collections import OrderedDict
import re

INT_REGEX_PATTERN = r'[+-]?\d+'
DOUBLE_REGEX_PATTERN = r'[+-]?((\d*\.\d+)|(\d+(\.\d*)?))'
STRING_REGEX_PATTERN = r'\"((\\\")|[^\"])*\"'
DATE_TIME_REGEX_PATTERN = r'\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}\.\d{3}'

INT_TYPE = "INT"
DOUBLE_TYPE = "DOUBLE"
STRING_TYPE = "STRING"
DATE_TIME_TYPE = "DATE_TIME"


class LogParser:
    def __init__(self):
        self._declarations = OrderedDict()
        self._ids = set()
        self._elements = []


# IR
def create_regex_object(regex, default_regex_pattern):
    """
        Return regex if not None, else create Regex object using default default_regex_pattern
    :param regex: instance of Regex
    :param default_regex_pattern:
    :return:
    """
    if regex is None:
        return Regex(re.compile(default_regex_pattern))
    return regex


class Declaration:
    def __init__(self, identifier, datatype, regex):
        self.id = identifier
        self.type = datatype
        print(regex)
        self.regex = regex


class IntDeclaration(Declaration):
    def __init__(self, identifier, regex=INT_REGEX_PATTERN):
        super().__init__(identifier, INT_TYPE, create_regex_object(regex, INT_REGEX_PATTERN))


class DoubleDeclaration(Declaration):
    def __init__(self, identifier, regex=DOUBLE_REGEX_PATTERN):
        super().__init__(identifier, DOUBLE_TYPE, create_regex_object(regex, DOUBLE_REGEX_PATTERN))


class StringDeclaration(Declaration):
    def __init__(self, identifier, regex=STRING_REGEX_PATTERN):
        super().__init__(identifier, STRING_TYPE, create_regex_object(regex, STRING_REGEX_PATTERN))


class DateTimeDeclaration(Declaration):
    def __init__(self, identifier, regex=DATE_TIME_REGEX_PATTERN):
        super().__init__(identifier, DATE_TIME_TYPE, create_regex_object(regex, DATE_TIME_REGEX_PATTERN))


class LogFormat:
    def __init__(self, body, declarations=None):
        self.declarations = declarations
        self.body = body


class Declarations:
    def __init__(self):
        self.declarations = []

    def add_declaration(self, declaration):
        self.declarations.append(declaration)
        # so we could propagate declarations
        return self


class Body:
    def __init__(self):
        self.elements = []

    def add_element(self, element):
        self.elements.append(element)
        # so we could propagate body
        return self


class Element:
    def __init__(self, content):
        self.content = content


class Identifier:
    def __init__(self, identifier):
        self.id = identifier


class Regex:
    def __init__(self, regex):
        self.regex = re.compile(regex)


def create_declaration(identifier, rhsdecl_tupple):
    datatype, regex = rhsdecl_tupple
    if datatype == INT_TYPE:
        return IntDeclaration(identifier, regex)
    elif datatype == DOUBLE_TYPE:
        return DoubleDeclaration(identifier, regex)
    elif datatype == STRING_TYPE:
        return StringDeclaration(identifier, regex)
    elif datatype == DATE_TIME_TYPE:
        return DateTimeDeclaration(identifier, regex)
    else:
        raise Exception("Bad declaration. Unknown type: %s" % datatype)


actions = {
    # LogFormat:
    #     Body
    #     | Declarations Body
    # ;
    "LogFormat": [
        lambda _, nodes: LogFormat(body=nodes[0]),
        lambda _, nodes: LogFormat(declarations=nodes[0], body=nodes[1])
    ],
    # Declarations:
    #     Declaration
    #     | Declarations Declaration
    # ;
    "Declarations": [
        lambda _, nodes: Declarations().add_declaration(nodes[0]),
        lambda _, nodes: nodes[0].add_declaration(nodes[1])
    ],
    # Body:
    #     Element
    #     | Body Element
    # ;
    "Body": [
        lambda _, nodes: Body().add_element(nodes[0]),
        lambda _, nodes: nodes[0].add_element(nodes[1])
    ],
    # Declaration:
    #     Decl SEMICOLON
    # ;
    "Declaration": lambda _, nodes: nodes[0],
    # Decl:
    #     ID ASSIGN RHSDecl
    # ;
    "Decl": lambda _, nodes: create_declaration(nodes[0], nodes[2]),
    # RHSDecl:
    #     DataType
    #     | DataType LPAREN REGEX RPAREN
    # ;
    "RHSDecl": [
        lambda _, nodes: (nodes[0], None),
        lambda _, nodes: (nodes[0], nodes[2])
    ],
    # Element:
    #     LT Content GT
    # ;
    "Element": lambda _, nodes: Element(nodes[1]),
    # Content:
    #     ID
    #     | Decl
    #     | REGEX
    # ;
    "Content": [
        lambda _, nodes: nodes[0],
        lambda _, nodes: nodes[0],
        lambda _, nodes: nodes[0],
    ],
    # DataType:
    #     INT
    #     | DOUBLE
    #     | STRING
    #     | DATETIME
    # ;
    "DataType": [
        lambda _, nodes: nodes[0],
        lambda _, nodes: nodes[0],
        lambda _, nodes: nodes[0],
        lambda _, nodes: nodes[0],
    ],

    # terminals
    # ID: /\w+/;
    "ID": lambda _, value: Identifier(value),
    # ASSIGN: ':=';
    # LPAREN: '(';
    # RPAREN: ')';
    # REGEX: / \ / ((\\\ /) | [ ^\ /]) *\ //;
    "REGEX": lambda _, value: Regex(value),
    # LT: '<';
    # GT: '>';
    # KEYWORD: / \w + /;
    # INT: 'int';
    "INT": lambda _, value: INT_TYPE,
    # DOUBLE: 'double';
    "DOUBLE": lambda _, value: DOUBLE_TYPE,
    # STRING: 'string';
    "STRING": lambda _, value: STRING_TYPE,
    # DATETIME: 'datetime';
    "DATETIME": lambda _value: DATE_TIME_TYPE
    # SEMICOLON: ';';

}

primer = """
    p:=int; 
    a:=double; 
    b:=double; 
    <asd := int(/\./)> </\..*/> <p>
    """


def main():
    g = Grammar.from_file('log_formatter.pg')
    p = Parser(g, actions=actions)
    res = p.parse(primer)
    print(res)


if __name__ == '__main__':
    main()
