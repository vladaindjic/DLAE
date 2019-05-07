from parglare import Grammar, Parser
from collections import OrderedDict
import dateutil.parser as date_parser
import re

INT_REGEX_PATTERN = r'[+-]?\d+'
DOUBLE_REGEX_PATTERN = r'[+-]?((\d*\.\d+)|(\d+(\.\d*)?))'
STRING_REGEX_PATTERN = r'\"((\\\")|[^\"])*\"'
DATE_TIME_REGEX_PATTERN = r'\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}\.\d{3}'

INT_TYPE = "INT"
DOUBLE_TYPE = "DOUBLE"
STRING_TYPE = "STRING"
DATE_TIME_TYPE = "DATE_TIME"
SEPARATOR_TYPE = "SEPARATOR"


class Log:
    pass

    def __str__(self):
        return str(self.__dict__)


class LogParser:
    def __init__(self, declarations, elements):
        self.declarations = declarations
        self._elements = elements

    def parse_log(self, text):
        unmatched = text
        log = Log()
        for el in self._elements:
            if isinstance(el, Regex):
                matched, unmatched = self._match_regex(el, unmatched, text)
            else:
                if isinstance(el, Identifier):
                    declaration = self.declarations[el.id]
                elif isinstance(el, Declaration):
                    declaration = el
                else:
                    raise ValueError('This should not happen')

                identifier, datatype, regex = declaration.identifier, declaration.type, declaration.regex
                matched, unmatched = self._match_regex(regex, unmatched, text)

                # if the declaration is of type SeparatorDeclaration, we don't need to extract value
                if isinstance(declaration, SeparatorDeclaration):
                    continue

                value = self._create_value_for_type(matched, datatype)
                # FIXME: bad, but easy way to add attribute to object
                log.__dict__[identifier.id] = value

        if unmatched:
            raise ValueError('There is unmathced characters: (%s) in log: %s' % (unmatched, text))

        return log

    @staticmethod
    def _match_regex(regex, unmatched, text):
        m = re.match(regex.regex, unmatched)
        if m is None:
            raise ValueError(
                "Regex: /%s/ is not matched at this part: %s of the log: %s" % (
                    regex.regex.pattern, unmatched, text))
        start, end = m.span()
        return unmatched[start:end], unmatched[end:]

    @staticmethod
    def _create_value_for_type(matched_str, datatype):
        if datatype == INT_TYPE:
            return int(matched_str)
        elif datatype == DOUBLE_TYPE:
            return float(matched_str)
        elif datatype == STRING_TYPE:
            return str(matched_str)
        elif datatype == DATE_TIME_TYPE:
            datetime_value = date_parser.parse(matched_str)
            # FIXME: for now, we will remove timezone so that we don't have problems
            # with naive and timezone aware datetime objects
            datetime_value = datetime_value.replace(tzinfo=None)
            return datetime_value
        else:
            raise TypeError('Wrong datetype: %s' % datatype)


# IR
def create_regex_object(regex, default_regex_pattern):
    """
        Return regex if not None, else create Regex object using default default_regex_pattern
    :param regex: instance of Regex
    :param default_regex_pattern:
    :return:
    """
    if regex is None:
        # slashes are added, beacuse constructor of Regex class will remove them
        return Regex('/' + default_regex_pattern + '/')
    return regex


class Element:
    pass


class Declaration(Element):
    def __init__(self, identifier, datatype, regex):
        self.identifier = identifier
        self.type = datatype
        # print(regex)
        self.regex = regex


class PropertyDeclaration(Declaration):
    def __init__(self, identifier, datatype, regex):
        super().__init__(identifier, datatype, regex)


class IntDeclaration(PropertyDeclaration):
    def __init__(self, identifier, regex=INT_REGEX_PATTERN):
        super().__init__(identifier, INT_TYPE, create_regex_object(regex, INT_REGEX_PATTERN))


class DoubleDeclaration(PropertyDeclaration):
    def __init__(self, identifier, regex=DOUBLE_REGEX_PATTERN):
        super().__init__(identifier, DOUBLE_TYPE, create_regex_object(regex, DOUBLE_REGEX_PATTERN))


class StringDeclaration(PropertyDeclaration):
    def __init__(self, identifier, regex=STRING_REGEX_PATTERN):
        super().__init__(identifier, STRING_TYPE, create_regex_object(regex, STRING_REGEX_PATTERN))


class DateTimeDeclaration(PropertyDeclaration):
    def __init__(self, identifier, regex=DATE_TIME_REGEX_PATTERN):
        super().__init__(identifier, DATE_TIME_TYPE, create_regex_object(regex, DATE_TIME_REGEX_PATTERN))


class SeparatorDeclaration(Declaration):
    def __init__(self, sep_identifier, regex):
        super().__init__(sep_identifier, SEPARATOR_TYPE, regex)


class LogFormat:
    def __init__(self, body, declarations=None):
        self.declarations = declarations
        self.body = body

    def semantic_analysis(self):
        decl_dict = OrderedDict()
        body_decl_dict = OrderedDict()
        id_set = set()
        # identifiers in declarations must be unique
        if self.declarations is not None:
            for decl in self.declarations:
                if decl.identifier.id in decl_dict:
                    raise ValueError("Declaration for id: %s is not unique" % decl.identifier.id)
                decl_dict[decl.identifier.id] = decl
        # each element should have unique id (FIXME: should this be fixed)
        # each id in declarations places in body should be unique too
        for el in self.body:
            if isinstance(el, Declaration):
                declaration = el
                if declaration.identifier.id in decl_dict or declaration.identifier.id in body_decl_dict:
                    raise ValueError("Declaration placed in body for id: %s is not unique" % declaration.identifier.id)
                if isinstance(declaration, PropertyDeclaration):
                    body_decl_dict[declaration.identifier.id] = declaration
                elif isinstance(declaration, SeparatorDeclaration):
                    decl_dict[declaration.identifier.id] = declaration
                else:
                    raise ValueError('This declaration %s should inherit Declaration' % declaration)
            elif isinstance(el, Regex):
                pass
            elif isinstance(el, PropertyIdentifier):
                identifier = el
                # element id should be unique in body
                if identifier.id in body_decl_dict:
                    raise ValueError('Id: %s is used in declaration placed in body.' % identifier.id)
                elif identifier.id not in decl_dict:
                    raise ValueError('Id: %s is not declared.' % identifier.id)
                elif identifier.id in id_set:
                    raise ValueError('Element with id: %s is put multiple times in body.' % identifier.id)
                id_set.add(identifier.id)
            elif isinstance(el, SeparatorIdentifier):
                sep_identifier = el
                # separator_identifier must be definied
                if sep_identifier.id not in decl_dict:
                    raise ValueError('Separator Id: %s is not declared.' % sep_identifier.id)
            else:
                raise TypeError('Wrong value of body element %s' % el)

        # merge dicts with declarations
        for k in body_decl_dict:
            decl_dict[k] = body_decl_dict[k]

        return LogParser(decl_dict, self.body.elements)


class Declarations:
    def __init__(self):
        self.declarations = []

    def add_declaration(self, declaration):
        self.declarations.append(declaration)
        # so we could propagate declarations
        return self

    def __iter__(self):
        # this could work in python 3
        return (yield from self.declarations)
        # for i in self.declarations:
        #     yield i


class Body:
    def __init__(self):
        self.elements = []

    def add_element(self, element):
        self.elements.append(element)
        # so we could propagate body
        return self

    def __iter__(self):
        # this could work in python 3
        return (yield from self.elements)
        # for i in self.elements:
        #     yield i


class Identifier(Element):
    def __init__(self, identifier):
        self.id = identifier


class PropertyIdentifier(Identifier):
    def __init__(self, identifier):
        super().__init__(identifier)


class SeparatorIdentifier(Identifier):
    def __init__(self, sep_identifier):
        super().__init__(sep_identifier)


class Regex(Element):
    def __init__(self, regex):
        self.regex = re.compile(regex[1:-1])


def create_property_declaration(identifier, rhsdecl_tupple):
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
    #     PropDecl
    #     | SepDecl
    # ;
    "Decl": lambda _, nodes: nodes[0],

    # PropDecl:
    #     ID ASSIGN RHSDecl
    # ;
    "PropDecl": lambda _, nodes: create_property_declaration(nodes[0], nodes[2]),
    # SepDecl:
    #     SEP_ID ASSIGN REGEX
    # ;
    "SepDecl": lambda _, nodes: SeparatorDeclaration(nodes[0], nodes[2]),
    # RHSDecl:
    #     DataType
    #     | DataType LPAREN REGEX RPAREN
    # ;
    "RHSDecl": [
        lambda _, nodes: (nodes[0], None),
        lambda _, nodes: (nodes[0], nodes[2])
    ],
    # Element:
    #     ID
    #     | SEP_ID
    #     | Decl
    #     | REGEX
    # ;
    "Element": lambda _, nodes: nodes[0],  # I think that we don't need class for Element
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
    # ID:
    #     PROP_ID
    #     | SEP_ID
    # ;
    "ID": lambda _, nodes: nodes[0],  # I think that we don't need class for Element

    # ID: /[a-zA-Z]\w*/;
    "PROP_ID": lambda _, value: PropertyIdentifier(value),
    # SEP_ID: /_\w*/;
    "SEP_ID": lambda _, value: SeparatorIdentifier(value),
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
    "DATETIME": lambda _, value: DATE_TIME_TYPE
    # SEMICOLON: ';';

}

primer = """
    p:=int; 
    a:=double; 
    b:=double;
    c:=datetime(/\d{2}\.\d{2}\.\d{4}/);
    e:=int;
    _comma:=/\s*,\s*/;
    
    a _comma b _comma c _comma p _comma m:=int _dot:=/\s*\.\s*/ n:=int _dot asd:=int _dot /aaa/ e
    
    """
log = '1.1, 2.2 , 20.02.1995, 10,  +123 . 100. 1001 .  aaa111'


# primer = '''
#     </,\s+/> </asd/>
# '''
# log = ",   asd  "

def build_log_parser(log_format_definition):
    g = Grammar.from_file('log_formatter.pg')
    p = Parser(g, actions=actions)
    lf = p.parse(log_format_definition)
    lp = lf.semantic_analysis()
    return lp


def main():
    lp = build_log_parser(primer)
    l = lp.parse_log(log)
    print(l.a)
    print(l.b)
    print(l.c)
    print(l.p)
    print(l.m)
    print(l.__dict__)


if __name__ == '__main__':
    main()
