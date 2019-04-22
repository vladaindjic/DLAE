import datetime
import re

from parglare import Grammar, Parser
from collections import OrderedDict
from log_formatter import build_log_parser, IntDeclaration, DoubleDeclaration, StringDeclaration, \
    DateTimeDeclaration

COUNT_STR = 'COUNT'
LAST_STR = 'LAST'
GROUP_BY_STR = 'GROUP_BY'

# u akcijama je potrebno proveriti postojanje atributa
# a takodje i njihov tip

class IRObject:
    def inv(self):
        return self

    def remove_not(self):
        return self

    def python_condition(self):
        return ""

    def semantic_analysis(self, log_parser):
        """
            We need a log parser in order to do semantic analysis
        :param log_parser:
        :return:
        """
        pass


class AlarmQuery(IRObject):
    def __init__(self, query, header=None):
        self.query = query
        self.header = header

    def remove_not(self):
        self.query = self.query.remove_not()

    def python_condition(self):
        return self.query.python_condition()

    def semantic_analysis(self, log_parser):
        self.query.semantic_analysis(log_parser)
        if self.header is not None:
            self.header.semantic_analysis(log_parser)

    def __str__(self):
        return "Query: %s; Header: %s" % (self.query, self.header)


class Not(IRObject):
    def __init__(self, term):
        self.term = term

    def __str__(self):
        return "Not(%s)" % self.term

    def inv(self):
        return self.term.inv()

    # FIXME: check if this is ok
    def remove_not(self):
        return self.inv()

    def python_condition(self):
        return "(not %s)" % self.term.python_condition()

    def semantic_analysis(self, log_parser):
        self.term.semantic_analysis(log_parser)


class And(IRObject):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __str__(self):
        return "And(%s, %s)" % (self.left, self.right)

    def inv(self):
        return Or(self.left.inv(), self.right.inv())

    def remove_not(self):
        self.left = self.left.remove_not()
        self.right = self.right.remove_not()
        return self

    def python_condition(self):
        return "(%s and %s)" % (self.left.python_condition(), self.right.python_condition())

    def semantic_analysis(self, log_parser):
        self.left.semantic_analysis(log_parser)
        self.right.semantic_analysis(log_parser)


class Or(IRObject):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __str__(self):
        return "Or(%s, %s)" % (self.left, self.right)

    def inv(self):
        return And(self.left.inv(), self.right.inv())

    def remove_not(self):
        self.left = self.left.remove_not()
        self.right = self.right.remove_not()
        return self

    def python_condition(self):
        return "(%s or %s)" % (self.left.python_condition(), self.right.python_condition())

    def semantic_analysis(self, log_parser):
        self.left.semantic_analysis(log_parser)
        self.right.semantic_analysis(log_parser)


class AtomicExpr(IRObject):
    pass


class RelExpr(AtomicExpr):
    def __init__(self, prop, value):
        self.property = prop
        self.value = value

    def set_property_and_value(self, prop, value):
        self.property = prop
        self.value = value
        return self

    def str_rel_op(self):
        return ""

    def python_rel_op(self):
        return ""

    def __str__(self):
        return "%s(%s, %s)" % (self.str_rel_op(), self.property, self.value)

    def python_condition(self):
        return "%s %s %s" % (self.property.python_condition(),
                             self.python_rel_op(),
                             self.value.python_condition())

    def semantic_analysis(self, log_parser):
        property_name = self.property.name
        if property_name not in log_parser.declarations:
            raise AttributeError("Log does not have property with name: %s" % property_name)
        log_property = log_parser.declarations[property_name]
        # Check if type of log_property is the same as type of value of RelExpr
        # log_property is subclass of Declaration.
        # value if subclass of Value
        if isinstance(log_property, IntDeclaration):
            # make implicit cast
            if isinstance(self.value, DoubleValue):
                # this will cast to integer, and then convert to string
                self.value = IntValue(str(int(self.value.value)))
                return
            if not isinstance(self.value, IntValue):
                raise TypeError("%s is defined as integer." % property_name)
        elif isinstance(log_property, DoubleDeclaration):
            # make implicit cast
            if isinstance(self.value, IntValue):
                self.value = DoubleValue(str(float(self.value.value)))
                return
            if not isinstance(self.value, DoubleValue):
                raise TypeError("%s is defined as double." % property_name)
        elif isinstance(log_property, StringDeclaration):
            if not isinstance(self.value, StringValue):
                raise TypeError("%s is defined as string." % property_name)
        elif isinstance(log_property, DateTimeDeclaration):
            if not isinstance(self.value, DatetimeValue):
                raise TypeError("%s is defined as datetime." % property_name)
        else:
            raise TypeError("Wrong type of log property: %s" % property_name)


# FIXME: see if this is needed at all
class RegExpr(AtomicExpr):
    def __init__(self, rel_expr):
        self.rel_expr = rel_expr

    def __str__(self):
        return "Reg(%s)" % self.rel_expr

    def inv(self):
        return RegExpr(self.rel_expr.inv())

    def python_condition(self):
        """
            Note: Only full regex match is accepted
        :return:
        """
        match_prefix = "re.match(%s, %s)" % (self.rel_expr.value.python_condition(),
                                             self.rel_expr.property.python_condition())
        output = ""

        # Uncomment for full match
        # positive_match_condition = "(" + match_prefix + " is not None) and len(" + \
        #                            match_prefix + ".group()) == len(" + \
        #                            str(
        #                                self.rel_expr.property.python_condition()) + ")"  # self.rel_expr.value.value is simple string
        # if isinstance(self.rel_expr, Eq):
        #     output += positive_match_condition
        # elif isinstance(self.rel_expr, Ne):
        #     output += "not ( " + positive_match_condition + " )"
        # else:
        #     raise TypeError("RelExpr in RegExpr can only be Eq or Ne")

        output += match_prefix
        if isinstance(self.rel_expr, Eq):
            output += " is not None"
        elif isinstance(self.rel_expr, Ne):
            output += " is None"
        else:
            raise TypeError("RelExpr in RegExpr can only be Eq or Ne")

        return "( " + output + " )"

    def semantic_analysis(self, log_parser):
        self.rel_expr.semantic_analysis(log_parser)


class TimestampExpr(AtomicExpr):
    def __init__(self, prop, value):
        self.prop = prop
        self.value = value


# FIXME: I won't use it for now
class AtExpr(TimestampExpr):
    def __init__(self, prop, value):
        super().__init__(prop, value)

    def __str__(self):
        return "At(%s, %s)" % (self.prop, self.value)


class Lt(RelExpr):
    def __init__(self, prop=None, value=None):
        super().__init__(prop, value)

    def inv(self):
        return Gte(self.property, self.value)

    def str_rel_op(self):
        return "Lt"

    def python_rel_op(self):
        return "<"


class Lte(RelExpr):
    def __init__(self, prop=None, value=None):
        super().__init__(prop, value)

    def inv(self):
        return Gt(self.property, self.value)

    def str_rel_op(self):
        return "Lte"

    def python_rel_op(self):
        return "<="


class Gt(RelExpr):
    def __init__(self, prop=None, value=None):
        super().__init__(prop, value)

    def inv(self):
        return Lte(self.property, self.value)

    def str_rel_op(self):
        return "Gt"

    def python_rel_op(self):
        return ">"


class Gte(RelExpr):
    def __init__(self, prop=None, value=None):
        super().__init__(prop, value)

    def inv(self):
        return Lt(self.property, self.value)

    def str_rel_op(self):
        return "Gte"

    def python_rel_op(self):
        return ">="


class Eq(RelExpr):
    def __init__(self, prop=None, value=None):
        super().__init__(prop, value)

    def inv(self):
        return Ne(self.property, self.value)

    def str_rel_op(self):
        return "Eq"

    def python_rel_op(self):
        return "=="


class Ne(RelExpr):
    def __init__(self, prop=None, value=None):
        super().__init__(prop, value)

    def inv(self):
        return Eq(self.property, self.value)

    def str_rel_op(self):
        return "Ne"

    def python_rel_op(self):
        return "!="


# mozda eventualno dodati i neki tip
class Property(IRObject):
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name

    def python_condition(self):
        return "l.%s" % self.name


class Value(IRObject):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str(self.value)

    def python_condition(self):
        return str(self.value)


class IntValue(Value):
    def __init__(self, value):
        super().__init__(value)

    # def __str__(self):
    #     return "%d" % int(self.value)
    #
    # def python_condition(self):
    #     return "%d" % int(self.value)


class DoubleValue(Value):
    def __init__(self, value):
        super().__init__(value)

    # def __str__(self):
    #     return "%f" % float(self.value)
    #
    # def python_condition(self):
    #     return "%f" % float(self.value)


class StringValue(Value):
    def __init__(self, value):
        super().__init__(value)

    def python_condition(self):
        return "\"%s\"" % self.value


class RegExprValue(Value):
    def __init__(self, value):
        super().__init__(value)

    def python_condition(self):
        return 're.compile("%s")' % self.value


class DatetimeValue(Value):
    def __init__(self, value):
        super().__init__(value)

    def __getitem__(self, item):
        if isinstance(item, str):
            if item == "start_time":
                return self.value.start_time
            elif item == "end_time":
                return self.value.end_time
            else:
                raise AttributeError("DateTimeValue object does not have attribute %s." % item)
        elif isinstance(item, int):
            if item == 0:
                return self.value.start_time
            elif item == 1:
                return self.value.end_time
            else:
                raise IndexError("Index out of range: %d" % item)
        else:
            raise AttributeError("DateTimeValue does not have attribute: %s" % str(item))

    def __str__(self):
        if isinstance(self.value, DateTimeInterval):
            return "(start: %s, end: %s)" % (self.value.start_time, self.value.end_time)
        elif isinstance(self.value, datetime.datetime):
            return "%s" % self.value

    def python_condition(self):
        if isinstance(self.value, DateTimeInterval):
            # FIXME: try to find better solution, this one is made in 2:42am
            return "date_parser.parse(\"%s\")" % self.value.start_time
        elif isinstance(self.value, datetime.datetime):
            return "date_parser.parse(\"%s\")" % self.value


class TimeOffset(Value):
    def __init__(self, value):
        super().__init__(value)


class Header(IRObject):
    def __init__(self, first_expr=None):
        self.header_expressions = []
        if first_expr is not None:
            self.add_header_expression(first_expr)

    def add_header_expression(self, expr):
        self.header_expressions.append(expr)
        return self

    def semantic_analysis(self, log_parser):
        expr_dict = {}
        count_found = False
        last_found = False
        for expr in self.header_expressions:
            # each header expr can appear only once in header
            if expr.get_type() in expr_dict:
                raise AttributeError("%s appears multiple times in header." % expr.get_type())
            expr.semantic_analysis(log_parser)
            expr_dict[expr.get_type()] = expr
            if expr.get_type() == COUNT_STR:
                count_found = True
            elif expr.get_type() == LAST_STR:
                last_found = True
        if last_found and not count_found:
            raise Exception('Last specified, but not count.')

    def __str__(self):
        out_str = ""
        for expr in self.header_expressions:
            out_str += str(expr) + ", "
        if out_str:
            out_str = out_str[:-2]
        return out_str


class HeaderExpr(IRObject):
    def get_type(self):
        return ""


class CountExpr(HeaderExpr):
    def __init__(self, count):
        self.count = count

    def __str__(self):
        return "count(%s)" % self.count

    def get_type(self):
        return COUNT_STR

    def semantic_analysis(self, log_parser):
        if self.count.value > 0 and isinstance(self.count, IntValue):
            return
        raise ValueError("Count must be positive integer")


class LastExpr(HeaderExpr):
    def __init__(self, time_offset_seconds):
        self.time_offset_seconds = time_offset_seconds

    def __str__(self):
        return "last(%ss)" % self.time_offset_seconds

    def get_type(self):
        return LAST_STR

    def semantic_analysis(self, log_parser):
        if self.time_offset_seconds <= 0:
            raise ValueError("Last value must represents the positive number of seconds offset.")


from utils_functions import convert_timedelta_offset_to_seconds, calculate_timedelta_offset, \
    YearInterval, MonthInterval, DayInterval, HourInterval, \
    MinuteInterval, SecondInterval, \
    DateTimeInterval

actions = {
    # AlarmQuery:
    #     Query
    #     | Query SEMICOLON Header
    # ;
    "AlarmQuery": [
        lambda _, nodes: AlarmQuery(nodes[0]),
        lambda _, nodes: AlarmQuery(nodes[0], nodes[2]),
    ],
    # Query:
    #     Expr
    #     | Query or_op Expr;;
    "Query": [
        lambda _, nodes: nodes[0],
        lambda _, nodes: Or(nodes[0], nodes[2])
    ],
    # Expr:
    #     NonTerm
    #     | Expr and_op NonTerm;
    "Expr": [
        lambda _, nodes: nodes[0],
        lambda _, nodes: And(nodes[0], nodes[2])
    ],
    # NonTerm:
    #    Term
    #    | not_op Term;
    "NonTerm": [
        lambda _, nodes: nodes[0],
        lambda _, nodes: Not(nodes[1])
    ],
    # Term:
    #     AtomicExpr
    #     | LPAREN Query RPAREN;
    "Term": [
        lambda _, nodes: nodes[0],
        lambda _, nodes: nodes[1]
    ],

    # Property rel_op RelValue
    "RelExpr": lambda _, nodes: nodes[1].set_property_and_value(nodes[0], nodes[2]),
    # Property reg_expr_op REG_EXPR
    "RegExpr": lambda _, nodes: RegExpr(nodes[1].set_property_and_value(nodes[0], nodes[2])),

    # Property at_op Datetime
    "AtExpr": lambda _, nodes: And(
        Gte(nodes[0], DatetimeValue(nodes[2]["start_time"])),
        Lt(nodes[0], DatetimeValue(nodes[2]["end_time"]))
    ),
    # Header:
    #     HeaderExpr
    #     | Header COMMA HeaderExpr
    # ;
    "Header": [
        lambda _, nodes: Header(nodes[0]),
        lambda _, nodes: nodes[0].add_header_expression(nodes[2]),
    ],
    # HeaderExpr:
    #     CountExpr
    #     | LastExpr
    # ;
    "HeaderExpr": lambda _, nodes: nodes[0],
    # CountExpr:
    #     COUNT LPAREN INT RPAREN
    # ;
    "CountExpr": lambda _, nodes: CountExpr(nodes[2]),
    # LastExpr:
    #     COUNT LPAREN TIME_OFFSET RPAREN
    # ;
    "LastExpr": lambda _, nodes: LastExpr(nodes[2]),

    # terminals
    "Property": lambda _, nodes: Property(nodes[0]),
    # HASH DatetimeValue HASH
    "Datetime": lambda _, nodes: nodes[1],
    # operators
    "lt_op": lambda _, value: Lt(),
    "lte_op": lambda _, value: Lte(),
    "gt_op": lambda _, value: Gt(),
    "gte_op": lambda _, value: Gte(),
    "eq_op": lambda _, value: Eq(),
    "ne_op": lambda _, value: Ne(),
    # literals
    "INT": lambda _, value: IntValue(int(value)),
    "DOUBLE": lambda _, value: DoubleValue(float(value)),
    "STRING": lambda _, value: StringValue(value[1:-1]),
    "REG_EXPR": lambda _, value: RegExprValue(value[1:-1]),
    # datetime literals
    "YEAR": lambda _, value: DatetimeValue(YearInterval("%s-01-01T00:00:00" % (value))),
    "YEAR_MONTH": lambda _, value: DatetimeValue(MonthInterval("%s-01T00:00:00" % (value))),
    "YEAR_MONTH_DAY": lambda _, value: DatetimeValue(DayInterval("%sT00:00:00" % (value))),
    "YEAR_MONTH_DAY_HOUR": lambda _, value: DatetimeValue(HourInterval("%s:00:00" % (re.sub("\s+", "T", value)))),
    "YEAR_MONTH_DAY_HOUR_MINUTE": lambda _, value: DatetimeValue(MinuteInterval("%s:00" % (re.sub("\s+", "T", value)))),
    "YEAR_MONTH_DAY_HOUR_MINUTE_SECOND": lambda _, value: DatetimeValue(
        SecondInterval("%s" % (re.sub("\s+", "T", value)))),
    "TIME_OFFSET": lambda _, value: convert_timedelta_offset_to_seconds(calculate_timedelta_offset(value))
}


def compile_alarm_python_condition(alarm_str, log_format):
    g = Grammar.from_file('alarm_language.pg')
    # no actions for now
    p = Parser(g, actions=actions)

    res = p.parse(alarm_str)
    # print(res)
    res.remove_not()
    res.semantic_analysis(build_log_parser(log_format))
    res = res.python_condition()
    return res


def front_end_alarm_compiler(alarm_str, log_format):
    """
        This function represents front-end of alarm compiler. It does:
            - syntax analysis
            - semantic analysis
            - generate IR
            - optimizing IR
    :param alarm_str:
    :param log_format:
    :return: object of AlarmQuery class
    """
    g = Grammar.from_file('alarm_language.pg')
    # no actions for now
    p = Parser(g, actions=actions, debug=False)

    res = p.parse(alarm_str)
    # print(res)
    res.remove_not()
    res.semantic_analysis(build_log_parser(log_format))
    return res


def test_integration():
    log_format = """
        severity:=int; 
        facility:=int; 
        message:=string;
        timestamp:=datetime(/\d{2}\.\d{2}\.\d{4}\.\s+\d{2}\:\d{2}\:\d{2}/);
        scaling:=double;

        <timestamp> </\s*,\s*/> <severity> </\s*,\s*/> <facility> </\s*,\s*/> <scaling> </\s*,\s*/> <message> 
        """
    log_str = '20.02.1995. 20:45:00, 3, 1, 1.5, "Ovo je moja pozdravna poruka, Vladimire"'
    alarm_str = "scaling > 2 or severity<5 and facility >=0"

    log_format = """
        brojka:=int;
        </</> <brojka> </>/> </.*/>
    """
    log_str = '<11>1 2019-04-08T01:08:12+02:00 12.12.12.1 FakeWebApp - msg77 - from:192.52.223.99 "GET /recipe HTTP/1.0" 200 4923 "Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10_12_6) AppleWebKit/5361 (KHTML, like Gecko) Chrome/53.0.892.0 Safari/5361 "'
    alarm_str = 'brojka == 11 or brojka > 12; count(11), last(12s)'

    lp = build_log_parser(log_format)
    l = lp.parse_log(log_str)
    print(l)

    res = front_end_alarm_compiler(alarm_str, log_format)
    print(res)

    # py_cond = compile_alarm_python_condition(alarm_str, log_format)
    # print(py_cond)
    # print(eval(py_cond))
    pass


if __name__ == '__main__':
    # g = Grammar.from_file('alarm_language.pg')
    # # no actions for now
    # p = Parser(g, actions=actions)
    #
    # # res = p.parse("ceca@#1234# or mica==1231 and ceca>#1234-12# and not mica>=-123. or celka==-0.123")
    # # res = p.parse("not (micko > -.123 or not cele==/.*/) and mile@#2014#")
    # # print("Original: %s" % res)
    # # res.remove_not()
    # # print("De Morganovi zakoni: %s" % res)
    #
    # # res = p.parse("ceca==123 and (mica==321 or nica==123)")
    # # print(res)
    # #
    # # # res = p.parse("ceca==/123/ and not (mica!=/123/ or nica==/1\"\.23/)")
    # # res = p.parse("ceca > #2014# and mile @ #2015# ")
    # # print(res)
    # # res = res.remove_not()
    # # print(res)
    # # res = res.python_condition()
    # # print(res)
    #
    # from collections import namedtuple
    #
    # Log = namedtuple("Log", ["severity", "timestamp", "name", "message"])
    # l = Log(severity=10, timestamp=date_parser.parse("05.06.2014"), name="majka", message="moja")
    #
    # # res = p.parse("severity > 7 and timestamp@#2014# or name==/m.*e/")
    #
    # res = p.parse("a > 7.123 and b==123 or p>3.0")
    # # print(res)
    # res.remove_not()
    # res.semantic_analysis(build_log_parser(primer))
    # res = res.python_condition()
    # print(res)
    # # print(eval(res))

    test_integration()
