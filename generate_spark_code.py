from alarm_language import front_end_alarm_compiler
import os

TEMPLATES_PATH = 'templates'
PY_COND_TEMPLATE_PATH = os.path.join(TEMPLATES_PATH, 'py_cond_template.py')
PY_COND_COUNT_TEMPLATE_PATH = os.path.join(TEMPLATES_PATH, 'py_cond_count_template.py')
PY_COND_COUNT_LAST_TEMPLATE_PATH = os.path.join(TEMPLATES_PATH, 'py_cond_count_last_template.py')

GENERATED_PATH = 'generated'
PY_COND_GENERATED_PATH = os.path.join(GENERATED_PATH, 'py_cond_generated.py')
PY_COND_COUNT_GENERATED_PATH = os.path.join(GENERATED_PATH, 'py_cond_count_generated.py')
PY_COND_COUNT_LAST_GENERATED_PATH = os.path.join(GENERATED_PATH, 'py_cond_count_last_generated.py')

log_format_str = """
        brojka:=int;
        </</> <brojka> </>/> </.*/>
    """
alarm_str = 'not(brojka != 11 and brojka != 13); count(11), last(12s)'

ENUM_PY_COND = 'PY_COND'
ENUM_PY_COND_COUNT = 'PY_COND_COUNT'
ENUM_PY_COND_COUNT_LAST = 'PY_COND_COUNT_LAST'

COUNT_STR = 'COUNT'
LAST_STR = 'LAST'
GROUP_BY_STR = 'GROUP_BY'


def find_proper_template_type(alarm_query):
    if alarm_query.header is None:
        return ENUM_PY_COND
    count_expr_exist = False
    last_expr_exist = False
    group_by_expr_exist = False
    for expr in alarm_query.header.header_expressions:
        if expr.get_type() == COUNT_STR:
            count_expr_exist = True
        elif expr.get_type() == LAST_STR:
            last_expr_exist = True
        elif expr.get_type() == GROUP_BY_STR:
            group_by_expr_exist = True
        else:
            raise TypeError('Unknown header expression type')
    if count_expr_exist and not last_expr_exist and not group_by_expr_exist:
        return ENUM_PY_COND_COUNT
    elif count_expr_exist and last_expr_exist and not group_by_expr_exist:
        return ENUM_PY_COND_COUNT_LAST
    else:
        raise ValueError('Still not implemented case scenarios')


def get_py_cond_str_template_tupple(log_format_grammar, alarm_query):
    return log_format_grammar, alarm_query.python_condition()


def get_py_cond_count_str_template_tupple(log_format_grammar, alarm_query):
    count_value = alarm_query.header.header_expressions[0].count.value
    return log_format_grammar, count_value, count_value, alarm_query.python_condition()


def get_py_cond_count_last_str_template_tupple(log_format_grammar, alarm_query):
    # count_value = alarm_query.header.header_expressions[0].count.value
    count_value = 0
    last_second_value = 0
    for expr in alarm_query.header.header_expressions:
        if expr.get_type() == COUNT_STR:
            count_value = expr.count.value
        elif expr.get_type() == LAST_STR:
            last_second_value = expr.time_offset_seconds
    return log_format_grammar, count_value, alarm_query.python_condition(), last_second_value


def generate_spark_code(log_format_grammar, alarm_query, template_path, generate_path, get_template_tupple_func):
    with open(template_path, 'r') as template_file:
        template_str = template_file.read()
        generate_str = template_str % get_template_tupple_func(log_format_grammar, alarm_query)
        with open(generate_path, 'w') as generate_file:
            generate_file.write(generate_str)


def generator(log_format_grammar, alarm_definition_str):
    alarm_query = front_end_alarm_compiler(alarm_definition_str, log_format_grammar)
    template_type = find_proper_template_type(alarm_query)
    if template_type == ENUM_PY_COND:
        generate_spark_code(log_format_grammar, alarm_query, PY_COND_TEMPLATE_PATH, PY_COND_GENERATED_PATH,
                            get_py_cond_str_template_tupple)
    elif template_type == ENUM_PY_COND_COUNT:
        generate_spark_code(log_format_grammar, alarm_query, PY_COND_COUNT_TEMPLATE_PATH, PY_COND_COUNT_GENERATED_PATH,
                            get_py_cond_count_str_template_tupple)
    elif template_type == ENUM_PY_COND_COUNT_LAST:
        generate_spark_code(log_format_grammar, alarm_query, PY_COND_COUNT_LAST_TEMPLATE_PATH,
                            PY_COND_COUNT_LAST_GENERATED_PATH,
                            get_py_cond_count_last_str_template_tupple)
    else:
        raise ValueError('Still not implemented')


if __name__ == '__main__':
    generator(log_format_str, alarm_str)
