from alarm_language import front_end_alarm_compiler
import os

TEMPLATES_PATH = 'templates'
PY_COND_TEMPLATE_PATH = os.path.join(TEMPLATES_PATH, 'py_cond_template.py')
PY_COND_COUNT_TEMPLATE_PATH = os.path.join(TEMPLATES_PATH, 'py_cond_count_template.py')
PY_COND_COUNT_LAST_TEMPLATE_PATH = os.path.join(TEMPLATES_PATH, 'py_cond_count_last_template.py')
PY_COND_COUNT_GROUP_BY_TEMPLATE_PATH = os.path.join(TEMPLATES_PATH, 'py_cond_count_group_by_template.py')
PY_COND_COUNT_LAST_GROUP_BY_TEMPLATE_PATH = os.path.join(TEMPLATES_PATH, 'py_cond_count_last_group_by_template.py')

GENERATED_PATH = 'generated'
PY_COND_GENERATED_PATH = os.path.join(GENERATED_PATH, 'py_cond_generated.py')
PY_COND_COUNT_GENERATED_PATH = os.path.join(GENERATED_PATH, 'py_cond_count_generated.py')
PY_COND_COUNT_LAST_GENERATED_PATH = os.path.join(GENERATED_PATH, 'py_cond_count_last_generated.py')
PY_COND_COUNT_GROUP_BY_GENERATED_PATH = os.path.join(GENERATED_PATH, 'py_cond_count_group_by_generated.py')
PY_COND_COUNT_LAST_GROUP_BY_GENERATED_PATH = os.path.join(GENERATED_PATH, 'py_cond_count_last_group_by_generated.py')

log_format_str = """
    priority        := int;
    version         := int(/\d/);
    timestamp       := datetime(/\d{4}\-\d{2}\-\d{2}T\d{2}\:\d{2}\:\d{2}\+\d{2}\:\d{2}/);
    _ws             := /\s+/;
    server_id       := string(/\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/);
    app_name        := string(/\w+/);
    _dash           := /\s+\-\s+/;
    msg_id          := string(/msg\d+/);
    workstation_id  := string(/\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/);
    
    /</ priority />/ version _ws timestamp _ws server_id _ws app_name _dash msg_id _dash _from:=/from:/ workstation_id _ws msg:=string(/.*/)

"""

alarm_str = 'version == 1 and (priority > 10 and priority <= 14) or not timestamp@#2018#; count(10, groupBy=[server_id, workstation_id], last=1m30s)'

ENUM_PY_COND = 'PY_COND'
ENUM_PY_COND_COUNT = 'PY_COND_COUNT'
ENUM_PY_COND_COUNT_LAST = 'PY_COND_COUNT_LAST'
ENUM_PY_COND_COUNT_GROUP_BY = 'PY_COND_COUNT_GROUP_BY'
ENUM_PY_COND_COUNT_LAST_GROUP_BY = 'PY_COND_COUNT_LAST_GROUP_BY'

COUNT_STR = 'COUNT'
LAST_STR = 'LAST'
GROUP_BY_STR = 'GROUP_BY'


def find_proper_template_type(alarm_query):
    if alarm_query.header is None:
        return ENUM_PY_COND
    count_expr = alarm_query.header.count_expr
    # for current implementation, this should be always true
    count_expr_exist = count_expr is not None
    if not count_expr_exist:
        return ENUM_PY_COND

    count_keyword_params = alarm_query.header.count_expr.count_expr_params.count_keyword_params
    # only count value is specified
    if not count_keyword_params:
        return ENUM_PY_COND_COUNT

    # check if last is specified
    last_param = count_keyword_params.last_param
    last_param_exist = last_param is not None
    # check if groupBy is specified
    group_by_param = count_keyword_params.group_by_param
    group_by_param_exist = group_by_param is not None

    if last_param_exist and group_by_param_exist:
        return ENUM_PY_COND_COUNT_LAST_GROUP_BY
    elif last_param_exist:
        return ENUM_PY_COND_COUNT_LAST
    elif group_by_param_exist:
        return ENUM_PY_COND_COUNT_GROUP_BY
    else:
        raise ValueError('Still not implemented case scenarios')


def get_py_cond_str_template_tupple(log_format_grammar, alarm_query):
    return log_format_grammar, alarm_query.python_condition()


def get_py_cond_count_str_template_tupple(log_format_grammar, alarm_query):
    count_value = alarm_query.header.count_expr.count_expr_params.count_param.count.value
    return log_format_grammar, count_value, count_value, alarm_query.python_condition()


def get_py_cond_count_last_str_template_tupple(log_format_grammar, alarm_query):
    # count_value = alarm_query.header.header_expressions[0].count.value
    count_value = alarm_query.header.count_expr.count_expr_params.count_param.count.value
    last_second_value = alarm_query.header.count_expr.count_expr_params.count_keyword_params.last_param.time_offset_seconds
    return log_format_grammar, count_value, alarm_query.python_condition(), last_second_value


def extract_key_template_and_key_parts_from_group_by_list(group_by_list):
    key_template = ""
    key_parts = ""
    key_part_ind = 0

    for prop in group_by_list.properties:
        key_template += ("{%d}_" % key_part_ind)
        key_parts += ("l.%s, " % prop.name)
        key_part_ind += 1
    # remove separators
    key_template = key_template[:-1]
    key_parts = key_parts[:-2]
    return key_template, key_parts


def get_py_cond_count_group_by_str_template_tupple(log_format_grammar, alarm_query):
    count_value = alarm_query.header.count_expr.count_expr_params.count_param.count.value
    group_by_list = alarm_query.header.count_expr.count_expr_params.count_keyword_params.group_by_param.group_by_list
    key_template, key_parts = extract_key_template_and_key_parts_from_group_by_list(group_by_list)
    return log_format_grammar, count_value, count_value, alarm_query.python_condition(), key_template, key_parts


def get_py_cond_count_last_group_by_str_template_tupple(log_format_grammar, alarm_query):
    count_value = alarm_query.header.count_expr.count_expr_params.count_param.count.value
    last_second_value = alarm_query.header.count_expr.count_expr_params.count_keyword_params.last_param.time_offset_seconds
    group_by_list = alarm_query.header.count_expr.count_expr_params.count_keyword_params.group_by_param.group_by_list
    key_template, key_parts = extract_key_template_and_key_parts_from_group_by_list(group_by_list)
    return log_format_grammar, count_value, alarm_query.python_condition(), key_template, key_parts, last_second_value


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
    elif template_type == ENUM_PY_COND_COUNT_GROUP_BY:
        generate_spark_code(log_format_grammar, alarm_query, PY_COND_COUNT_GROUP_BY_TEMPLATE_PATH,
                            PY_COND_COUNT_GROUP_BY_GENERATED_PATH,
                            get_py_cond_count_group_by_str_template_tupple)
    elif template_type == ENUM_PY_COND_COUNT_LAST_GROUP_BY:
        generate_spark_code(log_format_grammar, alarm_query, PY_COND_COUNT_LAST_GROUP_BY_TEMPLATE_PATH,
                            PY_COND_COUNT_LAST_GROUP_BY_GENERATED_PATH,
                            get_py_cond_count_last_group_by_str_template_tupple)
    else:
        raise ValueError('Still not implemented')


if __name__ == '__main__':
    generator(log_format_str, alarm_str)
