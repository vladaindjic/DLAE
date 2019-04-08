from alarm_language import front_end_alarm_compiler
import os

TEMPLATES_PATH = 'templates'
JUST_PY_CONDITION_TEMPLATE_PATH = os.path.join(TEMPLATES_PATH, 'just_py_condition_template.py')

GENERATED_PATH = 'generated'
TEST_GENERATED_PATH = os.path.join(GENERATED_PATH, 'test-generated.py')

log_format_str = """
        brojka:=int;
        </</> <brojka> </>/> </.*/>
    """
alarm_str = 'not(brojka != 11 and brojka != 13)'


def generate_spark_code(log_format_grammar, alarm_definition_str):
    alarm_query = front_end_alarm_compiler(alarm_definition_str, log_format_grammar)
    with open(JUST_PY_CONDITION_TEMPLATE_PATH, 'r') as template_file:
        template_str = template_file.read()
        print(template_str)
        generate_str = template_str % (log_format_grammar, alarm_query.python_condition())
        with open(TEST_GENERATED_PATH, 'w')  as generate_file:
            generate_file.write(generate_str)


if __name__ == '__main__':
    generate_spark_code(log_format_str, alarm_str)
