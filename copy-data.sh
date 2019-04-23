# log_formatter and its grammar
docker cp log_formatter.pg spark-master:/logs
docker cp log_formatter.py spark-master:/logs

# generated python file
#docker cp generated/test-generated.py spark-master:/logs
#docker cp tests/mozda-uspe.py spark-master:/logs
#docker cp tests/py_cond_count_last.py spark-master:/logs
#docker cp tests/py_cond_count_group_by.py spark-master:/logs
#docker cp tests/py_cond_count_last_group_by.py spark-master:/logs

#docker cp generated/py_cond_generated.py spark-master:/logs
#docker cp generated/py_cond_count_generated.py spark-master:/logs
#docker cp generated/py_cond_count_last_generated.py spark-master:/logs
docker cp generated/py_cond_count_group_by_generated.py spark-master:/logs
docker cp generated/py_cond_count_last_group_by_generated.py spark-master:/logs
