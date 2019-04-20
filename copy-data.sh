# log_formatter and its grammar
docker cp log_formatter.pg spark-master:/logs
docker cp log_formatter.py spark-master:/logs

# generated python file
#docker cp generated/test-generated.py spark-master:/logs
#docker cp tests/mozda-uspe.py spark-master:/logs
docker cp generated/py_cond_generated.py spark-master:/logs
docker cp generated/py_cond_count_generated.py spark-master:/logs
