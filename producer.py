import socket

from kafka import KafkaProducer

MAX_MESSAGE_SIZE = 4096
PORT = 50000
TOPIC_NAME = "logs"


def main():
    # kafka cluster listens on 9094
    # we should encode data using utf-8
    producer = KafkaProducer(bootstrap_servers=['localhost:9094'],
                             value_serializer=lambda x:
                             x.encode('utf-8'))

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', PORT))
    s.listen(1)
    print("Producer starts")
    while True:
        # accpet message from log generator and send message to kafka cluster
        conn, addr = s.accept()
        data = conn.recv(MAX_MESSAGE_SIZE)
        if not data:
            break
        data_str = data.decode('utf-8')
        print(data_str)
        producer.send(TOPIC_NAME, value=data_str)
    conn.close()
    s.close()


if __name__ == '__main__':
    main()
