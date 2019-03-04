import datetime
import random
import socket
import time

import numpy
from faker import Faker
from rfc3339 import rfc3339
from tzlocal import get_localzone

facility = 1
levelMap = {}

PORT = 50000


def send_log(log_str):
    print(log_str)
    # simple send message to kafka producer
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', PORT))
    s.send(log_str.encode('utf-8'))
    s.close()


def getAndUpdateCounter():
    global counter
    counter += 1
    return "msg%s" % counter


def pri(logLevel):
    return facility * 8 + levelMap[logLevel]


def same_username_login_scenario(dt, logLevel, ip, byt, useragent):
    for i in range(9):
        ip = faker.ipv4()
        send_log(unsuccessful_login(dt, "Informational", ip, "503", byt, useragent, "same_username"))
    send_log(unsuccessful_login(dt, "Error", ip, "503", byt, useragent, "same_username"))
    # file.flush()
    return


def same_ip_login_scenario(dt, logLevel, ip, byt, useragent):
    for i in range(9):
        send_log(unsuccessful_login(dt, "Informational", ip, "503", byt, useragent, "Unsuccessful Login"))
    send_log(unsuccessful_login(dt, "Error", ip, "503", byt, useragent, "Unsuccessful Login same IP"))
    # file.flush()
    return


def unsuported_ip_alert(dt, logLevel, ip, byt, useragent):
    methods = ["GET", "POST", "PUT", "DELETE"]
    method = numpy.random.choice(methods, p=[0.6, 0.15, 0.15, 0.1])
    send_log(
        '<%s>1 %s %s %s - %s - from:%s "%s %s HTTP/1.0" %s %s "%s Unsuported IP Address"\n' % (
            pri("Error"), dt, hostmachine, applicationName, getAndUpdateCounter(),
            ip, method, "/recipe", "404", byt,
            useragent))
    # file.flush()
    return


def random_scenario(dt, logLevel, ip, byt, useragent):
    options = [search_recipe, search_recipes, add_recipe, update_recipe, delete_recipe]
    send_log(successful_login(dt, logLevel, ip, "200", byt, useragent))
    for i in range(random.randint(2, 10)):
        resp = numpy.random.choice(response, p=[0.9, 0.04, 0.02, 0.04])
        logLevel = numpy.random.choice(level, p=[0, 0, 0.01, 0.02, 0.1, 0.07, 0.5, 0.3])
        send_log(
            numpy.random.choice(options, p=[0.15, 0.35, 0.2, 0.25, 0.05])(dt, logLevel, ip, resp, byt, useragent))
    send_log(successful_logout(dt, logLevel, ip, "200", byt, useragent))
    # file.flush()
    return


def search_recipes(dt, logLevel, ip, resp, byt, useragent, message=""):
    return '<%s>1 %s %s %s - %s - from:%s "%s %s HTTP/1.0" %s %s "%s %s"\n' % (
        pri("Error"), dt, hostmachine, applicationName, getAndUpdateCounter(),
        ip, "GET", "/recipe", resp, byt,
        useragent, message)


def search_recipe(dt, logLevel, ip, resp, byt, useragent, message=""):
    return '<%s>1 %s %s %s - %s - from:%s "%s %s HTTP/1.0" %s %s "%s %s"\n' % (
        pri("Error"), dt, hostmachine, applicationName, getAndUpdateCounter(),
        ip, "GET", "/recipe/" + str(random.randint(1000, 10000)), resp, byt,
        useragent, message)


def add_recipe(dt, logLevel, ip, resp, byt, useragent, message=""):
    return '<%s>1 %s %s %s - %s - from:%s "%s %s HTTP/1.0" %s %s "%s %s"\n' % (
        pri(logLevel), dt, hostmachine, applicationName, getAndUpdateCounter(),
        ip, "POST", "/recipe", resp, byt, useragent, message)


def update_recipe(dt, logLevel, ip, resp, byt, useragent, message=""):
    return '<%s>1 %s %s %s - %s - from:%s "%s %s HTTP/1.0" %s %s "%s %s"\n' % (
        pri(logLevel), dt, hostmachine, applicationName, getAndUpdateCounter(),
        ip, "PUT", "/recipe/" + str(random.randint(1000, 10000)), resp, byt,
        useragent, message)


def delete_recipe(dt, logLevel, ip, resp, byt, useragent, message=""):
    return '<%s>1 %s %s %s - %s - from:%s "%s %s HTTP/1.0" %s %s "%s %s"\n' % (
        pri(logLevel), dt, hostmachine, applicationName, getAndUpdateCounter(),
        ip, "DELETE", "/recipe/" + str(random.randint(1000, 10000)), resp, byt,
        useragent, message)


def successful_login(dt, logLevel, ip, resp, byt, useragent, message=""):
    return '<%s>1 %s %s %s - %s - from:%s "%s %s HTTP/1.0" %s %s "%s %s"\n' % (
        pri(logLevel), dt, hostmachine, applicationName, getAndUpdateCounter(),
        ip, "POST", "/login", resp, byt, useragent, message)


def successful_logout(dt, logLevel, ip, resp, byt, useragent, message=""):
    return '<%s>1 %s %s %s - %s - from:%s "%s %s HTTP/1.0" %s %s "%s %s"\n' % (
        pri(logLevel), dt, hostmachine, applicationName, getAndUpdateCounter(),
        ip, "POST", "/logout", resp, byt, useragent, message)


def unsuccessful_login(dt, logLevel, ip, resp, byt, useragent, message=""):
    return '<%s>1 %s %s %s - %s - from:%s "%s %s HTTP/1.0" %s %s "%s %s"\n' % (
        pri(logLevel), dt, hostmachine, applicationName, getAndUpdateCounter(),
        ip, "POST", "/login", resp, byt, useragent, message)


def provide_timestamp():
    """
        added by Vlada
    :return:
    """
    timestamp = time.time()
    dt = datetime.datetime.fromtimestamp(timestamp)
    timestamp = rfc3339(dt)
    return timestamp


local = get_localzone()
faker = Faker()

timestr = time.strftime("%Y%m%d-%H%M%S")
otime = datetime.datetime.now()
outFileName = 'web_app_log' + timestr + '.log'
hostmachine = "12.12.12.1"
applicationName = "FakeWebApp"

response = ["200", "404", "500", "301"]
facility = 1
level = ["Emergency", "Alert", "Critical", "Error", "Warning", "Notice", "Informational", "Debug"]
levelMap = {"Emergency": 0, "Alert": 1, "Critical": 2, "Error": 3, "Warning": 4, "Notice": 5, "Informational": 6,
            "Debug": 7}
resources_and_scenarios = [same_ip_login_scenario, same_username_login_scenario, unsuported_ip_alert,
                           random_scenario]
ualist = [faker.firefox, faker.chrome, faker.safari, faker.internet_explorer, faker.opera]

flag = True

counter = 0
while flag:

    increment = datetime.timedelta(seconds=random.randint(1, 5))
    otime += increment

    dt = otime.strftime('%Y-%m-%dT%H:%M:%S.%f')
    # ms+02:00
    tz = datetime.datetime.now(local).strftime('%z')
    # dt = dt + tz
    dt = provide_timestamp()
    logLevel = numpy.random.choice(level, p=[0, 0, 0.01, 0.02, 0.1, 0.07, 0.5, 0.3])
    ip = faker.ipv4()
    byt = int(random.gauss(5000, 50))

    useragent = numpy.random.choice(ualist, p=[0.3, 0.5, 0.1, 0.05, 0.05])()

    numpy.random.choice(resources_and_scenarios, p=[0.05, 0.1, 0.05, 0.8])(dt, logLevel, ip, byt, useragent)

    if increment:
        time.sleep(1)

# <%s(pri)>1 %s(time stamp) %s(hostname) %s(aplication name) - %s(messageid) - %s(poruka)

# TODO: pip install rfc3339
