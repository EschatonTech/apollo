#!/usr/bin/env python3

import sys
import socket
import struct
import time
import os
from threading import Thread
sys.path.append("../")
from cyber_py3 import cyber
from modules.prediction.proto.prediction_obstacle_pb2 import PredictionObstacles


TCP_IP = '10.0.0.10' #ip address and port to connect to Eschaton Hardware
TCP_PORT = 5005

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


def send(data):

    message = data.SerializeToString()
    packed_len = struct.pack('>L', len(message))
    sock.sendall(packed_len + message)


def socket_read_n(sock, n):

    """ Read exactly n bytes from the socket.
        Raise RuntimeError if the connection closed before
        n bytes were read.
    """
    buf = str.encode('')
    while n > 0:
        data = sock.recv(n)
        if data == '':
            raise RuntimeError('unexpected connection close')
        buf += data
        n -= len(data)
    return buf


def send():

    sendNode = cyber.Node("eschaton-bridge-send")
    sendNode.create_reader("/apollo/prediction", PredictionObstacles, send)
    sendNode.spin()


def receive():

    receiveNode = cyber.Node("eschaton-bridge-receive")
    writer = receiveNode.create_writer("/apollo/prediction/eschaton", PredictionObstacles)

    while 1:
        len_buf = socket_read_n(sock, 4)
        msg_len = struct.unpack('>L', len_buf)[0]
        msg_buf = socket_read_n(sock, msg_len)
        message = PredictionObstacles()
        message.ParseFromString(msg_buf)
        writer.write(message)
    conn.close()


if __name__ == '__main__':

    cyber.init()

    while True:
        try:
            sock.connect((TCP_IP, TCP_PORT))
            break
        except Foo:
            print("Eschaton Hardware not connected, retrying in 5 seconds...")
            time.sleep(5)

    print("connected to Eschaton Hardware at:", TCP_IP)

    t1 = Thread(target = send)
    t2 = Thread(target = receive)

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    cyber.shutdown()
    sock.close()
