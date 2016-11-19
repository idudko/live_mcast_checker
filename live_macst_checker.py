#!/usr/bin/python3
import logging
import socket
import struct
import signal
import threading
import sys
import subprocess
import argparse
import queue

__author__ = "Ivan Dudko"
__email__ = "ivan.dudko@gmail.com"
__status__ = "Production"
__version__ = "1.0.1"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description='Simple script for checking live multicast stream data size'
                                             ' and send it to Zabbix monitoring tool')
parser.add_argument('-i', '--multicat_group', dest='multicat_group', help='UDP multicast group to check,'
                                                                          ' e.g. 239.2.12.1:1234', required=True)
parser.add_argument('-z', '--zabbix_server', help='Zabbix server', required=True)
parser.add_argument('-s', '--zabbix_server_name', help='Zabbix server name', required=True)
parser.add_argument('-k', '--zabbix_key', help='Zabbix key', required=True)

args = parser.parse_args()


def parse_ip_port(addr):
    ip, separator, port = addr.rpartition(':')
    assert separator  # separator (`:`) must be present
    return ip, port


# Constants
MTU = 1500
INTERVAL = 1
ZABBIX_SERVER_NAME = args.zabbix_server_name
ZABBIX_KEY = args.zabbix_key
ZABBIX_SERVER = args.zabbix_server
MCAST_GRP, MCAST_PORT = parse_ip_port(args.multicat_group)

# Join multicast group
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind((MCAST_GRP, int(MCAST_PORT)))
mreq = struct.pack('=4sl', socket.inet_aton(MCAST_GRP), socket.INADDR_ANY)
sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)


def print_stats():
    timer = threading.Timer(INTERVAL * 1.0, print_stats)
    timer.daemon = True
    timer.start()
    proc_params = ['zabbix_sender', '-z', ZABBIX_SERVER, '-s', ZABBIX_SERVER_NAME, '-k', ZABBIX_KEY, '-o', '0']
    if not comm_queue.empty():
        proc_params[8] = str(comm_queue.get() * 8)
    logger.info('Current bitrate %s', proc_params[8])

    try:
        subprocess.call(proc_params)
    except IOError as e:
        logger.error(e)
        sys.exit(1)


# Catch Ctrl+C and exit after multicast group leave
def signal_handler(signal, frame):
    print('You pressed Ctrl+C! Exiting..')
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, mreq)
    sock.close()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
comm_queue = queue.Queue(maxsize=1)
print_stats()


# main loop for recieve data from multicast group
while True:
    data = sock.recv(MTU)
    if data:
        tmp = 0
        if not comm_queue.empty():
            tmp = comm_queue.get()
        comm_queue.put(tmp + len(data))
