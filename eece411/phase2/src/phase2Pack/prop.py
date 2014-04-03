import commands
import paramiko
import os
import sys
import datetime
import time
from collections import defaultdict
from time import localtime, strftime

ServerJar='/Server.jar'
remotePath='~/'
username = 'ubc_EECE411_S9'
password = 'leonrockyk'

def scpJar(hostname):
    print hostname
    privatekeyfile = os.path.expanduser('~/eece411/.ssh/id_rsa')
    mykey = paramiko.RSAKey.from_private_key_file(privatekeyfile, password=password)
    stdout_data = ''
    try:
        client = paramiko.Transport((hostname, port))
        client.connect(username=username,pkey = mykey)
        sftp = paramiko.SFTPClient.from_transport(client)
        sftp.put(ServerJar, remotePath)
    except:
        stdout_data='server fail'

    return stdout_data

def main():
    nodeList = [line.strip() for line in open('/ubc/ece/home/ugrads/a/a2m7/etc/www/nodeList.txt')]
    for hostname in nodeList:
        try:
            log=scpJar(hostname)
        except:
            print "wtf\n"
            next

main()
