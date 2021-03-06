#!/ubc/ece/home/ugrads/a/a2m7/bin/python

import commands
import paramiko
import os
import sys
import datetime
import time
from collections import defaultdict
from time import localtime, strftime
import gviz_api
import json

nbytes = 4096
port = 22
username = 'ubc_EECE411_S9' 
password = 'leonrockyk'
commands = ['df -h']
DataDf = []
DataDescriptionDf ={ "node":("string","Node"),"diskUsed":("number","DiskUsed")}
DataDu = []
DataDescriptionDu ={ "node":("string","Node"),"diskFree":("number","DiskFree")}
NodeFail = []
NodeFailDescription = {"node":("string","Node"),"cause":("string","Causes")}
results={}
DataUptime = []
DataUptimeDescription = {"node":("string","Node"),"uptime":("number","Uptime")}

class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

class NestedDict(dict):
    def __getitem__(self, key):
        if key in self: return self.get(key)
        return self.setdefault(key, NestedDict())

def sshConnect(hostname):
	print hostname	
	privatekeyfile = os.path.expanduser('~/eece411/.ssh/id_rsa')
	mykey = paramiko.RSAKey.from_private_key_file(privatekeyfile, password=password)
	stdout_data = ''
	try:
	    client = paramiko.Transport((hostname, port))
	    client.connect(username=username,pkey = mykey)

	    stderr_data = []
	    session = client.open_channel(kind='session')
	    for idx, command in enumerate(commands):
	    	session.exec_command(command)
		while True:
			if session.recv_ready():
			    stdout_data = session.recv(nbytes)
			    if stdout_data.split()[-1]=='exiting':
				stdout_data = 'login fail' 
			if session.recv_stderr_ready():
			    stderr_data.append(session.recv_stderr(nbytes))
			if session.exit_status_ready():
			    break
	    #print 'exit status: ', session.recv_exit_status()
	    session.close()
	    client.close()
	except paramiko.AuthenticationException:
	    stdout_data='Autohentication fail'
	except:
	    stdout_data='server fail'
	
	try:
		gotdata = stdout_data.split('\n')
    		secRow = gotdata[1]
	except IndexError:
    		stdout_data = 'login fail'

	return stdout_data

def sshUptime(hostname):
        privatekeyfile = os.path.expanduser('~/eece411/.ssh/id_rsa')
        mykey = paramiko.RSAKey.from_private_key_file(privatekeyfile, password=password)
        stdout_data = ''
        try:
            client = paramiko.Transport((hostname, port))
            client.connect(username=username,pkey = mykey)

            stderr_data = []
            session = client.open_channel(kind='session')
            for idx, command in enumerate(commands):
                session.exec_command('uptime')
                while True:
                        if session.recv_ready():
                            stdout_data = session.recv(nbytes)
                            if stdout_data.split()[-1]=='exiting':
                                stdout_data = 'login fail'
                        if session.recv_stderr_ready():
                            stderr_data.append(session.recv_stderr(nbytes))
                        if session.exit_status_ready():
                            break
            #print 'exit status: ', session.recv_exit_status()
            session.close()
            client.close()
        except paramiko.AuthenticationException:
            stdout_data='Autohentication fail'
        except:
            stdout_data='server fail'

	if stdout_data != 'server fail' and stdout_data != 'Autohentication fail':
		try:
			gotdata = stdout_data.split('\n')
			secRow = gotdata[1]
		except IndexError:
			stdout_data = 'login fail'

        return stdout_data

def parseDf(node,data):
	global DataDf
	global DataDu

	words = data.split('\n')
 	diskUsage = words[1].split()

	#print diskUsage
	tempUsed = {"node":node,
		"diskUsed":(float(diskUsage[1].strip("G"))-float(diskUsage[3].strip("G")))}
	tempFree = {"node":node,
		"diskFree":float(diskUsage[3].strip("G"))}
    	#print node +" disk used "+str(tempUsed['diskUsed'])
	DataDf.append(tempUsed)
	DataDu.append(tempFree)

def failureReport(hostname,cause):
	global NodeFail
	
	temp = {"node":hostname,"cause":cause}	
	
	NodeFail.append(temp)    

def parseUptime(node, uptime):
	global DataUptime

        words = uptime.split('\n')
        time = words[0].split()

        #print uptime
	#time[2] is day, time[4] is hr:min
	#print time
        try:
                hrMin = time[4].split(':')
                #print hrMin
		minutes = hrMin[1]
		totalUptime = int(time[2])*24+int(hrMin[0])
        except IndexError:
		hrMin = time[2].split(':')
		totalUptime = int(hrMin[0])

	#print totalUptime

      	temp = {"node":node,
                "uptime":float(totalUptime)}

        DataUptime.append(temp)	

def main():
	global results
	results = NestedDict()
	nodeList = [line.strip() for line in open('nodeList.txt')]
	for hostname in nodeList:
		try:
			diskData = sshConnect(hostname)
			if diskData == "server fail" or diskData == "Autohentication fail" or diskData == "login fail":
				failureReport(hostname, diskData)
				next	
			else:
			    sysUptime = sshUptime(hostname)
			    parseDf(hostname, diskData)
			    parseUptime(hostname, sysUptime)	
		except:
			failureReport(hostname, 'miscellious failure')
			next
	#json format for disk usage
	disk_usage_table = gviz_api.DataTable(DataDescriptionDf)
	disk_usage_table.LoadData(DataDf)

	results['disk_usage_json']=disk_usage_table.ToJSon(columns_order=("node","diskUsed"),order_by=("node"))

	#json format for disk free
        disk_free_table = gviz_api.DataTable(DataDescriptionDu)
        disk_free_table.LoadData(DataDu)

        results['disk_free_json']=disk_free_table.ToJSon(columns_order=("node","diskFree"),order_by=("node"))

	#log uptime	
	uptime_table = gviz_api.DataTable(DataUptimeDescription)
        uptime_table.LoadData(DataUptime)

        results['uptime_data_json']=uptime_table.ToJSon(columns_order=("node","uptime"),order_by=("node"))

	#log failure
	fail_table = gviz_api.DataTable(NodeFailDescription)
	fail_table.LoadData(NodeFail)

	results['node_fail_json']=fail_table.ToJSon(columns_order=("node","cause"),order_by=("node"))
         #log time

	results['time']=strftime("%Y-%m-%d %H:%M:%S", localtime())
	
	fo = open("/ubc/ece/home/ugrads/a/a2m7/etc/www/results.txt","wb")
    	fo.write(json.dumps(results,cls=MyEncoder))
    	fo.close()


main()
