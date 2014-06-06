#!/apps/admin/tools.x86_64/python/2.7.3/bin/python
import paramiko
import string
from threading import Thread
import threading
import os
import sys
import argparse
import sets
import smtplib
from email.mime.text import MIMEText
hostlist = ['192.168.200.10', '192.168.200.20', '192.168.200.30', '192.168.200.40', '192.168.200.50']
parser = argparse.ArgumentParser(description='Check some blade hosts')
parser.add_argument('hosts', metavar='HOST', nargs='*', help='Host(s) to check load and files on', default=hostlist)
args = parser.parse_args()
print args.hosts
hostlist = ['192.168.200.10', '192.168.200.20', '192.168.200.30', '192.168.200.40', '192.168.200.50']
def mail_function(message):
   me = "foo@you.com"
   you = "you@foo.com"
   message = "Top 5 files in use:\n" + message
   msg = MIMEText(message)
   msg['Subject'] = "Panfs file usage"
   msg['From'] = me
   msg['To'] = you
   s = smtplib.SMTP('localhost')
   s.sendmail(me, you, msg.as_string())
   s.quit()

class ThreadWithReturnValue(Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs, Verbose)
        self._return = None
    def run(self):
        if self._Thread__target is not None:
            self._return = self._Thread__target(*self._Thread__args,
                                                **self._Thread__kwargs)
    def join(self):
        Thread.join(self)
        return self._return

def high_load(hostname):

   ssh = paramiko.SSHClient()
   ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
   try:
       ssh.connect (hostname,username='admin')
   except:
       print "Could not connect to " + hostname
       exit(0)
   commandlet = "uptime"
   stdin, stdout, stderr = ssh.exec_command(commandlet)
   data = stdout.readline()
   dataerr = []
   dataerr.append(stderr.readline())

   if len(dataerr) > 1:
      print dataerr

   with outlock:
       empty_host = 0
       tested_load = load_split(data,hostname)
       try:
          test_load_split = tested_load.split(" ")
       except:
	  empty_host = 1
       if ( empty_host == 0 ):

         for line in test_load_split:
           commandlet = "sysstat hotfiles " + str(hostname)
           stdin, stdout, stderr = ssh.exec_command(commandlet)
           data = stdout.readlines()
           data_line = ""

           for line in data[1:6]:

              data_line = data_line + line
           message_line = hostname + ":\n"
           for line in data_line.split(" "):
	      find_line = str(line).find("I-x")
              if str(line).isdigit():
                 save_line = line
              if find_line == 0:
                convert_id=object_id_resolution(hostname,line)
                message_line = message_line + save_line + " " + convert_id
           dataerr = []
           dataerr.append(stderr.readline())
           if len(dataerr) > 1:
             print dataerr
	   return message_line
   ssh.close()

def object_id_resolution(hostname,id):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect (hostname='panfs-10g',username='admin')
    except:
        print "Could not connect to " + hostname
        exit(0)
    commandlet = "support servicemode yes; objutil paths " + str(id)
    stdin, stdout, stderr = ssh.exec_command(commandlet)
    data = stdout.readlines()
    data_line = ""
    for line in data[1]:
      data_line = data_line + line
    dataerr = []
    dataerr.append(stderr.readline())
    if len(dataerr) > 1:
       print dataerr
    ssh.close()
    return data_line
def load_split(load,host):
    list_load = load.replace(",","")
    list_load = str(list_load).replace("\n","")
    list_load = str(list_load).split(" ")
    if ( len(list_load) == 16 ):
      try:
         float(list_load[13]) == 0
      except:
         list_load[13] = 0.1
      try:
         float(list_load[14]) == 0
      except:
         list_load[14] = 0.1
      try:
         float(list_load[15]) == 0
      except:
         list_load[15] = 0.1
      list_load.append(list_load[15])
      list_load[15] = list_load[14]
      list_load[14] = list_load[13]
    try:
       float(list_load[14]) == 0
    except:
       list_load[14] = 0.1
    try:
       float(list_load[15]) == 0
    except:
       list_load[15] = 0.1
    try:
       float(list_load[16]) == 0
    except:
       list_load[16] = 0.1
    if ( float(list_load[14]) > 1 or float(list_load[15]) > 1 or float(list_load[16]) > 1 ):
       return hostname
    else:
       return
outlock = threading.Lock()
threads = [];
for hostname in hostlist:
    t = ThreadWithReturnValue(target=high_load, args=(hostname,))
    t.start()
    threads.append(t)
line = " "
for t in threads:
   if ( str(t.join()) == "None" ):
      line = ""
   else:
      line = str(line) + str(t.join())
if ( len(line) > 0 ):
   mail_function(line)
