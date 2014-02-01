#! /usr/bin/env python

import pickle
import os
import sys
import subprocess
import shutil
from ssh import SSHConnection

# global variables
g_temp_dir = "temp"
g_config_name = "testHadoopAtmo.cfg"
g_hadoopDownloadPath = "http://archive.cloudera.com/cdh/3/hadoop-0.20.2-cdh3u5.tar.gz"
g_hadoopFileName = "hadoop-0.20.2-cdh3u5.tar.gz"
g_hadoopConfigTempDir = "hadoop"
g_hadoopConfigTemplateDir = "hadoop_template"

g_hosts = []
g_userid = ""
g_userpwd = ""

# Configuration File I/O
def readConfigFile():
    configlist = {}
    try:
        configFile = open(g_config_name, "rb")
        configlist = pickle.load(configFile)
        configFile.close()
    except:
        print "readConfigError :", g_config_name

    return configlist

def writeConfigFile(configList):
    try:
        print configList
        configFile = open(g_config_name, "wb")
        pickle.dump(configList, configFile)
        configFile.close()
    except:
        print "writeConfigError :", g_config_name

# Configuration Highlevel APIs
def loadConfig():
    configList = readConfigFile()
    try:
        global g_hosts
        g_hosts = configList["hosts"]

        global g_userid
        global g_userpwd
        g_userid = configList["userid"]
        g_userpwd = configList["userpassword"]
    except KeyError:
        print "Config load error"

def saveConfig():
    configList = {}
    configList["hosts"] = g_hosts
    configList["userid"] = g_userid
    configList["userpassword"] = g_userpwd
    writeConfigFile(configList)

def viewConfig():
    print "============================="
    print " Config "
    print "============================="
    print "hosts :", g_hosts
    print "user id :", g_userid
    print "user password :", g_userpwd

def config():
    print "============================="
    print " Config "
    print "============================="
    
    hostList = []
    
    host = raw_input("host (Master) : ")
    if host != None and len(host) != 0:
        hostidx = 0
        hostList.append(host)
        
        while host != None and len(host) != 0:
            hostidx += 1
            host = raw_input("host (Slave " + str(hostidx) + ") : ")
            if host != None and len(host) != 0:
                hostList.append(host)

        global g_hosts
        g_hosts = hostList
    else:
        print "Host setup was cancelled"

    userid = raw_input("user id : ")
    userpwd = raw_input("user password : ")

    global g_userid
    global g_userpwd
    g_userid = userid
    g_userpwd = userpwd

    saveConfig()

# SSH Highlevel APIs
def openSSHConn(host, id, passwd):
    sshConn = SSHConnection(host, id, password = passwd)
    return sshConn

def closeSSHConn(sshConn):
    sshConn.close()

def printSSHmsg(msgs):
    for msg in msgs:
        print msg,

#    #s.put('/home/warrior/hello.txt', '/home/zombie/textfiles/report.txt')
#    #s.get('/var/log/strange.log', '/home/warrior/serverlog.txt')
#    s.execute("ls -l")
#    s.close()

def setupTmp(host, sconn):
    msg = sconn.execute("ls -al")
    printSSHmsg(msg)

def setHostname(host, sconn, password):
    msg = sconn.execute("hostname -f")
    print "current hostname : "
    printSSHmsg(msg)
    nameset = False
    for m in msg:
        if m.strip() == host:
            nameset = True

    if nameset == False:
        msg = sconn.execute("echo " + password + " | sudo -S hostname " + host)
        printSSHmsg(msg)

def copyRemoteFile(sconn, path, destPath, overwrite=False):
    print "copying " + path
    print "--> to : " + destPath

    if overwrite == False:
        hasfile = True
        msg = sconn.execute("ls " + destPath)
        for m in msg:
            if "No such file" in m:
                hasfile = False

        if hasfile == False:
            sconn.put(path, destPath)
    else:
        sconn.put(path, destPath)

def prepareHadoopConfigFiles():
    print "preparing hadoop configurations"
    host_master = g_hosts[0]
    host_slaves = ""
    for i in range(1, len(g_hosts)):
        host_slaves += g_hosts[i] + "\\n"

    #hadoop_temp_dir = "\\/home\\/" + g_userid + "\\/hadoopfs2\\/tmp"
    hadoop_temp_dir = "\\/home\\/" + g_userid + "\\/hadoopfs\\/tmp"
    hadoop_name_dir = "\\/home\\/" + g_userid + "\\/hadoopfs\\/name"
    #hadoop_data_dir = "\\/home\\/" + g_userid + "\\/hadoopfs\\/data, " + "\\/home\\/" + g_userid + "\\/hadoopfs2\\/data"
    hadoop_data_dir = "\\/home\\/" + g_userid + "\\/hadoopfs\\/data"

    abspath = os.path.realpath(g_hadoopConfigTempDir)
    if os.path.exists(abspath):
        shutil.rmtree(abspath)

    abstemplatepath = os.path.realpath(g_hadoopConfigTemplateDir)
    shutil.copytree(abstemplatepath, abspath)
    
    for root, dirs, files in os.walk(g_hadoopConfigTempDir + "/conf", topdown=True):
        for name in files:
            print(os.path.join(root, name))
            entry = os.path.join(root, name)
            abspath = os.path.realpath(entry)

            subprocess.call("sed -i 's/$MASTER_HOSTNAME/" + host_master + "/g' " + abspath, shell=True)
            subprocess.call("sed -i 's/$SLAVE_HOSTNAMES/" + host_slaves + "/g' " + abspath, shell=True)
            subprocess.call("sed -i 's/$HADOOP_TEMP_DIR/" + hadoop_temp_dir + "/g' " + abspath, shell=True)
            subprocess.call("sed -i 's/$HADOOP_NAME_DIR/" + hadoop_name_dir + "/g' " + abspath, shell=True)
            subprocess.call("sed -i 's/$HADOOP_DATA_DIR/" + hadoop_data_dir + "/g' " + abspath, shell=True)

def setupHadoop(host, sconn, userid, userpwd):
    # check hostname
    setHostname(host, sconn, userpwd)

    # change path to home
    sconn.execute("cd ~")    

    """
    # copy hadoop package
    realHadoopPath = os.path.realpath(g_temp_dir + "/" + g_hadoopFileName)
    copyRemoteFile(sconn, realHadoopPath, "/home/" + userid + "/" + g_hadoopFileName)
    """

    # uncompress package
    print "uncompressing " + g_hadoopFileName
    sconn.execute("tar zxvf " + g_hadoopFileName)

    # make symbolic link
    hadoopPath = os.path.splitext(os.path.splitext(g_hadoopFileName)[0])[0]
    sconn.execute("ln -sf " + hadoopPath + " hadoop")
    
    # make hadoopfs dir
    sconn.execute("mkdir hadoopfs")

    # copy hadoop configs
    print "copying hadoop configurations"
    for root, dirs, files in os.walk(g_hadoopConfigTempDir, topdown=True):
        for name in files:
            print "copying " + os.path.join(root, name)
            entry = os.path.join(root, name)
            copyRemoteFile(sconn, os.path.realpath(entry), "/home/" + userid + "/" + entry, True)
    
    # listing
    msg = sconn.execute("ls -al")
    printSSHmsg(msg)

def setup():
    # prepare temp directory
    realTempPath = os.path.realpath(g_temp_dir)
    if not os.path.exists(realTempPath):
        os.makedirs(realTempPath)

    # prepare hadoop package
    subprocess.call("wget -P " + realTempPath +" -c " + g_hadoopDownloadPath, shell=True)

    # prepare hadoop configuration file
    prepareHadoopConfigFiles()
    
    for host in g_hosts:
        print "============================="
        print host
        print "============================="
        sconn = openSSHConn(host, g_userid, g_userpwd)
        setupHadoop(host, sconn, g_userid, g_userpwd)
        closeSSHConn(sconn)

def main():
    if len(sys.argv) < 2:
        print "command : ./testHadoopAtmo.py config"
        print "command : ./testHadoopAtmo.py cfgview"
    else:
        command = sys.argv[1]

        loadConfig()

        if command == "config":
            config()
        elif command == "cfgview":
            viewConfig()
        elif command == "setup":
            setup()
        else:
            print "invalid command"

if __name__ == "__main__":
    main()