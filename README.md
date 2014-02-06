iRODS-HDFS
==========

iRODS-HDFS provides a Hadoop Distributed File System (HDFS) driver for iRODS.

Setup on Hadoop
---------------

Place iRODS-HDFS package and dependent libraries to classpath directory. So that Hadoop system can find these libraries when they are called.

Register iRODS-HDFS to Hadoop configuration. We need to modify "core-site.xml" file which is Hadoop configuration file.

```
	<property>
		<name>fs.irods.impl</name>
		<value>org.apache.hadoop.fs.irods.IrodsHdfsFileSystem</value>
		<description>The FileSystem for irods:// uris.</description>
	</property>
	<property>
		<name>fs.irods.host</name>
		<value>data.iplantcollaborative.org</value>
	</property>
	<property>
		<name>fs.irods.port</name>
		<value>1247</value>
	</property>
	<property>
		<name>fs.irods.zone</name>
		<value>iplant</value>
	</property>
	<property>
		<name>fs.irods.account.username</name>
		<value>USERNAME_HERE</value>
	</property>
	<property>
		<name>fs.irods.account.password</name>
		<value>PASSWORD_HERE</value>
	</property>
	<property>
		<name>fs.irods.account.homedir</name>
		<value>WORKING_DIRECTORY_HERE</value>
	</property>
```

Restart the hadoop system then you could use "irods://path/to/your/resources" like path.

Dependencies
------------

iRODS-HDFS uses [Jargon](https://www.irods.org/index.php/Jargon) library to connect to iRODS system.
Following libraries are using now in iRODS-HDFS.
- jargon-core-3.3.2-20140124.145551-44.jar
- jargon-transfer-engine-3.3.2-20140124.145551-43.jar
- jargon-transfer-dao-spring-3.3.2-20140124.145551-43.jar


Building
--------

Building from the source code is very simple. Source code is written in Java and provides "NetBeans" project file and "Ant" scripts for building. If you are using NetBeans IDE, load the project and build through the IDE. Or, simple type "ant".

```
$ ant
```

All dependencies for this project are already in /libs/ directory.


