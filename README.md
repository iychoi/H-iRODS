H-iRODS
==========

H-iRODS provides a Hadoop file system interface for iRODS.


Setup on Hadoop
---------------

Place H-iRODS package and dependent libraries to classpath directory (or use -libjars option). So that Hadoop system can find these libraries when they are called.

Register H-iRODS to Hadoop configuration. We need to modify "core-site.xml" file which is Hadoop configuration file.

```
	<property>
		<name>fs.irods.impl</name>
		<value>edu.arizona.cs.hadoop.fs.irods.HirodsFileSystem</value>
		<description>The FileSystem for irods: uris.</description>
	</property>
	<property>
		<name>fs.irods.host</name>
		<value>data.iplantcollaborative.org</value> <!-- host -->
	</property>
	<property>
		<name>fs.irods.port</name>
		<value>1247</value> <!-- port -->
	</property>
	<property>
		<name>fs.irods.zone</name>
		<value>iplant</value> <!-- zone -->
	</property>
	<property>
		<name>fs.irods.account.username</name>
		<value>USERNAME_HERE</value>
	</property>
	<property>
		<name>fs.irods.account.password</name>
		<value>PASSWORD_HERE</value>
	</property>
```

Restart the hadoop system then you could use "irods://host/zone/path-to-your-resources" like path.


Dependencies
------------

H-iRODS uses [Jargon](https://www.irods.org/index.php/Jargon) library to connect to iRODS system.
Following library is used currently.
- jargon-core-3.3.2-20140124.145551-44.jar


Hadoop Compatibility
--------------------

H-iRODS is tested at Cloudera CDH3u5.


Building
--------

Building from the source code is very simple. Source code is written in Java and provides "NetBeans" project file and "Ant" scripts for building. If you are using NetBeans IDE, load the project and build through the IDE. Or, simple type "ant".

```
$ ant
```

All dependencies for this project are already in /libs/ directory.


Note
----

H-iRODS provides an implementation of Hadoop's FileSystem interface. Thus, you can easily access iRODS contents via FileSystem Interface (edu.arizona.cs.hadoop.fs.irods.HirodsFileSystem). Hadoop's command line tools (hadoop dfs) work very well with.

However, this interface implementation does not work well with MapReduce. This is just because Reducer holds remote file descriptors for a long time (hours or days) and this causes timeout or lost descriptors error. Hence, "edu.arizona.cs.hadoop.fs.irods.output" package provides custom output formats that buffers reducer's output at HDFS and write to iRODS on close. Hence, if you want to H-iRODS library directly with MapReduce, you should change your program code to use proper output formats.

HDFS | H-iRODS
--- | --- 
FileOutputFormat | HirodsFileOutputFormat
TextOutputFormat | HirodsTextOutputformat
MapFileOutputFormat | HirodsMapFileOutputFormat
SequenceFileOutputFormat | HirodsSequenceFileOutputFormat
SequenceFileAsBinaryOutputFormat | HirodsSequenceFileAsBinaryOutputFormat
MultipleOutputs | HirodsMultipleOutputs
