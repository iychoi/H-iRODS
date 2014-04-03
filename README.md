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

Restart the hadoop system then you could use "irods://irods_host/path/to/your/resources" like path.

Dependencies
------------

H-iRODS uses [Jargon](https://www.irods.org/index.php/Jargon) library to connect to iRODS system.
Following library is used currently.
- jargon-core-3.3.2-20140124.145551-44.jar


Building
--------

Building from the source code is very simple. Source code is written in Java and provides "NetBeans" project file and "Ant" scripts for building. If you are using NetBeans IDE, load the project and build through the IDE. Or, simple type "ant".

```
$ ant
```

All dependencies for this project are already in /libs/ directory.


