package edu.arizona.cs.hadoop.fs.irods.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class HirodsConfigUtils {
    
    public static final Log LOG = LogFactory.getLog(HirodsConfigUtils.class);
    
    public static final String CONFIG_IRODS_HOST = "fs.irods.host";
    public static final String CONFIG_IRODS_PORT = "fs.irods.port";
    public static final String CONFIG_IRODS_ZONE = "fs.irods.zone";
    public static final String CONFIG_IRODS_USERNAME = "fs.irods.account.username";
    public static final String CONFIG_IRODS_PASSWORD = "fs.irods.account.password";
    public static final String CONFIG_IRODS_INPUT_BUFFER_SIZE = "fs.irods.input.buffer.size";
    public static final String CONFIG_IRODS_OUTPUT_BUFFER_SIZE = "fs.irods.output.buffer.size";
    public static final String CONFIG_IRODS_OUTPUT_BUFFERED_PATH = "fs.irods.output.hdfs_filebuffer.dir";
    
    public static final int DEFAULT_IRODS_PORT = 1247;
    public static final int DEFAULT_BUFFER_SIZE = 1024 * 100;
    public static final String DEFAULT_OUTPUT_BUFFERED_PATH = "hirods_temp/";
    
    public static String getIrodsHost(Configuration conf) {
        return conf.get(CONFIG_IRODS_HOST, null);
    }
    
    public static void setIrodsHost(Configuration conf, String host) {
        conf.set(CONFIG_IRODS_HOST, host);
    }
    
    public static int getIrodsPort(Configuration conf) {
        return conf.getInt(CONFIG_IRODS_PORT, DEFAULT_IRODS_PORT);
    }
    
    public static void setIrodsPort(Configuration conf, int port) {
        conf.setInt(CONFIG_IRODS_PORT, port);
    }
    
    public static String getIrodsZone(Configuration conf) {
        return conf.get(CONFIG_IRODS_ZONE, null);
    }
    
    public static void setIrodsZone(Configuration conf, String zone) {
        conf.set(CONFIG_IRODS_ZONE, zone);
    }
    
    public static String getIrodsUsername(Configuration conf) {
        return conf.get(CONFIG_IRODS_USERNAME, null);
    }
    
    public static void setIrodsUsername(Configuration conf, String username) {
        conf.set(CONFIG_IRODS_USERNAME, username);
    }
    
    public static String getIrodsPassword(Configuration conf) {
        return conf.get(CONFIG_IRODS_PASSWORD, null);
    }
    
    public static void setIrodsPassword(Configuration conf, String password) {
        conf.set(CONFIG_IRODS_PASSWORD, password);
    }
    
    public static int getIrodsInputBufferSize(Configuration conf) {
        return conf.getInt(CONFIG_IRODS_INPUT_BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
    }
    
    public static void setIrodsInputBufferSize(Configuration conf, int buffer_size) {
        conf.setInt(CONFIG_IRODS_INPUT_BUFFER_SIZE, buffer_size);
    }
    
    public static int getIrodsOutputBufferSize(Configuration conf) {
        return conf.getInt(CONFIG_IRODS_OUTPUT_BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
    }
    
    public static void setIrodsOutputBufferSize(Configuration conf, int buffer_size) {
        conf.setInt(CONFIG_IRODS_OUTPUT_BUFFER_SIZE, buffer_size);
    }
    
    public static String getIrodsOutputBufferedPath(Configuration conf) {
        return conf.get(CONFIG_IRODS_OUTPUT_BUFFERED_PATH, DEFAULT_OUTPUT_BUFFERED_PATH);
    }
    
    public static void setIrodsOutputBufferedPath(Configuration conf, String path) {
        conf.set(CONFIG_IRODS_OUTPUT_BUFFERED_PATH, path);
    }
}
