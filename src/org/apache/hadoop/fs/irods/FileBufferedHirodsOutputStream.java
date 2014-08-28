package org.apache.hadoop.fs.irods;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.pub.io.IRODSFile;
import org.irods.jargon.core.pub.io.IRODSFileFactory;

/**
 *
 * @author iychoi
 */
public class FileBufferedHirodsOutputStream extends OutputStream {
    
    private static final Log LOG = LogFactory.getLog(FileBufferedHirodsOutputStream.class);
    
    private static final short TEMP_FILE_REPLICATION = 1;
    
    private IRODSFileFactory irods_factory;
    private IRODSFile irods_path;
    private Path temp_path;
    private int buffer_size;
    private FSDataOutputStream temp_os;
    private Configuration conf;
    
    public FileBufferedHirodsOutputStream(IRODSFileFactory irods_factory, IRODSFile irods_path, Path temp_path, Configuration conf, int buffer_size) throws IOException {
        this.irods_factory = irods_factory;
        this.irods_path = irods_path;
        this.temp_path = temp_path;
        this.buffer_size = buffer_size;
        this.conf = conf;
        
        FileSystem fs = temp_path.getFileSystem(conf);
        // prepare dirs
        if(!temp_path.getParent().getName().equals("/") && !temp_path.getParent().getName().isEmpty()) {
            fs.mkdirs(temp_path.getParent());
        }
        this.temp_os = fs.create(temp_path, TEMP_FILE_REPLICATION);
    }
    
    @Override
    public void write(int i) throws IOException {
        // write to temp_os
        this.temp_os.write(i);
    }
    
    @Override
    public void write(byte[] bytes) throws IOException {
        // write to temp_os
        this.temp_os.write(bytes);
    }
    
    @Override
    public void write(byte[] bytes, int i, int i1) throws IOException {
        // write to temp_os
        this.temp_os.write(bytes, i, i1);
    }

    @Override
    public void flush() throws IOException {
        // write to temp_os
        this.temp_os.flush();
    }

    @Override
    public void close() throws IOException {
        this.temp_os.close();
        BufferedOutputStream irods_os = null;
        FSDataInputStream hdfs_is = null;
        try {
            // commit to iRODS
            irods_os = new BufferedOutputStream(this.irods_factory.instanceIRODSFileOutputStream(this.irods_path), this.buffer_size);
            hdfs_is = this.temp_path.getFileSystem(this.conf).open(this.temp_path);
            
            byte[] buffer = new byte[this.buffer_size];
            int bytes_read = 0;
            
            while((bytes_read = hdfs_is.read(buffer)) != -1) {
                irods_os.write(buffer, 0, bytes_read);
            }
        } catch (JargonException ex) {
            throw new IOException(ex);
        } finally {
            if(hdfs_is != null) {
                try {
                    hdfs_is.close();
                } catch(IOException ex) {
                    // ignore exceptions
                }
            }
            
            // remove temporary file
            try {
                this.temp_path.getFileSystem(this.conf).delete(this.temp_path, true);
            } catch(IOException ex) {
                // ignore exceptions
            }
            
            if(irods_os != null) {
                irods_os.close();
            }
        }
    }
}
