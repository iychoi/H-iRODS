package org.apache.hadoop.fs.irods;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.pub.IRODSFileSystem;
import org.irods.jargon.core.pub.io.FileIOOperations.SeekWhenceType;
import org.irods.jargon.core.pub.io.IRODSFile;
import org.irods.jargon.core.pub.io.IRODSFileFactory;
import org.irods.jargon.core.pub.io.IRODSRandomAccessFile;

public class IrodsHdfsInputStream extends FSInputStream {

    private static final Log LOG = LogFactory.getLog(IrodsHdfsInputStream.class);
    
    private IRODSFile path;
    private IRODSFileSystem irodsFS;
    private IRODSFileFactory fileFactory;
    private FileSystem.Statistics stats;
    private boolean closed;
    private long fileLength;
    private long pos = 0;
    private IRODSRandomAccessFile raf;
    
    public IrodsHdfsInputStream(Configuration conf, IRODSFile path, IRODSFileSystem irodsFS, IRODSFileFactory fileFactory, FileSystem.Statistics stats) throws IOException {
        this.path = path;
        this.irodsFS = irodsFS;
        this.fileFactory = fileFactory;
        this.stats = stats;
        this.fileLength = path.length();
        this.pos = 0;
        
        //LOG.info("FileLength : " + fileLength);

        try {
            this.raf = this.fileFactory.instanceIRODSRandomAccessFile(path.getAbsolutePath());
        } catch (JargonException ex) {
            throw new IOException(ex);
        }
    }
    
    public synchronized long getSize() throws IOException {
        return this.fileLength;
    }
    
    @Override
    public synchronized long getPos() throws IOException {
        return this.pos;
    }

    @Override
    public synchronized int available() throws IOException {
        return (int) (this.fileLength - this.pos);
    }
    
    @Override
    public synchronized void seek(long targetPos) throws IOException {
        if (targetPos > this.fileLength) {
            throw new IOException("Cannot seek after EOF");
        }
        this.pos = targetPos;
        this.raf.seek(targetPos, SeekWhenceType.SEEK_START);
    }
    
    @Override
    public long skip(long l) throws IOException {
        if(l <= 0) {
            return 0;
        }
        
        long newOff = Math.min(l, this.fileLength - this.pos);
        seek(this.pos + newOff);
        return newOff;
    }

    @Override
    public synchronized boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }
    
    @Override
    public synchronized int read() throws IOException {
        if (this.closed) {
            throw new IOException("Stream closed");
        }
        //LOG.info("pos : " + this.pos + " / " + this.fileLength);
        
        int result = -1;
        if (this.pos < this.fileLength) {
            byte[] bytes = new byte[1];
            result = this.raf.read(bytes, 0, 1);
            //LOG.info("read results : " + result);
            if (result > 0) {
                this.pos += result;
                result = ((int)bytes[0] & 0xff);
            }
            
            //result = this.raf.read();
            //if (result >= 0) {
            //    this.pos++;
            //}
        }
        if (this.stats != null & result >= 0) {
            this.stats.incrementBytesRead(1);
        }
        
        //LOG.info("read : " + result);
        return result;
    }
    
    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        if (this.closed) {
            throw new IOException("Stream closed");
        }
        if (this.pos < this.fileLength) {
            int readLen = (int)Math.min(this.fileLength - this.pos, len);
            
            int result = this.raf.read(bytes, off, readLen);
            if (result >= 0) {
                this.pos += result;
            }
            if (this.stats != null && result > 0) {
                this.stats.incrementBytesRead(result);
            }
            return result;
        }
        
        return -1;
    }
    
    @Override
    public void close() throws IOException {
        if (this.closed) {
            return;
        }
        if (this.raf != null) {
            this.raf.close();
            this.raf = null;
        }
        super.close();
        this.closed = true;
    }
    
    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(int readLimit) {
        // Do nothing
    }

    @Override
    public void reset() throws IOException {
        throw new IOException("Mark not supported");
    }
}
