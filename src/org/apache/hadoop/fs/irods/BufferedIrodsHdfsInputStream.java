package org.apache.hadoop.fs.irods;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.irods.util.IrodsHdfsConfigUtil;

public class BufferedIrodsHdfsInputStream extends FSInputStream {
    
    private static final Log LOG = LogFactory.getLog(BufferedIrodsHdfsInputStream.class);
    
    public static final int DEFAULT_BUFFER_SIZE = 100 * 1024;
    
    private byte[] buffer;
    private int buffer_end = 0;
    private int buffer_pos = 0;
    private long buffer_start_pos = 0;
    private int buffer_size = 0;
    private IrodsHdfsInputStream is;
    
    public BufferedIrodsHdfsInputStream(IrodsHdfsInputStream is) throws IOException {
        init(is, DEFAULT_BUFFER_SIZE);
    }
    
    public BufferedIrodsHdfsInputStream(IrodsHdfsInputStream is, Configuration conf) throws IOException {
        int size = IrodsHdfsConfigUtil.getIrodsInputBufferSize(conf);
        init(is, size);
    }
    
    public BufferedIrodsHdfsInputStream(IrodsHdfsInputStream is, int buffer_size) throws IOException {
        init(is, buffer_size);
    }
    
    private void init(IrodsHdfsInputStream is, int buffer_size) throws IOException {
        this.is = is;
        this.buffer_size = buffer_size;
        this.buffer = new byte[this.buffer_size];
        this.buffer_start_pos = is.getPos();
        this.buffer_pos = 0;
        this.buffer_end = 0;
    }
    
    private int fillBuffer(long startOffset) throws IOException {
        this.buffer_start_pos = startOffset;
        this.buffer_pos = 0;
        this.buffer_end = this.is.read(startOffset, this.buffer, 0, this.buffer_size);
        return this.buffer_end;
    }
    
    @Override
    public synchronized long getPos() throws IOException {
        return this.buffer_start_pos + this.buffer_pos;
    }
    
    @Override
    public synchronized int available() throws IOException {
        return (this.buffer_end - this.buffer_pos) + this.is.available();
    }
    
    @Override
    public synchronized void seek(long targetPos) throws IOException {
        this.is.seek(targetPos);
        
        this.buffer_start_pos = this.is.getPos();
        this.buffer_pos = 0;
        this.buffer_end = 0;
    }
    
    @Override
    public synchronized boolean seekToNewSource(long targetPos) throws IOException {
        return this.is.seekToNewSource(targetPos);
    }
    
    @Override
    public synchronized int read() throws IOException {
        if(this.buffer_end - this.buffer_pos > 0) {
            int value = (this.buffer[this.buffer_pos] & 0xff);
            this.buffer_pos++;
            return value;
        } else {
            if(fillBuffer(this.buffer_start_pos + this.buffer_end) <= 0) {
                // eof
                return -1;
            }
            
            int value = (this.buffer[this.buffer_pos] & 0xff);
            this.buffer_pos++;
            return value;
        }
    }
    
    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        if(len < (this.buffer_end - this.buffer_pos)) {
            System.arraycopy(this.buffer, this.buffer_pos, bytes, off, len);
            this.buffer_pos += len;
            return len;
        }
        
        if(this.buffer_end - this.buffer_pos > 0) {
            // has buffer
            int cur_buff_size = buffer_end - this.buffer_pos;
            System.arraycopy(this.buffer, this.buffer_pos, bytes, off, cur_buff_size);
            this.buffer_pos += cur_buff_size;
            
            int result = this.is.read(bytes, off + cur_buff_size, len - cur_buff_size);
            this.buffer_start_pos += result;
            this.buffer_pos = 0;
            this.buffer_end = 0;
            return result;
        } else {
            // no buffer
            int result = this.is.read(bytes, off, len);
            this.buffer_start_pos += result;
            this.buffer_pos = 0;
            this.buffer_end = 0;
            return result;
        }
    }
    
    @Override
    public void close() throws IOException {
        this.buffer_end = 0;
        this.buffer_pos = 0;
        this.buffer = null;
        this.is.close();
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
