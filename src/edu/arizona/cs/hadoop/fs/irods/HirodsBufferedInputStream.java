package edu.arizona.cs.hadoop.fs.irods;

import edu.arizona.cs.hadoop.fs.irods.util.HirodsConfigUtils;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSInputStream;

public class HirodsBufferedInputStream extends FSInputStream {
    
    private static final Log LOG = LogFactory.getLog(HirodsBufferedInputStream.class);
    
    private byte[] buffer;
    private int buffer_end = 0;
    private int buffer_pos = 0;
    private long buffer_start_pos = 0;
    private int buffer_size = 0;
    private HirodsInputStream is;
    
    public HirodsBufferedInputStream(HirodsInputStream is) throws IOException {
        init(is, HirodsConfigUtils.DEFAULT_BUFFER_SIZE);
    }
    
    public HirodsBufferedInputStream(HirodsInputStream is, int buffer_size) throws IOException {
        init(is, buffer_size);
    }
    
    private void init(HirodsInputStream is, int buffer_size) throws IOException {
        this.is = is;
        this.buffer_size = buffer_size;
        this.buffer = new byte[this.buffer_size];
        this.buffer_start_pos = is.getPos();
        this.buffer_pos = 0;
        this.buffer_end = 0;
    }
    
    private int fillBuffer(long startOffset) throws IOException {
        this.is.seek(startOffset);
        this.buffer_start_pos = startOffset;
        this.buffer_pos = 0;
        this.buffer_end = 0;

        int retval = this.is.read(this.buffer, 0, this.buffer_size);
        if(retval >= 0) {
            this.buffer_end = retval;
        }
        
        return retval;
    }
    
    @Override
    public long skip(long l) throws IOException {
        if(l <= 0) {
            return 0;
        }
        
        if(this.buffer_pos + l <= this.buffer_end) {
            this.buffer_pos += l;
            return l;
        }
    
        long newlen = Math.min(l, this.is.getSize() - this.buffer_start_pos - this.buffer_pos);
        
        seek(this.buffer_start_pos + this.buffer_pos + newlen);
        return l;
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
        if(targetPos >= this.buffer_start_pos && targetPos < this.buffer_start_pos + this.buffer_end) {
            this.buffer_pos = (int) (targetPos - this.buffer_start_pos);
        } else {
            this.is.seek(targetPos);

            this.buffer_start_pos = targetPos;
            this.buffer_pos = 0;
            this.buffer_end = 0;
        }
    }
    
    @Override
    public synchronized boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }
    
    @Override
    public synchronized int read() throws IOException {
        if(this.buffer_end - this.buffer_pos > 0) {
            int value = ((int)this.buffer[this.buffer_pos] & 0xff);
            this.buffer_pos++;
            return value;
        } else {
            int ret = fillBuffer(this.buffer_start_pos + this.buffer_end);
            if(ret <= 0) {
                // eof
                return ret;
            }
            
            int value = ((int) this.buffer[this.buffer_pos] & 0xff);
            this.buffer_pos++;
            return value;
        }
    }
    
    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        if(len <= (this.buffer_end - this.buffer_pos)) {
            System.arraycopy(this.buffer, this.buffer_pos, bytes, off, len);
            this.buffer_pos += len;
            return len;
        } else {
            if (this.buffer_end - this.buffer_pos > 0) {
                // has buffer
                int cur_buff_size = this.buffer_end - this.buffer_pos;
                System.arraycopy(this.buffer, this.buffer_pos, bytes, off, cur_buff_size);
                this.buffer_pos += cur_buff_size;
                // toss to next iteration
                return cur_buff_size;
            } else {
                // no buffer
                int ret = fillBuffer(this.buffer_start_pos + this.buffer_end);
                if (ret <= 0) {
                    // eof
                    return ret;
                }
                
                int cur_buff_size = this.buffer_end - this.buffer_pos;
                int min_read = Math.min(cur_buff_size, len);
                
                System.arraycopy(this.buffer, this.buffer_pos, bytes, off, min_read);
                this.buffer_pos += min_read;
                // toss to next iteration
                return min_read;
            }
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

