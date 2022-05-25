package alluxio.underfs.cubefs;


import com.sun.jna.Native;
import com.sun.jna.Pointer;
import org.apache.hadoop.util.DirectBufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;

@NotThreadSafe
public class CubeFsInputStream extends InputStream {
    private int fd;
    private ByteBuffer bf;
    private Pointer pbuf;
    private CubeFsMount cfs;
    private long fileOffset;
    private long bufferSize;
    private long bufferOffset;
    private boolean closed;
    private String path;
    private DirectBufferPool bufferPool;
    private static final Logger LOG = LoggerFactory.getLogger(CubeFsInputStream.class);

    public CubeFsInputStream(CubeFsMount cfs, DirectBufferPool bufferPool, String path, int fd, int bufferSize
    ) {
        this.cfs = cfs;
        this.fd = fd;
        this.bufferPool = bufferPool;
        this.bf = this.bufferPool.getBuffer(bufferSize);
        this.pbuf = Native.getDirectBufferPointer(bf);
        this.bf.limit(0);
        this.path = path;

    }

    private int checkAndReadFromCfs() throws IOException {
        if (bufferOffset == bufferSize) {
            ((Buffer) bf).clear(); // 兼容java 1.8，clear需要转一下类型
            long size = cfs.read(fd, pbuf, (long) bf.capacity(), fileOffset, path);
            if (size == 0) {
                return -1;
            }
            bufferSize = size;
            bufferOffset = 0;
        }
        return 0;
    }

    @Override
    public synchronized int read() throws IOException {
        checkNotClosed();
        if (checkAndReadFromCfs() == -1) {
            return -1;
        }
        fileOffset += 1;
        bufferOffset += 1;
        return (bf.get() + 256) % 256;
    }


    public synchronized void seek(long pos) throws IOException {
        if (fileOffset == pos) {
            return;
        }
        checkNotClosed();
        fileOffset = pos;
        ((Buffer) bf).clear(); // 兼容java 1.8，clear需要转一下类型
        bufferOffset = 0;
        bufferSize = 0;
    }

    public synchronized long getPos() throws IOException {
        return fileOffset;
    }


    @Override
    public synchronized int read(byte[] buffer, int offset, int length) throws IOException {
        LOG.debug("read: fd=" + this.fd + " path=" + this.path + " offset=" + offset + " length=" + length);
        checkNotClosed();
        if (checkAndReadFromCfs() == -1) {
            return -1;
        }
        int size = (int) (bufferSize - bufferOffset);
        if (size > length) {
            size = length;
        }
        int left = buffer.length - offset;
        if (size > left) {
            size = left;
        }
        bf.get(buffer, offset, size);
        fileOffset += size;
        bufferOffset += size;
        return size;
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    private void checkNotClosed() throws IOException {
        if (closed) {
            throw new IOException("close error");
        }
    }

    @Override
    public synchronized void close() throws IOException {
        LOG.debug("close,fd = " + this.fd + " path=" + this.path);
        if (closed) {
            return;
        }
        super.close();
        try {
            cfs.close(fd);
            closed = true;
        } catch (Exception e) {
            throw new IOException("close failed, fd = " + fd);
        } finally {
            this.pbuf = null;
            bufferPool.returnBuffer(this.bf);
            this.bf = null;
        }

    }
}
