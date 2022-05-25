package alluxio.underfs.cubefs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;

@NotThreadSafe
public class CubeFsOutputStream extends OutputStream {
    private boolean closed;
    private int fileHandle;
    private CubeFsMount cfs;
    private ByteBuffer bf;
    private long offset = 0;
    private static final Logger LOG = LoggerFactory.getLogger(CubeFsOutputStream.class);

    public CubeFsOutputStream() {
        super();
    }

    public CubeFsOutputStream(CubeFsMount cfs, int fd, long offset, int bufferSize) {
        this.cfs = cfs;
        this.fileHandle = fd;
        this.bf = ByteBuffer.allocate(bufferSize);
        this.offset = offset;
    }

    @Override
    public void flush() throws IOException {
        LOG.debug("flush,fd = " + fileHandle);
        cfsWrite();
        int result = cfs.flush(fileHandle);
        if (result != 0) {
            throw new IOException("flush failed: " + result);
        }
    }

    private void cfsWrite() throws IOException {
        if (bf.position() == 0) {
            return;
        }

        long size = cfs.write(fileHandle, bf.array(), bf.position(), offset);
        if (size != bf.position()) {
            throw new IOException("write failed:" + size);
        }
        offset += bf.position();
        // 兼容java 1.8，clear需要转一下类型
        ((Buffer) bf).clear();
    }

    @Override
    public void write(int b) throws IOException {
        bf.put((byte) b);
        if (bf.position() == bf.capacity()) {
            cfsWrite();
        }
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        LOG.debug("write: fd=" + this.fileHandle + " offset=" + off + " length=" + len);
        while (len > 0) {
            int wsize = bf.capacity() - bf.position();
            if (len < wsize) {
                wsize = len;
            }
            bf.put(b, off, wsize);
            off += wsize;
            len -= wsize;
            if (bf.position() == bf.capacity()) {
                cfsWrite();
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        LOG.debug("close,fd = " + fileHandle);

        if (closed) {
            return;
        }
        try {
            flush();
        } finally {
            try {
                this.bf = null;
                closed = true;
                cfs.close(fileHandle);
            } catch (Exception e) {
                throw new IOException("close failed, fd = " + fileHandle);
            }
        }


    }
}
