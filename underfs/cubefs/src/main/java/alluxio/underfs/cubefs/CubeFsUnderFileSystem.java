package alluxio.underfs.cubefs;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.exception.InvalidPathException;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.*;
import alluxio.underfs.options.*;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.util.DirectBufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * CubeFs implement base on cubefs interface.
 */
public class CubeFsUnderFileSystem extends ConsistentUnderFileSystem
        implements AtomicFileOutputStreamCallback {
    private static final Logger LOG = LoggerFactory.getLogger(CubeFsUnderFileSystem.class);
    private static final DirectBufferPool bufferPool = new DirectBufferPool();
    private int bufferSize;
    private int readBufferSize;

    private static final int MAX_RETRY_TIMES = 10;
    private static final long BLOCK_SIZE = 128 << 20;
    private static final int BATCH_SIZE = 100;

    /**
     * cubefs client instance
     */
    private static CubeFsMount cfs;

    /**
     * Creates a new {@link CubeFsUnderFileSystem} for the given uri.
     *
     * @param uri  path belonging to this under file system
     * @param conf UFS configuration
     */
    public CubeFsUnderFileSystem(AlluxioURI uri, UnderFileSystemConfiguration conf) throws IOException {
        super(uri, conf);
        LOG.info("Cubefs URI: {}.", uri);
        if (cfs == null) {
            cfs = new CubeFsMount();
        }
        String volumeName = uri.getRootPath();
        LOG.info("Cubefs volumeName: {}.", volumeName);
        if (isEmpty(volumeName)) {
            throw new IOException("volume name is required.");
        }
        String masterAddress = conf.getString(PropertyKey.UNDERFS_CUBEFS_MASTESR_ADDRESS);
        if (isEmpty(masterAddress)) {
            throw new IOException("master address is required.");
        }
        String accessKey = conf.getString(PropertyKey.UNDERFS_CUBEFS_ACCESS_KEY);
        if (isEmpty(accessKey)) {
            throw new IOException("accessKey is required.");
        }
        String secretKey = conf.getString(PropertyKey.UNDERFS_CUBEFS_SECRET_KEY);
        if (isEmpty(secretKey)) {
            throw new IOException("secretKey  is required.");
        }
        String logDir = conf.getString(PropertyKey.UNDERFS_CUBEFS_LOGGER_DIR);
        String logLevel = conf.getString(PropertyKey.UNDERFS_CUBEFS_LOGGER_LEVEL);

        cfs.setClient("volName", volumeName);
        cfs.setClient("masterAddr", masterAddress);
        cfs.setClient("logDir", logDir);
        cfs.setClient("logLevel", logLevel);
        cfs.setClient("accessKey", accessKey);
        cfs.setClient("secretKey", secretKey);

        bufferSize = conf.getInt(PropertyKey.UNDERFS_CUBEFS_MIN_BUFFER_SIZE);
        readBufferSize = conf.getInt(PropertyKey.UNDERFS_CUBEFS_MIN_READ_BUFFER_SIZE);

        int ret = cfs.startClient();
        if (ret < 0) {
            throw new IOException(String.format("Cubefs initialize fail for cubefs://%s,code=%s", volumeName, ret));
        }
    }

    private boolean isEmpty(String key) {
        return key == null || key.isEmpty();
    }

    public static UnderFileSystem createInstance(AlluxioURI uri, UnderFileSystemConfiguration conf) throws IOException {
        return new CubeFsUnderFileSystem(uri, conf);
    }

    @Override
    public OutputStream createDirect(String path, CreateOptions options) throws IOException {

        int flags = CubeFsMount.O_WRONLY | CubeFsMount.O_CREAT;
        while (true) {
            int fd = cfs.open(formatPath(path), flags, options.getMode().toShort());
            if (fd == CubeFsMount.ENOENT) {
                String parent = null;
                try {
                    parent = PathUtils.getParent(path);
                } catch (InvalidPathException e) {
                    throw new IOException("Invalid path " + path, e);
                }
                try {
                    mkdirs(parent, MkdirsOptions.defaults(mUfsConf));
                } catch (Exception e) {
                }
                continue;
            }
            if (fd < 0) {
                throw CubeFsMount.error(fd, formatPath(path));
            }
            return new CubeFsOutputStream(cfs, fd, 0, checkBufferSize(bufferSize));
        }
    }

    private String formatPath(String path) {
        return new AlluxioURI(path).getPath();
    }

    @Override
    public void cleanup() throws IOException {

    }

    @Override
    public void connectFromMaster(String hostname) throws IOException {
        // No op
    }

    @Override
    public void connectFromWorker(String hostname) throws IOException {
        // No op
    }

    @Override
    public OutputStream create(String path, CreateOptions options) throws IOException {
        if (!options.isEnsureAtomic()) {
            return createDirect(path, options);
        }
        return new AtomicFileOutputStream(path, this, options);
    }

    @Override
    public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
        int r = cfs.unlink(formatPath(path));
        if (r == 0) {
            return true;
        }
        if (r == CubeFsMount.ENOENT) {
            return false;
        }
        if (r == CubeFsMount.EISDIR) {
            //get directory contents
            UfsStatus[] fileStatuses = listStatus(path);
            if (fileStatuses == null) {
                return false;
            }
            if (!options.isRecursive() && ArrayUtils.isNotEmpty(fileStatuses)) {
                throw new IOException("Directory: " + path + "  is not empty");
            }

            for (UfsStatus status : fileStatuses) {
                //todo name
                if (!deleteDirectory(status.getName(), options)) {
                    return false;
                }
            }
        } else {
            throw CubeFsMount.error(r, formatPath(path));
        }
        cfs.rmdir(formatPath(path), true);
        return true;
    }

    @Override
    public boolean deleteFile(String path) throws IOException {
        RetryPolicy retryPolicy = new CountingRetry(MAX_RETRY_TIMES);
        while (retryPolicy.attempt()) {
            int r = cfs.unlink(formatPath(path));
            if (r == 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public long getBlockSizeByte(String path) throws IOException {
        return 0;
    }

    @Override
    public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
        CubeFsLib.StatInfo stat = new CubeFsLib.StatInfo();
        cfs.getAttr(formatPath(path), stat);
        return new UfsDirectoryStatus(formatPath(path), System.getProperty("user.name"), System.getProperty("user.name"), (short) stat.mode);
    }

    @Override
    public List<String> getFileLocations(String path) throws IOException {
        return null;
    }

    @Override
    public List<String> getFileLocations(String path, FileLocationOptions options) throws IOException {
        return null;
    }

    @Override
    public UfsFileStatus getFileStatus(String path) throws IOException {
        CubeFsLib.StatInfo stat = new CubeFsLib.StatInfo();
        cfs.getAttr(formatPath(path), stat);
        String contentHash = UnderFileSystemUtils.approximateContentHash(stat.size, stat.mtime);
        return new UfsFileStatus(formatPath(path), contentHash, stat.size, stat.mtime * 1000 + (long) (stat.mtime_nsec / Math.pow(10, 6)), System.getProperty("user.name"), System.getProperty("user.name"), (short) stat.mode, null, BLOCK_SIZE);
    }

    @Override
    public long getSpace(String path, SpaceType type) throws IOException {
        //return volume namespace capacity
//        switch (type) {
//            case SPACE_TOTAL:
//                return stat.bsize * stat.blocks;
//            case SPACE_USED:
//                return stat.bsize * (stat.blocks - stat.bavail);
//            case SPACE_FREE:
//                return stat.bsize * stat.bavail;
//            default:
//                throw new IOException("Unknown space type: " + type);
//        }
        return -1;
    }

    @Override
    public UfsStatus getStatus(String path) throws IOException {
        CubeFsLib.StatInfo stat = new CubeFsLib.StatInfo();
        cfs.getAttr(formatPath(path), stat);
        if (isDir(stat.mode)) {
            return new UfsDirectoryStatus(formatPath(path), System.getProperty("user.name"), System.getProperty("user.name"), (short) stat.mode);
        } else if (isFile(stat.mode)) {
            String contentHash = UnderFileSystemUtils.approximateContentHash(stat.size, stat.mtime * 1000 + (long) (stat.mtime_nsec / Math.pow(10, 6)));
            return new UfsFileStatus(formatPath(path), contentHash, stat.size, stat.mtime * 1000 + (long) (stat.mtime_nsec / Math.pow(10, 6)), System.getProperty("user.name"), System.getProperty("user.name"), (short) stat.mode, null, BLOCK_SIZE);
        }
        throw new IOException("Failed getStatus: path=" + formatPath(path));
    }

    @Override
    public String getUnderFSType() {
        return Constants.HEADER_CUBEFS;
    }

    @Override
    public boolean isDirectory(String path) throws IOException {
        CubeFsLib.StatInfo stat = new CubeFsLib.StatInfo();
        cfs.getAttr(formatPath(path), stat);
        return isDir(stat.mode);
    }

    @Override
    public boolean isFile(String path) throws IOException {
        CubeFsLib.StatInfo stat = new CubeFsLib.StatInfo();
        cfs.getAttr(formatPath(path), stat);
        return isFile(stat.mode);
    }

    @Override
    public UfsStatus[] listStatus(String path) throws IOException {
        if (isFile(path)) {
            return new UfsStatus[0];
        }
        int fd = cfs.open(formatPath(path), CubeFsMount.O_RDONLY, 0777);
        if (fd < 0) {
            throw CubeFsMount.error(fd, formatPath(path));
        }
        ArrayList<UfsStatus> arrayList = new ArrayList<>();
        CubeFsLib.Dirent dirent = new CubeFsLib.Dirent();
        CubeFsLib.Dirent[] dirents = (CubeFsLib.Dirent[]) dirent.toArray(BATCH_SIZE);
        while (true) {
            int count = cfs.readdir(fd, dirents, dirents.length);
            if (count < 0) {
                cfs.close(fd);
                throw new IOException(String.format("readdir fail for %s,code=%s", path, count));
            }
            if (count == 0) {
                break;
            }
            long[] inodes = new long[count];
            Map<Long, String> names = new HashMap(count);
            for (int i = 0; i < count; i++) {
                inodes[i] = dirents[i].ino;
                names.put(dirents[i].ino, new String(dirents[i].name, 0, dirents[i].nameLen, "utf-8"));
            }
            CubeFsLib.StatInfo statInfo = new CubeFsLib.StatInfo();
            CubeFsLib.StatInfo[] statInfos = (CubeFsLib.StatInfo[]) statInfo.toArray(count);
            CubeFsLib.DirentArray.ByValue direntArray = new CubeFsLib.DirentArray.ByValue();

            direntArray.data = statInfos[0].getPointer();
            direntArray.len = BATCH_SIZE;
            direntArray.cap = BATCH_SIZE;
            int num = cfs.cfs_batch_get_inodes(fd, inodes, direntArray, count);
            if (num < 0) {
                throw new IOException(String.format("cfs_batch_get_inodes fail for %s,code=%s", path, count));
            }
            for (int i = 0; i < num; i++) {
                statInfos[i].read();
                CubeFsLib.StatInfo stat = statInfos[i];
                if (isDir(stat.mode)) {
                    arrayList.add(new UfsDirectoryStatus(formatPath(path), System.getProperty("user.name"), System.getProperty("user.name"), (short) stat.mode));
                } else if (isFile(stat.mode)) {
                    String contentHash = UnderFileSystemUtils.approximateContentHash(stat.size, stat.mtime * 1000 + (long) (stat.mtime_nsec / Math.pow(10, 6)));
                    arrayList.add(new UfsFileStatus(formatPath(path), contentHash, stat.size, stat.mtime * 1000 + (long) (stat.mtime_nsec / Math.pow(10, 6)), System.getProperty("user.name"), System.getProperty("user.name"), (short) stat.mode, null, BLOCK_SIZE));
                }
            }
        }
        UfsStatus[] ufsStatus = new UfsStatus[arrayList.size()];
        return arrayList.toArray(ufsStatus);
    }

    @Override
    public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
        if (exists(path)) {
            return false;
        }
        RetryPolicy retryPolicy = new CountingRetry(MAX_RETRY_TIMES);
        while (retryPolicy.attempt()) {
            String parent = getParentPath(path);
            if (!options.getCreateParent() && !exists(parent)) {
                return false;
            }
            int r = cfs.mkdirs(formatPath(path), options.getMode().toShort());
            if (r == 0) {
                return true;
            }
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                LOG.warn("mkdir error, path:" + path + " retry idx: " + retryPolicy.getAttemptCount());
            }
        }
        return false;
    }

    //todo test
    private String getParentPath(String path) {
        return new AlluxioURI(path).getParent().getPath();
    }

    @Override
    public InputStream open(String path, OpenOptions options) throws IOException {
        int mode = CreateOptions.defaults(mUfsConf).getMode().toShort();
        int fd = cfs.open(formatPath(path), CubeFsMount.O_RDONLY, mode);
        if (fd < 0) {
            throw CubeFsMount.error(fd, formatPath(path));
        }
        return new CubeFsInputStream(cfs, bufferPool, formatPath(path), fd, readBufferSize);
    }

    @Override
    public boolean renameDirectory(String src, String dst) throws IOException {
        if (!isDirectory(src)) {
            LOG.warn("Not rename src:{} to dst:{}, src:{} may be file or not exist.", src, dst);
            return false;
        }
        return rename(src, dst);
    }

    private boolean rename(String src, String dst) throws IOException {

        int r = cfs.rename(formatPath(src), formatPath(dst));
        if (r == CubeFsMount.ENOENT || r == CubeFsMount.EEXIST) {
            return false;
        }
        if (r < 0) {
            throw CubeFsMount.error(r, formatPath(src));
        }
        return true;
    }

    @Override
    public boolean renameFile(String src, String dst) throws IOException {
        if (!isFile(src)) {
            LOG.warn("Not rename src:{} to dst:{}, src:{} may be direct or not exist.", src, dst);
            return false;
        }
        return rename(src, dst);
    }

    @Override
    public void setMode(String path, short mode) throws IOException {
        int fd = cfs.open(formatPath(path), CubeFsMount.O_RDONLY, 777);
        int r = cfs.fchmod(fd, mode);
        if (r <= 0) {
            throw CubeFsMount.error(r, formatPath(path));
        }
    }

    @Override
    public void setOwner(String path, String owner, String group) throws IOException {
        //No op
    }

    @Override
    public boolean supportsFlush() throws IOException {
        return true;
    }


    @Override
    public void close() throws IOException {
        if (cfs != null) {
            cfs.closeClient();
        }
        cfs = null;
    }

    private int checkBufferSize(int bufferSize) {
        return bufferSize < this.bufferSize ? this.bufferSize : bufferSize;
    }

    private boolean isDir(int mode) {
        return (mode & CubeFsMount.S_IFDIR) == CubeFsMount.S_IFDIR;
    }

    private boolean isFile(int mode) {
        return (mode & CubeFsMount.S_IFREG) == CubeFsMount.S_IFREG;
    }
}
