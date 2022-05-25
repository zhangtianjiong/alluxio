package alluxio.underfs.cubefs;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;

@ThreadSafe
public final class CubeFsUnderFileSystemFactory implements UnderFileSystemFactory {
    private static final Logger LOG = LoggerFactory.getLogger(CubeFsUnderFileSystemFactory.class);

    /**
     * Constructs a new {@link CubeFsUnderFileSystemFactory)}
     */
    public CubeFsUnderFileSystemFactory() {
    }

    @Override
    public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
        Preconditions.checkNotNull(path, "path");
        try {
            return CubeFsUnderFileSystem.createInstance(new AlluxioURI(path), conf);
        } catch (IOException e) {
            LOG.error("Failed create CubeFsUnderFileSystem. {}", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean supportsPath(String path) {
        if (path != null) {
            return path.startsWith(Constants.HEADER_CUBEFS);
        }
        return false;
    }
}
