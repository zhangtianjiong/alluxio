package alluxio.underfs.cubefs;

import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.hdfs.HdfsUnderFileSystemFactory;
import org.apache.hadoop.util.StringUtils;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link alluxio.underfs.hdfs.HdfsUnderFileSystem}.
 * CubefsHadoopUnderFileSystem implement based on cubefs-hadoop interface.
 */
@ThreadSafe
public class CubefsHadoopUnderFileSystemFactory extends HdfsUnderFileSystemFactory {
    @Override
    public boolean supportsPath(String path) {
        if (path != null) {
            return path.startsWith(Constants.HEADER_CUBEFS_HADOOP);
        }
        return false;
    }

    @Override
    public boolean supportsPath(String path, UnderFileSystemConfiguration conf) {
        return supportsPath(path, conf);
    }
}
