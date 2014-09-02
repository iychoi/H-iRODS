package org.apache.hadoop.fs.irods;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import org.apache.hadoop.fs.irods.util.HirodsConfigUtils;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.irods.jargon.core.connection.IRODSAccount;
import org.irods.jargon.core.connection.auth.AuthResponse;
import org.irods.jargon.core.exception.AuthenticationException;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.exception.NoResourceDefinedException;
import org.irods.jargon.core.pub.IRODSFileSystem;
import org.irods.jargon.core.pub.io.IRODSFile;
import org.irods.jargon.core.pub.io.IRODSFileFactory;

public class HirodsFileSystem extends FileSystem {

    private static final Log LOG = LogFactory.getLog(HirodsFileSystem.class);
    
    private URI uri;
    private IRODSFileSystem irodsFS;
    private IRODSAccount irodsAccount;
    private Path workingDir;

    public HirodsFileSystem() {
    }

    @Override
    public URI getUri() {
        return this.uri;
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        
        LOG.info("initializing uri for iRODS : " + uri.toString());
        
        String host = uri.getHost();
        if (host == null) {
            host = HirodsConfigUtils.getIrodsHost(conf);
        }
        if (host == null) {
           throw new IOException("invalid host specified"); 
        }
        HirodsConfigUtils.setIrodsHost(conf, host);
        
        int port = uri.getPort();
        if (port == -1) {
            port = HirodsConfigUtils.getIrodsPort(conf);
        }
        HirodsConfigUtils.setIrodsPort(conf, port);
        
        String userAndPass = uri.getUserInfo();
        if (userAndPass == null) {
            String user = HirodsConfigUtils.getIrodsUsername(conf);
            String pass = HirodsConfigUtils.getIrodsPassword(conf);
            
            if(user == null || pass == null) {
                throw new IOException("invalid user and password specified"); 
            }
            
            userAndPass = HirodsConfigUtils.getIrodsUsername(conf) + ":" + HirodsConfigUtils.getIrodsPassword(conf);
        }
        
        String[] userAndPassArr = userAndPass.split(":");
        String user = null;
        String password = null;
        if(userAndPassArr.length > 1) {
            user = userAndPassArr[0];
            HirodsConfigUtils.setIrodsUsername(conf, user);
            password = userAndPassArr[1];
            HirodsConfigUtils.setIrodsPassword(conf, password);
        }
        
        String zone = null;
        String[] dirs = uri.getPath().split("/");
        if(dirs.length >= 1) {
            zone = dirs[0];
            if(zone == null || zone.equals("")) {
                zone = dirs[1];
            }
        }
        if(zone == null) {
            throw new IOException("invalid zone specified"); 
        }
        HirodsConfigUtils.setIrodsZone(conf, zone);
        
        if (this.irodsFS == null) {
            
            LOG.info("connecting to iRODS");
            
            try {
                this.irodsFS = IRODSFileSystem.instance();
            } catch (JargonException ex) {
                throw new IOException(ex);
            }
            this.irodsAccount = createIRODSAccount(host, port, zone, user, password);
            
            AuthResponse response;
            try {
                response = connectIRODS(this.irodsFS, this.irodsAccount);
            } catch (AuthenticationException ex) {
                LOG.error(ex);
                throw new IOException(ex);
            } catch (JargonException ex) {
                throw new IOException(ex);
            }
            
            if(!response.isSuccessful()) {
                throw new IOException("Cannot authenticate to IRODS");
            }
        }
        
        setConf(conf);
        this.uri = uri;
        this.workingDir = new Path(this.irodsAccount.getHomeDirectory()).makeQualified(this);
    }
    
    private static String makeDefaultHome(String zone) {
        String defaultdir = "/" + zone.trim();
        return defaultdir;
    }
    
    private static String makeDefaultStorageResource() {
        return "";
    }
    
    private static IRODSAccount createIRODSAccount(String host, int port, String zone, String user, String password) throws IOException {
        IRODSAccount account = null;
        String home = makeDefaultHome(zone);
        String resource = makeDefaultStorageResource();
        
        try {
            //LOG.info("IRODS Account Info - host : " + host);
            //LOG.info("IRODS Account Info - port : " + port);
            //LOG.info("IRODS Account Info - zone : " + zone);
            //LOG.info("IRODS Account Info - user : " + user);
            //LOG.info("IRODS Account Info - password : " + password);
            //LOG.info("IRODS Account Info - home : " + home);
            //LOG.info("IRODS Account Info - resource : " + resource);
            
            account = IRODSAccount.instance(host, port, user, password, home, zone, resource);
            return account;
        } catch (JargonException ex) {
            throw new IOException(ex);
        }
    }
    
    private static AuthResponse connectIRODS(IRODSFileSystem fs, IRODSAccount account) throws AuthenticationException, JargonException {
        return fs.getIRODSAccessObjectFactory().authenticateIRODSAccount(account);
    }
    
    private IRODSFileFactory getIRODSFileFactory() throws IOException {
        try {
            return this.irodsFS.getIRODSFileFactory(this.irodsAccount);
        } catch (JargonException ex) {
            throw new IOException(ex);
        }
    }
    
    @Override
    public String getName() {
        return getUri().toString();
    }

    @Override
    public Path getWorkingDirectory() {
        return this.workingDir;
    }
    
    @Override
    public void setWorkingDirectory(Path path) {
        this.workingDir = makeAbsolute(path);
    }
    
    private Path makeAbsolute(Path path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(this.workingDir, path);
    }
    
    private IRODSFile makeIrodsPath(Path path) throws IOException {
        Path absolutePath = makeAbsolute(path);
        return createIrodsPath(absolutePath.toUri());
    }
    
    private IRODSFile createIrodsPath(IRODSFile path, String name) throws IOException {
        try {
            return getIRODSFileFactory().instanceIRODSFile(path.getPath(), name);
        } catch (JargonException ex) {
            throw new IOException(ex);
        }
    }
    
    private IRODSFile createIrodsPath(URI uri) throws IOException {
        return createIrodsPath(uri.getPath());
    }
    
    private IRODSFile createIrodsPath(String path) throws IOException {
        try {
            return getIRODSFileFactory().instanceIRODSFile(path);
        } catch (JargonException ex) {
            throw new IOException(ex);
        }
    }
    
    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        IRODSFile ipath = makeIrodsPath(path);
        return ipath.mkdirs();
    }
    
    @Override
    public boolean isFile(Path path) throws IOException {
        IRODSFile ipath = makeIrodsPath(path);
        return ipath.isFile();
    }
    
    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        IRODSFile ipath = makeIrodsPath(f);
        if(!ipath.exists()) {
            return null;
        }
        
        if(ipath.isFile()) {
            return new FileStatus[]{
                new IrodsFileStatus(f.makeQualified(this), ipath)
            };
        }
        
        List<FileStatus> ret = new ArrayList<FileStatus>();
        for (String p : ipath.list()) {
            ret.add(getFileStatus(new Path(f, p)));
        }
        return ret.toArray(new FileStatus[0]);
    }
    
    /**
     * This optional operation is not yet supported.
     */
    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        throw new IOException("Not supported");
    }
    
    @Override
    public FSDataOutputStream create(Path file, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        IRODSFile ipath = makeIrodsPath(file);
        if(ipath.exists()) {
            if (overwrite) {
                delete(file);
            } else {
                throw new IOException("File already exists: " + file);
            }
        } else {
            Path parent = file.getParent();
            if(parent != null) {
                if (!mkdirs(parent)) {
                    throw new IOException("Mkdirs failed to create " + parent.toString());
                }
            }
        }
        
        int bSize = Math.max(HirodsConfigUtils.getIrodsOutputBufferSize(getConf()), bufferSize);
        try {
            //return new FSDataOutputStream(new BufferedOutputStream(this.irodsFileFactory.instanceIRODSFileOutputStream(ipath), bSize), this.statistics);
            return new FSDataOutputStream(new BufferedOutputStream(getIRODSFileFactory().instanceIRODSFileOutputStream(ipath), bSize), this.statistics);
        } catch (NoResourceDefinedException ex) {
            throw new IOException("Cannot get output stream from " + file);
        } catch (JargonException ex) {
            throw new IOException(ex);
        }
    }
    
    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        IRODSFile ipath = makeIrodsPath(path);
        if (!ipath.exists()) {
            throw new IOException("No such file.");
        }
        if (ipath.isDirectory()) {
            throw new IOException("Path " + path + " is a directory.");
        }
        int bSize = Math.max(HirodsConfigUtils.getIrodsInputBufferSize(getConf()), bufferSize);
        // BufferedHirodsInputStream is deprecated as it's same as bufferedInputStream
        //return new FSDataInputStream(new BufferedHirodsInputStream(new HirodsInputStream(getConf(), ipath, this.irodsFS, getIRODSFileFactory(), this.statistics), bSize));
        return new FSDataInputStream(new BufferedInputStream(new HirodsInputStream(getConf(), ipath, this.irodsFS, getIRODSFileFactory(), this.statistics), bSize));
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        IRODSFile isrc = makeIrodsPath(src);
        IRODSFile idst = makeIrodsPath(dst);
        
        if (!isrc.exists()) {
            // src path doesn't exist
            return false;
        }
        if (idst.isDirectory()) {
            idst = createIrodsPath(idst, isrc.getName());
        }
        if (idst.exists()) {
            // dst path already exists - can't overwrite
            return false;
        }
        
        IRODSFile idstParent = createIrodsPath(idst.getParent());
        if(idstParent != null) {
            if (!idstParent.exists() || idstParent.isFile()) {
                // dst parent doesn't exist or is a file
                return false;
            }
        }
        
        return isrc.renameTo(idst);
    }
    
    private boolean deleteAll(IRODSFile path) throws IOException {
        if(path.isFile()) {
            // remove file
            return path.delete();
        } else if(path.isDirectory()) {
            String[] entries = path.list();
            if(entries != null) {
                for(String entry : entries) {
                    IRODSFile entry_file = createIrodsPath(path, entry);
                    deleteAll(entry_file);
                }
            }
            // remove dir
            return path.delete();
        }
        return true;
    }
    
    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        IRODSFile ipath = makeIrodsPath(path);
        if (!ipath.exists()) {
            return false;
        }
        
        if (ipath.isFile()) {
            return ipath.delete();
        } else {
            // directory?
            if(recursive) {
                return deleteAll(ipath);
            } else {
                return ipath.delete();
            }
        }
    }
    
    @Override
    public boolean delete(Path path) throws IOException {
        return delete(path, true);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        IRODSFile ipath = makeIrodsPath(f);
        if(!ipath.exists()) {
            throw new FileNotFoundException(f + ": No such file or directory.");
        }
        
        return new IrodsFileStatus(f.makeQualified(this), ipath);
    }
    
    @Override
    public long getDefaultBlockSize() {
        return IrodsFileStatus.DEFAULT_IRODS_BLOCKSIZE;
    }
    
    private static class IrodsFileStatus extends FileStatus {

        public static final long DEFAULT_IRODS_BLOCKSIZE = 64*1024*1024; // 64MB
        
        IrodsFileStatus(Path f, IRODSFile ifile) throws IOException {
            super(findLength(ifile), ifile.isDirectory(), 1, findBlocksize(), 0, f);
        }

        private static long findLength(IRODSFile ifile) {
            if (!ifile.isDirectory()) {
                return ifile.length();
            }
            return 0;
        }

        private static long findBlocksize() {
            return DEFAULT_IRODS_BLOCKSIZE;
        }
    }
    
    @Override
    public void close() throws IOException {
        try {
            this.irodsFS.close();
        } catch (JargonException ex) {
            throw new IOException(ex);
        }
        
        super.close();
    }
}
