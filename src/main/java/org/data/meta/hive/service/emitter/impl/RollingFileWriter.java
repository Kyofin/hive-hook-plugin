package org.data.meta.hive.service.emitter.impl;

import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RollingFileWriter {
    private static final Logger LOG = LoggerFactory.getLogger(RollingFileWriter.class);
    private static final String FILE_TMP_DIR = "/tmp";
    private long lastCheckFileTime = 0L;
    private int lines = 0;
    private FileWriter writer = null;
    private File file = null;
    private final ReentrantLock writerLock = new ReentrantLock();
    private final int maxLineNum;
    private final String filePrefix;

    public RollingFileWriter(int maxLineNum, String filePrefix) {
        this.maxLineNum = maxLineNum;
        this.filePrefix = filePrefix;
    }

    public void writeLineWithLock(String line) throws IOException {
        try {
            this.writerLock.lock();
            this.writerLine(line);
            ++this.lines;
        } finally {
            this.writerLock.unlock();
        }

    }

    private void writerLine(String line) throws IOException {
        this.getWriter().write(line + "\n");
        this.getWriter().flush();
    }

    private FileWriter getWriter() throws IOException {
        if (this.writer == null || this.lines >= this.maxLineNum || this.file == null) {
            this.rollingWriter();
        }

        long now = System.currentTimeMillis();
        if (this.lastCheckFileTime + 1000L < now) {
            this.lastCheckFileTime = now;
            if (!this.file.exists()) {
                this.rollingWriter();
            }
        }

        return this.writer;
    }

    private void rollingWriter() throws IOException {
        if (this.writer != null) {
            this.writer.flush();
            this.writer.close();
            this.lines = 0;
        }

        this.file = this.findWritingFile();
        this.writer = new FileWriter(this.file, true);
        this.deleteOldFiles(this.file);
    }

    private void deleteOldFiles(File newFile) {
        final long lastModifiedThreshold = System.currentTimeMillis() - 60000L;
        final String currentName = newFile.getName();
        File dir = newFile.getParentFile();
        File[] existFileArr = dir.listFiles(new FileFilter() {
            public boolean accept(File f) {
                return f.getName().contains(RollingFileWriter.this.filePrefix) && f.lastModified() < lastModifiedThreshold && !f.getName().equals(currentName);
            }
        });
        if (existFileArr != null && existFileArr.length != 0) {
            if (existFileArr.length != 1) {
                List<File> existFiles = Arrays.asList(existFileArr);
                Collections.sort(existFiles);

                for(int i = 0; i < existFiles.size() - 1; ++i) {
                    File f = (File)existFiles.get(i);
                    boolean deleteResult = f.delete();
                    LOG.warn("delete hook event file : {}:{}", f.getName(), deleteResult);
                }

            }
        }
    }

    private File findWritingFile() {
        String dir = "/tmp" + File.separator + System.getProperty("user.name") + File.separator;
        File fileDir = new File(dir);
        if (!fileDir.exists() && !fileDir.mkdirs()) {
            LOG.error("create log file failed : {}", this.file);
        }

        return new File(dir + this.filePrefix + "." + System.currentTimeMillis() + ".log");
    }
}
