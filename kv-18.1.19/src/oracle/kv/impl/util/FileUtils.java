/*-
 * Copyright (C) 2011, 2018 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.impl.util;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Collection of utilities for file operations
 */
public class FileUtils {

    /**
     * Copy a file
     * @param sourceFile the file to copy from, which must exist
     * @param destFile the file to copy to.  The file is created if it does
     *        not yet exist.
     */
    public static void copyFile(File sourceFile, File destFile)
        throws IOException {

        if (!destFile.exists()) {
            destFile.createNewFile();
        }

        try (final FileInputStream source = new FileInputStream(sourceFile);
             final FileOutputStream dest = new FileOutputStream(destFile)) {
            final FileChannel sourceChannel = source.getChannel();
            dest.getChannel().transferFrom(sourceChannel, 0,
                                           sourceChannel.size());
        }
    }

    /**
     * Copy a directory.
     * @param fromDir the directory to copy from, which must exist.
     * @param toDir the directory to copy to. The directory is created if it
     *        does not yet exist.
     */
    public static void copyDir(File fromDir, File toDir)
        throws IOException {

        if (fromDir == null || toDir == null) {
            throw new NullPointerException("File location error");
        }

        if (!fromDir.isDirectory()) {
            throw new IllegalStateException(
                fromDir +  " should be a directory");
        }

        if (!fromDir.exists()) {
            throw new IllegalStateException(
                fromDir +  " does not exist");
        }

        if (!toDir.exists() && !toDir.mkdirs()) {
            throw new IllegalStateException(
                "Unable to create copy dest dir:" + toDir);
        }

        File [] fileList = fromDir.listFiles();
        if (fileList != null && fileList.length != 0) {
            for (File file : fileList) {
                if (file.isDirectory()) {
                    copyDir(file, new File(toDir, file.getName()));
                } else {
                    copyFile(file, new File(toDir, file.getName()));
                }
            }
        }
    }

    /**
     * Write a string to file.
     */
    public static void writeStringToFile(File destFile, String text)
        throws IOException {

        try (final BufferedWriter out =
                 new BufferedWriter(new FileWriter(destFile))) {
            out.write(text);
        }
    }

    /**
     * Write binary data to file.
     */
    public static void writeBytesToFile(final File destFile,
                                        final byte[] bytes)
        throws IOException {

        OutputStream output = null;
        try {
            output = new BufferedOutputStream(new FileOutputStream(destFile));
            output.write(bytes);
        }
        finally {
            if (output != null) {
                output.close();
            }
        }
    }

    /**
     * Recursively delete a directory and its contents.  Makes use of features
     * introduced in Java 7.  Does NOT follow symlinks.
     */
    public static boolean deleteDirectory(File d) {
        try {
            Files.walkFileTree(d.toPath(), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path path,
                                                 BasicFileAttributes attrs)
                    throws IOException {

                    Files.delete(path);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path path,
                                                          IOException ioe)
                    throws IOException {

                    if (ioe != null) {
                        throw ioe;
                    }

                    Files.delete(path);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * Verifies that the specified file exists and is a directory. If both
     * of the checks succeed null is returned, otherwise a description of the
     * failure is returned.
     *
     * @param directory the directory to verify
     * @return a reason string if the verify fails
     */
    public static String verifyDirectory(File directory) {
        if (!directory.exists()) {
            return directory.getAbsolutePath() + " does not exist";
        }
        if (!directory.isDirectory()) {
            return directory.getAbsolutePath() + " is not a directory";
        }
        return null;
    }

    /**
     * Verifies that the specified file exists and is a directory. If
     * requiredSize is > 0 the directory size is also checked. If all of
     * the checks succeed null is returned, otherwise a description of the
     * failure is returned.
     *
     * @param directory the directory to verify
     * @param requiredSize the required size of the directory, or 0L
     * @return a reason string if the verify fails
     */
    public static String verifyDirectory(File directory, long requiredSize) {
        final String reason = verifyDirectory(directory);
        if ((reason == null) && (requiredSize > 0L)) {
            final long actual = getDirectorySize(directory);
            if (actual < requiredSize) {
                return  directory.getAbsoluteFile() + " size of " + actual +
                        " is less than the required size of " + requiredSize;
            }
        }
        return reason;
    }

    /**
     * Gets the size of the specified directory. If the directory does not
     * exist, is not a directory, or if there is an exception getting the
     * size an IllegalArgumentException is thrown.
     *
     * @param directoryName the directory to get the size
     * @return the directory size
     */
    public static long getDirectorySize(String directoryName) {
        final File directory = new File(directoryName);
        final String reason = verifyDirectory(directory);
        if (reason != null) {
            throw new IllegalArgumentException("Cannot get size, " + reason);
        }
        return getDirectorySize(directory);
    }

    /**
     * Gets the size of the specified directory. If there is an exception
     * getting the size an IllegalArgumentException is thrown.
     *
     * @param directory the directory to get the size
     * @return the directory size
     */
    public static long getDirectorySize(File directory) {
        try {
            final FileStore fileStore =
                       Files.getFileStore(FileSystems.getDefault().
                                          getPath(directory.getAbsolutePath()));
            return fileStore.getTotalSpace();
        } catch (IOException ex) {
            throw new IllegalArgumentException("Exception getting size for: " +
                                               directory.getAbsoluteFile() +
                                              " - " + ex.getLocalizedMessage());
        }
    }
}
