package external_sort;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class FileSplitSortMergeUtil{
    /**
     * split the large file into several sorted temp files
     *
     * @param fileName
     * @param tempDirectory
     * @return List<File>
     * @throws IOException
     */
    public static List<File> splitAndSortTempFiles(final String fileName, final String tempDirectory,
            final int noOfSplits, final StringComparator cmp) throws IOException {
        List<File> files = new ArrayList<>();
        RandomAccessFile raf = new RandomAccessFile(fileName, "r");
        long sourceSize = raf.length();
        long bytesPerSplit = sourceSize / noOfSplits;
        long remainingBytes = sourceSize % noOfSplits;
        int maxReadBufferSize = 8 * 1024; // 8KB
        int fileCounter = 1;
        for (int i = 1; i <= noOfSplits; i++) {
            File dir = new File(tempDirectory);
            if (dir.exists()) {
                dir.delete();
            }
            dir.mkdir();
            File file = new File(tempDirectory + "/temp-file-" + fileCounter + ".txt");
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file));
            if (bytesPerSplit > maxReadBufferSize) {
                long numReads = bytesPerSplit / maxReadBufferSize;
                long numRemainingRead = bytesPerSplit % maxReadBufferSize;
                for (int j = 0; j < numReads; j++) {
                    readWrite(raf, bos, maxReadBufferSize);
                }
                if (numRemainingRead > 0) {
                    readWrite(raf, bos, numRemainingRead);
                }
            } else {
                readWrite(raf, bos, bytesPerSplit);
            }
            file = sortFileContent(file, cmp);
            files.add(file);
            fileCounter++;
            bos.close();
        }
        if (remainingBytes > 0) {
            File file = new File(tempDirectory + "/temp-file-" + fileCounter + ".txt");
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file));
            readWrite(raf, bos, remainingBytes);
            file = sortFileContent(file, cmp);
            files.add(file);
            bos.close();
        }
        return files;
    }
    private static void readWrite(RandomAccessFile raf, BufferedOutputStream bos, long numBytes) throws IOException {
        byte[] buf = new byte[(int) numBytes];
        int val = raf.read(buf);
        if (val != -1) {
            bos.write(buf);
            bos.flush();
        }
    }
    /**
     * Sort file content
     *
     * @param file
     * @return file
     * @throws IOException
     */
    private static File sortFileContent(File file, StringComparator cmp) throws IOException {
        List<String> lines = new ArrayList<>();
        try (Stream<String> ln = Files.lines(file.toPath())) {
            lines = ln.collect(Collectors.toList());
        }
        Collections.sort(lines, cmp);
        try (BufferedWriter bw = Files.newBufferedWriter(file.toPath())) {
            for (String line : lines) {
                bw.write(line);
                bw.write("\r\n");
            }
        }
        return file;
    }
    /**
     * Merge all sorted files into a big file in a sorted manner
     *
     * @param files
     * @param outputFile
     * @param cmp
     * @throws IOException
     */
    public static void mergeSortedFiles(final List<File> files, final String outputFile, final StringComparator cmp)
    throws IOException {
        List<BufferedReader> brReaders = new ArrayList<>();
        TreeMap<String, BufferedReader> map = new TreeMap<>(cmp);
        File f = new File(outputFile);
        if (f.exists()) {
        f.delete();
        }
        BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile, true));
        try {
        for (File file : files) {
            BufferedReader br = new BufferedReader(new FileReader(file));
            brReaders.add(br);
            String line = br.readLine();
            map.put(line, br);
        }
        while (!map.isEmpty()) {
            Map.Entry<String, BufferedReader> nextToGo = map.pollFirstEntry();
            bw.write(nextToGo.getKey());
            bw.write("\r\n");
            String line = nextToGo.getValue().readLine();
            if (line != null) {
                map.put(line, nextToGo.getValue());
            }
        }
        } finally {
        if (brReaders != null) {
            for (BufferedReader br : brReaders) {
                br.close();
            }
            File dir = files.get(0).getParentFile();
            // for (File file : files) {
            //     file.delete();
            // }
            // if (dir.exists()) {
            //     dir.delete();
            // }
        }
        if (bw != null) {
            bw.close();
        }
        }
    }
}