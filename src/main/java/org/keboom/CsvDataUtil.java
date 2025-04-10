package org.keboom;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author: keboom
 * @date: 2025/4/7
 */
@Slf4j
public class CsvDataUtil {

    private static final int BUFFER_SIZE = 1024 * 1024 * 100; // 100MB buffer
    private static final int MAX_OPEN_FILES = 3;

    /**
     * 文件分割，按照每个文件的行数进行分割
     *
     * @param sourceFilePath
     * @param targetFilePath
     * @param fileSize
     * @param fileFormat
     * @param sum
     * @return
     */
    public static List<String> oneToMoreBySize(String sourceFilePath, String targetFilePath, Integer fileSize, String fileFormat, AtomicLong sum) {
        List<String> moreFiles = new ArrayList<>();
        AtomicLong size = new AtomicLong(0);
        Map<Long, CSVPrinter> map = new HashMap<>();

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(sourceFilePath)); CSVParser csvParser = CSVParser.parse(reader, CSVFormat.DEFAULT)) {

            for (CSVRecord csvRecord : csvParser) {
                long fileIndex = size.get() / fileSize;
                CSVPrinter printer = map.get(fileIndex);

                if (printer == null) {
                    // Close old printers to prevent too many open files
                    closeOldPrinters(map, fileIndex);

                    String filePath = String.format("%s-%d.%s", targetFilePath, fileIndex, fileFormat);
                    log.info("Processed {} records, creating file: {}", size.get(), filePath);
                    moreFiles.add(filePath);

                    BufferedWriter writer = new BufferedWriter(Files.newBufferedWriter(Paths.get(filePath)), BUFFER_SIZE);
                    printer = new CSVPrinter(writer, CSVFormat.DEFAULT);
                    map.put(fileIndex, printer);
                }

                size.incrementAndGet();
                sum.incrementAndGet();
                printer.printRecord(csvRecord);
            }

        } catch (IOException e) {
            log.error("Error processing CSV file: {}", sourceFilePath, e);
            throw new RuntimeException("Failed to process CSV file", e);
        } finally {
            closeAllPrinters(map);
        }

        return moreFiles;
    }


    /**
     * 多个文件合并成一个文件
     *
     * @param sourceFilePathList 源文件列表
     * @param targetFilePath     目标文件路径
     */
    public static void moreToOne(List<String> sourceFilePathList, String targetFilePath) {
        AtomicLong sum = new AtomicLong(0);

        try (BufferedWriter writer = new BufferedWriter(Files.newBufferedWriter(Paths.get(targetFilePath)), BUFFER_SIZE); CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT)) {

            for (String path : sourceFilePathList) {
                try (BufferedReader reader = Files.newBufferedReader(Paths.get(path)); CSVParser parser = CSVParser.parse(reader, CSVFormat.DEFAULT)) {

                    for (CSVRecord record : parser) {
                        printer.printRecord(record);
                        sum.incrementAndGet();
                    }
                }
                // Explicitly flush after each file
                printer.flush();
            }

            log.info("Total records processed: {}", sum.get());
        } catch (IOException e) {
            log.error("Error merging files", e);
            throw new RuntimeException("Failed to merge files", e);
        }
    }

    /**
     * 源文件大，过滤文件也大。那么将源文件做分割，然后过滤
     *
     * @param filterFile     要过滤的文件
     * @param sourceFile     过滤原文件
     * @param targetFilePath 过滤后保存的文件
     */
    public static void filterFile(String filterFile, String sourceFile, String targetFilePath, int filterPosition) {
        //原文件分成5000万一份，每份进行过滤
        List<String> moreFiles = oneToMoreBySize(sourceFile, getFileBasePath(sourceFile), 500_0000, "csv", new AtomicLong(0));

        try {
            for (String file : moreFiles) {
                smallSourceFileFilterBigFilterFile(file, filterFile, targetFilePath, filterPosition);
                Files.delete(Paths.get(file));
            }
        } catch (IOException e) {
            log.error("Error in filterFile", e);
            throw new RuntimeException("Failed to filter files", e);
        }
    }

    /**
     * 源文件小，过滤文件大。将源文件放到内存中。
     * <p>
     * csv 文件默认没有header
     *
     * @param sourceFile     源文件路径
     * @param filterFile     过滤文件路径
     * @param targetFilePath 目标文件路径
     * @param filterPosition 过滤字段在CSV中的位置
     * @throws IOException
     */
    private static void smallSourceFileFilterBigFilterFile(String sourceFile, String filterFile, String targetFilePath, int filterPosition) {
        HashMap<String, CSVRecord> sourceFieldMap = new HashMap<>();

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(sourceFile)); CSVParser parser = CSVParser.parse(reader, CSVFormat.DEFAULT)) {
            for (CSVRecord record : parser) {
                sourceFieldMap.put(record.get(filterPosition), record);
            }
        } catch (IOException e) {
            log.error("Error in smallSourceFileFilterBigFilterFile", e);
            throw new RuntimeException("Failed to filter files", e);
        }

        log.info("读取过滤文件:{} 原文件行数：{}", sourceFile, sourceFieldMap.size());
        int sum = sourceFieldMap.size();

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filterFile));
             CSVParser parser = CSVParser.parse(reader, CSVFormat.DEFAULT);
             BufferedWriter writer = new BufferedWriter(Files.newBufferedWriter(Paths.get(targetFilePath)), BUFFER_SIZE);
             CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT)) {

            for (CSVRecord record : parser) {
                String s = record.get(filterPosition);
                if (sourceFieldMap.containsKey(s)) {
                    sourceFieldMap.remove(s);
                }
            }

            for (Map.Entry<String, CSVRecord> entry : sourceFieldMap.entrySet()) {
                CSVRecord v = entry.getValue();
                printer.printRecord(v);
            }

            printer.flush();
        } catch (IOException e) {
            log.error("Error in filterFile", e);
            throw new RuntimeException("Failed to filter files", e);
        }

        log.info("{}-原文件数:{},过滤数:{}", sourceFile, sum, (sum - sourceFieldMap.size()));
    }

    /**
     * 源文件大，过滤文件小，将过滤文件放入内存
     */
    public static void bigSourceFileFilterSmallFile(String sourceFile, String filterFile, String targetFilePath, int filterPosition) {
        log.info("开始过滤 sourceFile:{}  filterFile:{}  targetFile:{} ", sourceFile, filterFile, targetFilePath);

        try (BufferedWriter writer = new BufferedWriter(Files.newBufferedWriter(Paths.get(targetFilePath)), BUFFER_SIZE); CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT)) {

            HashSet<String> filterFieldSet = new HashSet<>();
            try (BufferedReader reader = Files.newBufferedReader(Paths.get(filterFile)); CSVParser parser = CSVParser.parse(reader, CSVFormat.DEFAULT)) {
                for (CSVRecord record : parser) {
                    String s = record.get(filterPosition);
                    filterFieldSet.add(s);
                }
            }

            AtomicLong filterNum = new AtomicLong();
            try (BufferedReader reader = Files.newBufferedReader(Paths.get(sourceFile)); CSVParser parser = CSVParser.parse(reader, CSVFormat.DEFAULT)) {
                for (CSVRecord record : parser) {
                    if (filterFieldSet.contains(record.get(filterPosition))) {
                        filterNum.getAndIncrement();
                    } else {
                        printer.printRecord(record);
                    }
                }
            }
            printer.flush();
            log.info("开始过滤 sourceFile:{}  filterFile:{}  targetFile:{} , 过滤条数: {} ", sourceFile, filterFile, targetFilePath, filterNum.get());
        } catch (IOException e) {
            log.error("Error in bigSourceFileFilterSmallFile", e);
            throw new RuntimeException("Failed to filter files", e);
        }
    }

    /**
     * 一个源文件，过滤多个文件
     *
     * @param sourceFile 文件数据大小无限制
     * @param filterList 单个过滤文件不能太大
     * @param targetFile
     */
    public static void filterMoreFileByBigSourceFile(String sourceFile, List<String> filterList, String targetFile, int filterPosition) {
        List<String> sourceFileSplit = oneToMoreBySize(sourceFile, targetFile.replace(".csv", "_split"), 500_0000, "csv", new AtomicLong(0));
        sourceFileSplit.stream().forEach(file -> {
            filterMoreFile(file, filterList, targetFile, filterPosition);
        });
    }

    /**
     * 过滤文件，从源文件中过滤
     *
     * @param sourceFile 文件数据不能超过5000万
     * @param filterList
     * @param targetFile
     */
    public static void filterMoreFile(String sourceFile, List<String> filterList, String targetFile, int filterPosition) {
        AtomicLong sum = new AtomicLong(0L);
        AtomicLong filterNum = new AtomicLong(0L);
        AtomicLong total = new AtomicLong(0L);
        HashMap<String, CSVRecord> sourceFieldMap = new HashMap<>();

        try (BufferedReader sourceReader = Files.newBufferedReader(Paths.get(sourceFile)); CSVParser sourceParser = CSVParser.parse(sourceReader, CSVFormat.DEFAULT)) {
            for (CSVRecord record : sourceParser) {
                String s = record.get(filterPosition);
                sourceFieldMap.put(s, record);
                sum.incrementAndGet();
            }
        } catch (IOException e) {
            log.error("Error reading source file: {}", sourceFile, e);
            throw new RuntimeException("Failed to read source file", e);
        }

        for (String filterFile : filterList) {
            try (BufferedReader filterReader = Files.newBufferedReader(Paths.get(filterFile)); CSVParser filterParser = CSVParser.parse(filterReader, CSVFormat.DEFAULT)) {
                log.info("开始处理文件: {}", filterFile);
                for (CSVRecord record : filterParser) {
                    String s = record.get(filterPosition);

                    if (sourceFieldMap.containsKey(s)) {
                        sourceFieldMap.remove(s);
                        filterNum.incrementAndGet();
                    }
                }
            } catch (IOException e) {
                log.error("Error reading filter file: {}", filterFile, e);
                throw new RuntimeException("Failed to read filter file", e);
            }
        }

        try (BufferedWriter writer = new BufferedWriter(Files.newBufferedWriter(Paths.get(targetFile)), BUFFER_SIZE); CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT)) {
            for (Map.Entry<String, CSVRecord> entry : sourceFieldMap.entrySet()) {
                CSVRecord v = entry.getValue();
                printer.printRecord(v);
                total.incrementAndGet();
            }
            printer.flush();
        } catch (IOException e) {
            log.error("Error writing target file: {}", targetFile, e);
            throw new RuntimeException("Failed to write target file", e);
        }

        log.info("过滤完毕，总数: {}, 过滤数: {}, 剩余数量: {}", sum.get(), filterNum.get(), total.get());
    }

    /**
     * 一个源文件，过滤多个文件
     *
     * @param sourceFileList 单个源文件不能太大
     * @param filterFile
     * @param targetFile
     * @param filterPosition
     */
    public static void filterFileByMoreSourceFile(List<String> sourceFileList, String filterFile, String targetFile, int filterPosition) {
        sourceFileList.stream().forEach(file -> {
            smallSourceFileFilterBigFilterFile(file, filterFile, targetFile, filterPosition);
        });
    }

    /**
     * 合并两个文件，均匀插入的方式。
     * 比如A文件有 300w 行，B文件有 100w 行，那么合并时就先写入 3 行 A 文件，在写入 1 行 B 文件，在写入 3 行 A 文件……
     *
     * @param biggerPath
     * @param littlePath
     * @param mergePath
     */
    public static void mergeByUniformInsert(String biggerPath, String littlePath, String mergePath) {
        try (Reader littleReader = Files.newBufferedReader(Paths.get(littlePath));
             CSVParser littleCsvParser = CSVParser.parse(littleReader, CSVFormat.DEFAULT);
             Reader biggerReader = Files.newBufferedReader(Paths.get(biggerPath));
             CSVParser biggerCsvParser = CSVParser.parse(biggerReader, CSVFormat.DEFAULT);
             BufferedWriter writer = Files.newBufferedWriter(Paths.get(mergePath),
                     StandardCharsets.UTF_8, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT)) {

            log.info("Start to mergeByUniformInsert. biggerPath: {} littlePath: {} mergePath: {}", biggerPath, littlePath, mergePath);

            long biggerRow = getFileRowNum(biggerPath);
            long littleRow = getFileRowNum(littlePath);

            log.info("biggerRow = {}, littleRow = {}", biggerRow, littleRow);

            if (biggerRow < littleRow) {
                throw new RuntimeException("biggerRow < littleRow, please check the file.");
            }
            long step = (biggerRow / littleRow) + 1;
            // 每从 biggerFile 中取出 step 行数据，则从 littleFile 中取出一行数据
            int count = 0;
            Iterator<CSVRecord> littleIterator = littleCsvParser.iterator();
            for (CSVRecord biggerRecord : biggerCsvParser) {
                if (count % step == 0 && littleIterator.hasNext()) {
                    // 从 littleFile 中取出一行数据
                    CSVRecord littleRecord = littleIterator.next();
                    csvPrinter.printRecord(littleRecord);
                }
                csvPrinter.printRecord(biggerRecord);
                count++;
            }
            // 如果 littleFile 还有数据，则继续写入
            while (littleIterator.hasNext()) {
                CSVRecord littleRecord = littleIterator.next();
                csvPrinter.printRecord(littleRecord);
            }
            csvPrinter.flush();
        } catch (IOException e) {
            log.error("Error in mergeByUniformInsert", e);
            throw new RuntimeException("Failed to merge files", e);
        }
    }

    /**
     * 获取文件行数，如果方便的话，用 wc -l filePath 或许更快
     *
     * @param filePath
     * @return
     */
    public static long getFileRowNum(String filePath) {
        long rowNum = 0;
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath))) {
            while (reader.readLine() != null) rowNum++;
        } catch (IOException e) {
            log.error("Error in getFileRowNum", e);
            throw new RuntimeException("Failed to get file row number", e);
        }
        return rowNum;
    }

    /**
     * 两文件去重。将 littlePathFile 文件放到内存中，过滤 biggerPathFile 与 littlePathFile 重复的部分，将 biggerPathFile 写入 outputBiggerPathFile
     *
     * @param biggerPathFile
     * @param littlePathFile
     * @param outputBiggerPathFile
     * @param filterPosition
     */
    public static void twoFileDistinct(String biggerPathFile, String littlePathFile, String outputBiggerPathFile, int filterPosition) {
        try (Reader littleReader = Files.newBufferedReader(Paths.get(littlePathFile));
             CSVParser littleCsvParser = CSVParser.parse(littleReader, CSVFormat.DEFAULT);
             Reader biggerReader = Files.newBufferedReader(Paths.get(biggerPathFile));
             CSVParser biggerCsvParser = CSVParser.parse(biggerReader, CSVFormat.DEFAULT);
             BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputBiggerPathFile),
                     StandardCharsets.UTF_8, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT)) {

            log.info("Start to distinct. biggerPathFile: {} littlePathFile: {} outputBiggerPathFile: {}", biggerPathFile, littlePathFile, outputBiggerPathFile);

            int duplicateCount = 0;

            HashSet<String> littleSet = new HashSet<>();
            for (CSVRecord record : littleCsvParser) {
                littleSet.add(record.get(filterPosition));
            }

            log.info("Put all littlePathFile data into set. littleSet.size() = {}", littleSet.size());

            for (CSVRecord csvRecord : biggerCsvParser) {
                String row = csvRecord.get(filterPosition);
                if (littleSet.contains(row)) {
                    duplicateCount++;
                    continue;
                }
                csvPrinter.printRecord(csvRecord);
            }

            log.info("Duplicated data size = {}", duplicateCount);

        } catch (IOException e) {
            log.error("Error in twoFileDistinct", e);
            throw new RuntimeException("Failed to distinct files", e);
        }

    }

    /**
     * 从源数据中取出指定数量数据，生成目标数据和剩余数据
     *
     * @param sourceFilePath   源数据文件, 文件里面都是 md5，如果不是 md5的直接跳过
     * @param targetFilePath   目标数据文件
     * @param residualFilePath 剩余数据
     * @param totalCount       要取出的总数
     * @param step             取数的步长，步长是指每间隔几个 step 取一个数
     */
    public static void fetchDataFromFile(String sourceFilePath, String targetFilePath, String residualFilePath, Long totalCount, Integer step) {
        if (Objects.isNull(step) || step <= 0) {
            throw new RuntimeException("请正确设置step");
        }
        if (Objects.isNull(totalCount) || totalCount <= 0) {
            throw new RuntimeException("请正确设置总数");
        }
        AtomicLong num = new AtomicLong(0);
        AtomicLong fetchCount = new AtomicLong(0);
        AtomicLong residualCount = new AtomicLong(0);

        try (BufferedReader sourceReader = Files.newBufferedReader(Paths.get(sourceFilePath));
             CSVParser sourceParser = CSVParser.parse(sourceReader, CSVFormat.DEFAULT);
             BufferedWriter targetWriter = new BufferedWriter(Files.newBufferedWriter(Paths.get(targetFilePath)), BUFFER_SIZE);
             CSVPrinter targetPrinter = new CSVPrinter(targetWriter, CSVFormat.DEFAULT);
             BufferedWriter residualWriter = new BufferedWriter(Files.newBufferedWriter(Paths.get(residualFilePath)), BUFFER_SIZE);
             CSVPrinter residualPrinter = new CSVPrinter(residualWriter, CSVFormat.DEFAULT)) {

            for (CSVRecord record : sourceParser) {
                // 如果拉取的数量大于等于想要拉取的数量，那么就放到保存剩余数据的文件中
                if (fetchCount.get() >= totalCount) {
                    residualCount.incrementAndGet();
                    residualPrinter.printRecord(record);
                    continue;
                }
                // 处理步长，根据步长，分别将数据放到 targetFilePath 和 residualFilePath
                if (num.incrementAndGet() % step == 0) {
                    targetPrinter.printRecord(record);
                    fetchCount.incrementAndGet();
                } else {
                    residualCount.incrementAndGet();
                    residualPrinter.printRecord(record);
                }
            }

            log.info("sourceFilePath {} 取出数据: {}", sourceFilePath, fetchCount.get());
            log.info("sourceFilePath {} 剩余数据: {}", sourceFilePath, residualCount.get());

            // Rename residual file
            String newResidualPath = residualFilePath.substring(0, residualFilePath.lastIndexOf(".")) +
                    "_剩余_" + residualCount.get() +
                    residualFilePath.substring(residualFilePath.lastIndexOf("."));
            Files.move(Paths.get(residualFilePath), Paths.get(newResidualPath));

        } catch (IOException e) {
            log.error("Error in fetchDataFromFile", e);
            throw new RuntimeException("Failed to fetch data from file", e);
        }
    }

    /**
     * 根据文件中每一行的行号，打乱每一行数据。原理就是将每一行数据放到内存中一个 List 中，然后 List 会通过随机交换不同行的方式进行打乱。
     * 至于能支持打乱多大的文件，取决于电脑的内存大小
     */
    public static void shuffleDataFileInMemory(String inputFile, String outputFile) {
        try (Reader reader = Files.newBufferedReader(Paths.get(inputFile));
             CSVParser csvParser = CSVParser.parse(reader, CSVFormat.DEFAULT);
             BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile),
                     StandardCharsets.UTF_8, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT)) {
            log.info("Start shuffle. InputPath: {} OutputPath:{}", inputFile, outputFile);
            // 将所有记录存储到列表中
            List<CSVRecord> csvRecords = new ArrayList<>();
            for (CSVRecord record : csvParser) {
                csvRecords.add(record);
            }

            log.info("Already Put all data in the list.");

            // 随机打乱列表
            Collections.shuffle(csvRecords);

            log.info("List shuffle finished.");
            // 写入输出文件
            for (CSVRecord record : csvRecords) {
                csvPrinter.printRecord(record);
            }

            log.info("Write in the output file Finished.");
        } catch (IOException e) {
            log.error("Error in shuffleTenMillionData", e);
            throw new RuntimeException("Failed to shuffle data", e);
        }

    }

    /**
     * 如果你有非常大的文件，无法一下子加载入内存，然后你想对这个文件进行随机打乱
     *
     * 举个例子，假如你有 5 亿个手机号在一个文件中，这 5 亿个手机号按照地区进行排序。现在想要对这 5 亿手机号打乱顺序
     * 但是我们无法一下子将这 5 亿手机号都加载入内存。
     * 方法：计算每个手机号的 md5 值，按照 md5 的前两位进行分组，同一分组的放到一个文件中。我们将会得到 256 个文件。最后我们将这256个文件合并，实际上我们就对这 5 亿手机号进行了打乱了。
     *
     * 其他情况可以类似的情况处理，我们要记得保存 md5与原始行的映射关系。
     * @param areaFilePath
     * @param areaFileDir
     */
    public static void shuffleVeryBigDataFileInDisk(String areaFilePath, String areaFileDir) {

        Map<String, CSVPrinter> printerMap = new HashMap<>();
        
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(areaFilePath));
             CSVParser csvParser = CSVParser.parse(reader, CSVFormat.DEFAULT)) {
            
            for (CSVRecord record : csvParser) {
                String md5 = record.get(0); // Assuming MD5 is in the first column
                // if not md5, skip
                if (md5.length() != 32) {
                    continue;
                }
                
                String key = md5.substring(0, 2);
                CSVPrinter printer = printerMap.get(key);
                
                if (printer == null) {
                    String filePath = areaFileDir + File.separator + "group" + File.separator + key + ".csv";
                    Files.createDirectories(Paths.get(areaFileDir + File.separator + "group"));
                    BufferedWriter writer = new BufferedWriter(Files.newBufferedWriter(Paths.get(filePath)), BUFFER_SIZE);
                    printer = new CSVPrinter(writer, CSVFormat.DEFAULT);
                    printerMap.put(key, printer);
                }
                
                printer.printRecord(record);
            }
            
            // Flush and close all printers
            for (CSVPrinter printer : printerMap.values()) {
                printer.close();
            }
            
        } catch (IOException e) {
            log.error("Error in shuffleVeryBigDataFileInDisk", e);
            throw new RuntimeException("Failed to shuffle data", e);
        }
    }

    private static void closeOldPrinters(Map<Long, CSVPrinter> map, long currentIndex) {
        if (currentIndex > MAX_OPEN_FILES) {
            long oldIndex = currentIndex - MAX_OPEN_FILES;
            CSVPrinter oldPrinter = map.remove(oldIndex);
            if (oldPrinter != null) {
                try {
                    oldPrinter.close();
                } catch (IOException e) {
                    log.error("Error closing old CSV printer", e);
                }
            }
        }
    }

    private static String getFileBasePath(String filePath) {
        String fileName = FilenameUtils.getName(filePath);
        if (fileName.contains(".")) {
            return filePath.substring(0, filePath.lastIndexOf("."));
        }
        return filePath;
    }

    private static void closeAllPrinters(Map<Long, CSVPrinter> map) {
        map.values().forEach(printer -> {
            try {
                printer.close();
            } catch (IOException e) {
                log.error("Error closing CSV printer", e);
            }
        });
    }
}
