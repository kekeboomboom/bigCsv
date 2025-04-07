package org.keboom;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
/**
 * @author: keboom
 * @date: 2025/4/7
 */
@Slf4j
public class CsvDataUtil {

    private static final int BUFFER_SIZE = 8192; // 8KB buffer
    private static final int MAX_OPEN_FILES = 3;

    /**
     * 文件分割，按照每个文件的行数进行分割
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

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(sourceFilePath));
             CSVParser csvParser = CSVParser.parse(reader, CSVFormat.DEFAULT)) {
            
            for (CSVRecord csvRecord : csvParser) {
                long fileIndex = size.get() / fileSize;
                CSVPrinter printer = map.get(fileIndex);
                
                if (printer == null) {
                    // Close old printers to prevent too many open files
                    closeOldPrinters(map, fileIndex);
                    
                    String filePath = String.format("%s-%d.%s", targetFilePath, fileIndex, fileFormat);
                    log.info("Processed {} records, creating file: {}", size.get(), filePath);
                    moreFiles.add(filePath);
                    
                    BufferedWriter writer = new BufferedWriter(
                            Files.newBufferedWriter(Paths.get(filePath)), BUFFER_SIZE);
                    printer = new CSVPrinter(writer, CSVFormat.DEFAULT);
                    map.put(fileIndex, printer);
                }
                
                size.incrementAndGet();
                sum.incrementAndGet();
                printer.printRecord(csvRecord.get(0), csvRecord.get(1), csvRecord.get(2));
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
     * @param sourceFilePathList 源文件列表
     * @param targetFilePath 目标文件路径
     */
    public static void moreToOne(List<String> sourceFilePathList, String targetFilePath) {
        AtomicLong sum = new AtomicLong(0);
        
        try (BufferedWriter writer = new BufferedWriter(
                Files.newBufferedWriter(Paths.get(targetFilePath)), BUFFER_SIZE);
             CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT)) {
            
            for (String path : sourceFilePathList) {
                try (BufferedReader reader = Files.newBufferedReader(Paths.get(path));
                     CSVParser parser = CSVParser.parse(reader, CSVFormat.DEFAULT)) {
                    
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
