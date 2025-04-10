package org.keboom;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.metadata.data.ReadCellData;
import com.alibaba.excel.read.listener.ReadListener;
import com.alibaba.excel.write.metadata.WriteSheet;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.compress.utils.Lists;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@code @author:} keboom
 * {@code @date:} 2025/4/10
 */
public class BigExcelUtil {

    // Split data into chunks of 800,000 rows per sheet to avoid Excel row limits
    private static final int maxRowsPerSheet = 80_0000;

    /**
     * 合并 excel 文件，每个文件的头部是要保持一致的
     *
     */
    public static void mergeExcelFile(List<File> fileList, String mergePath) {
        List<List<String>> dataList = new ArrayList<>();
        List<List<String>> header = new ArrayList<>();

        // read all data to the memory.
        for (File file : fileList) {
            // 这里 只要，然后读取第一个sheet 同步读取会自动finish
            EasyExcel.read(file, new ReadListener<Map<Integer, String>>() {
                @Override
                public void invoke(Map<Integer, String> data, AnalysisContext context) {
                    List<String> row = Lists.newArrayList();
                    for (int i = 0; i < data.size(); i++) {
                        String s = data.get(i);
                        row.add(s);
                    }

                    dataList.add(row);
                }

                @Override
                public void invokeHead(Map<Integer, ReadCellData<?>> headMap, AnalysisContext context) {
                    if (CollectionUtils.isEmpty(header)) {
                        for (int i = 0; i < headMap.size(); i++) {
                            ReadCellData<?> readCellData = headMap.get(i);

                            String stringValue = readCellData.getStringValue();

                            List<String> head = Lists.newArrayList();
                            head.add(stringValue);

                            header.add(head);
                        }
                    }

                }

                @Override
                public void doAfterAllAnalysed(AnalysisContext context) {
                    // Nothing to do
                }
            }).sheet().doRead();
        }

        // write all data to one Excel file
        try (ExcelWriter excelWriter = EasyExcel.write(mergePath).head(header).build()) {

            int totalSheets = (int) Math.ceil((double) dataList.size() / maxRowsPerSheet);

            for (int sheetNo = 0; sheetNo < totalSheets; sheetNo++) {
                // Create a new sheet for each batch of data
                WriteSheet writeSheet = EasyExcel.writerSheet("sheet" + (sheetNo + 1)).build();

                // Calculate the start and end indices for this sheet
                int startIndex = sheetNo * maxRowsPerSheet;
                int endIndex = Math.min((sheetNo + 1) * maxRowsPerSheet, dataList.size());

                // Write only a subset of data to this sheet
                List<List<String>> sheetData = dataList.subList(startIndex, endIndex);
                excelWriter.write(sheetData, writeSheet);
            }
        }

    }
}
