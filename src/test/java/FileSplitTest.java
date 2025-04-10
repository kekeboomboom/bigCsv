import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.junit.Test;

import java.io.File;

/**
 * @author: keboom
 * @date: 2025/4/7
 */
@Slf4j
public class FileSplitTest {

    @Test
    public void test1() {
        String sourceFile = "/Users/keboom/Downloads/sourceFile.csv";

        System.out.println(getFileBasePath(sourceFile));


    }


    private static String getFileBasePath(String filePath) {
        String fileName = FilenameUtils.getName(filePath);
        if (fileName.contains(".")) {
            return filePath.substring(0, filePath.lastIndexOf("."));
        }
        return filePath;
    }




}
