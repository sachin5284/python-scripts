import java.io.FileInputStream;
import java.io.FileOutputStream;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFSheet;

public class ReadExcelFile {

    public static void main(String[] args) {
        // Path to the existing Excel file
        String excelFilePath = "path/to/your/file.xlsx";

        try (FileInputStream fis = new FileInputStream(excelFilePath)) {
            // Create a Workbook from the input stream
            XSSFWorkbook workbook = new XSSFWorkbook(fis);

            // Get the desired sheet from the workbook
            XSSFSheet instructionSheet = (XSSFSheet) workbook.getSheet(FileConstants.INSTRUCTIONS_SHEET_NAME);

            // Check if the sheet is not null
            if (instructionSheet != null) {
                // Iterate over rows and cells to read data
                for (Row row : instructionSheet) {
                    for (Cell cell : row) {
                        switch (cell.getCellType()) {
                            case STRING:
                                System.out.print(cell.getStringCellValue() + "\t");
                                break;
                            case NUMERIC:
                                if (DateUtil.isCellDateFormatted(cell)) {
                                    System.out.print(cell.getDateCellValue() + "\t");
                                } else {
                                    System.out.print(cell.getNumericCellValue() + "\t");
                                }
                                break;
                            case BOOLEAN:
                                System.out.print(cell.getBooleanCellValue() + "\t");
                                break;
                            case FORMULA:
                                System.out.print(cell.getCellFormula() + "\t");
                                break;
                            default:
                                System.out.print("Unknown Cell Type\t");
                        }
                    }
                    System.out.println();
                }
            }

            // Don't forget to close the workbook
            workbook.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
