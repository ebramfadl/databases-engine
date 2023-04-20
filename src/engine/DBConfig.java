package engine;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Properties;

public class DBConfig {

    private static String fileName = "resources/DBApp.config";
    private static int pageMaximum;
    private static int octreeNodeEntries;

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public static int getPageMaximum() {
        Properties properties = new Properties();
        try (FileInputStream in = new FileInputStream(fileName)){
            properties.load(in);
            pageMaximum = Integer.parseInt(properties.getProperty("MaximumRowsCountinTablePage"));
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return pageMaximum;
    }

    public static void setPageMaximum(int pageMaximum) {
        DBConfig.pageMaximum = pageMaximum;
        Properties properties = new Properties();
        properties.setProperty("MaximumRowsCountinTablePage",Integer.toString((pageMaximum)));
        try (FileOutputStream out = new FileOutputStream(fileName)){
            properties.store(out,"Configured successfully!");
        }
        catch (Exception e){
            e.printStackTrace();
        }    }

    public static int getOctreeNodeEntries() {
        Properties properties = new Properties();
        try (FileInputStream in = new FileInputStream(fileName)){
            properties.load(in);
            octreeNodeEntries = Integer.parseInt(properties.getProperty("MaximumEntriesinOctreeNode"));
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return octreeNodeEntries;
    }

    public static void setOctreeNodeEntries(int octreeNodeEntries) {
        DBConfig.octreeNodeEntries = octreeNodeEntries;
        Properties properties = new Properties();
        properties.setProperty("MaximumEntriesinOctreeNode",Integer.toString(octreeNodeEntries));
        try (FileOutputStream out = new FileOutputStream(fileName)){
            properties.store(out,"Configured successfully!");
        }
        catch (Exception e){
            e.printStackTrace();
        }    }



    public static void main(String[] args) {
//        DBConfig.setPageMaximum(5);
//        DBConfig.setOctreeNodeEntries(10);

        System.out.println(DBConfig.getPageMaximum());
        System.out.println(DBConfig.getOctreeNodeEntries());
    }




}
