package main.engine;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Properties;

public class DBConfig {

    private static String fileName = "src/main/resources/DBApp.config";
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
        update(pageMaximum,getOctreeNodeEntries());
    }

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
        update(getPageMaximum(),octreeNodeEntries);
    }

    public static void update(int maxN, int maxOctree){
        Properties properties = new Properties();
        properties.setProperty("MaximumRowsCountinTablePage",Integer.toString(maxN));
        properties.setProperty("MaximumEntriesinOctreeNode",Integer.toString(maxOctree));
        try (FileOutputStream out = new FileOutputStream(fileName)){
            properties.store(out,"Configured successfully!");
            out.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        DBConfig.setPageMaximum(5);
        DBConfig.setOctreeNodeEntries(8);
//        update(5,8);
        DBConfig.getPageMaximum();
        System.out.println(DBConfig.getOctreeNodeEntries());
    }




}
