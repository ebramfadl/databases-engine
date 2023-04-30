package main.engine;

import java.io.Serializable;

public class Table implements Serializable {

    private  String tableName;
    private int numberOfPages;
    private int n;


    public Table(String tableName,int n) {
        this.tableName = tableName;
        this.numberOfPages = 0;
        this.n=n;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getNumberOfPages() {
        return numberOfPages;
    }

    public void setNumberOfPages(int numberOfPAges) {
        this.numberOfPages = numberOfPAges;
    }

    public int getN() {
        return n;
    }

    @Override
    public String toString() {
        return "Table name :"+tableName+", Number Of pages :"+numberOfPages+" , N : "+n;
    }

}
