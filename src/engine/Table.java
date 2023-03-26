package engine;

import java.io.Serializable;

public class Table implements Serializable {

    private  String tableName;
    private int numberOfPAges;
    private int n;


    public Table(String tableName,int n) {
        this.tableName = tableName;
        this.numberOfPAges = 0;
        this.n=n;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getNumberOfPAges() {
        return numberOfPAges;
    }

    public void setNumberOfPAges(int numberOfPAges) {
        this.numberOfPAges = numberOfPAges;
    }

    public int getN() {
        return n;
    }

    @Override
    public String toString() {
        return "Table name :"+tableName+", Number Of pages :"+numberOfPAges+" , N : "+n;
    }

}
