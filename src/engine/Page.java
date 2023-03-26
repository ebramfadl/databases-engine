package engine;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable {

    private String tableName;
    private int pageNumber;
    private int n;

    private Vector<Tuple> pageTuples;

    public Page(String tableName, int pageNumber, int n) {
        this.tableName = tableName;
        this.pageNumber = pageNumber;
        this.n = n;
        pageTuples = new Vector<>(n);

    }

    public void addTuple(Hashtable<String , Object> htblColNameValue){

        Tuple tuple = new Tuple(htblColNameValue);
        pageTuples.add(tuple);
    }


    public Vector<Tuple> getPageTuples() {
        return pageTuples;
    }

    @Override
    public String toString() {
        String str = "";

        for (Tuple tuple : pageTuples){
            str = str + "-"+tuple.toString()+"\n";
        }

        return "Page number " +pageNumber+"\n"+
        "Maximum tuples "+n+"\n"+
        "Tuples" + "\n"+str+"\n";
    }
}
