package engine;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map;

public class Tuple implements Serializable {

    private Hashtable<String,Object> htblColNameValue;

    public Tuple( Hashtable<String, Object> htblColNameValue ) {

        this.htblColNameValue = new Hashtable<String,Object>();

        for (Map.Entry<String,Object> entry : htblColNameValue.entrySet()){
            this.htblColNameValue.put(entry.getKey(),entry.getValue());
        }
    }

    @Override
    public String toString() {
       String str = "";

        for (Map.Entry<String,Object> entry : this.htblColNameValue.entrySet()){
            str = str + entry.getKey()+" : "+entry.getValue();
        }

        return  str;
    }

    public Hashtable<String, Object> getHtblColNameValue() {
        return htblColNameValue;
    }
}
