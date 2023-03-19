package engine;

import java.io.BufferedReader;
import java.io.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;

public class DBApp {


    public  void init(){

    }

    public static boolean validateDataTypes(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {
        String line = "";
        String path = "metadata.csv";
        ArrayList<String[]> allMetadata = new ArrayList<String[]>();

        try (BufferedReader br = new BufferedReader(new FileReader(path)))
        {
            while((line = br.readLine()) !=null ){
                String[] fields = line.split(",");
                if(fields[0].equals(strTableName))
                    allMetadata.add(fields);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if(allMetadata.size() != htblColNameValue.size())
            throw new DBAppException("Missing a field or extra field specified!");

        for (Map.Entry<String,Object> entry : htblColNameValue.entrySet()) {

            String colName = entry.getKey();
            Object colValue = entry.getValue();

            for (String[] strings : allMetadata){
                if(strings[1].equals(colName)){
                    if(!((colValue.getClass().getName().toString().equals(strings[2]))))
                        throw new DBAppException("Invalid column datatype! "+colValue.getClass().getName().toString());
                }
            }
        }

        return true;

        }


    public static boolean serializeTable(Table table)  {
        try(
            FileOutputStream fileOut = new FileOutputStream("tables.class");
            ObjectOutputStream objectOut = new ObjectOutputStream(fileOut)) {
            objectOut.writeObject(table);
            System.out.println("Table stored successfully!" + "\n" + table.toString());
            objectOut.close();
            fileOut.close();
            return true;
        }
        catch (IOException e){
            e.printStackTrace();
        }
        return false;
    }

    public static Table deserializeTable(String tableName){

        String path = "tables.class";
        try {
            FileInputStream fileInputStram = new FileInputStream(path);
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStram);
            while (true){
                try {
                    Table table = (Table)objectInputStream.readObject();
                    if(table.getTableName().equals(tableName))
                        return table;
                }
                catch (EOFException e){
                    break;
                }
            }
            fileInputStram.close();
            objectInputStream.close();
        }
        catch (IOException | ClassNotFoundException e){
            e.printStackTrace();
        }
        return null;
    }


    public static boolean checkTableExists(String tableName) throws FileNotFoundException,IOException {
        String line = "";
        String path = "metadata.csv";
        BufferedReader br = new BufferedReader(new FileReader(path));
        while((line = br.readLine())!=null) {
            String[] fields = line.split(",");
            if (fields[0].equals(tableName))
                return true;
        }
        br.close();
        return false;
    }




    // following method creates one table only
    // strClusteringKeyColumn is the name of the column that will be the primary
    // key and the clustering column as well. The data type of that column will
    // be passed in htblColNameType
    // htblColNameValue will have the column name as key and the data
    // type as value
    // htblColNameMin and htblColNameMax for passing minimum and maximum values
    // for data in the column. Key is the name of the column
    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType, Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax ) throws DBAppException, IOException {

        if(checkTableExists(strTableName))
            throw  new DBAppException("Table "+strTableName+" already exists!");

        String path = "metadata.csv";

        for (Map.Entry<String,String> entry : htblColNameType.entrySet()){
            if( !( entry.getValue().equals("java.lang.String") || entry.getValue().equals("java.lang.Integer") ||  entry.getValue().equals("java.lang.Date") || entry.getValue().equals("java.lang.Double")) )
                throw new DBAppException("Invalid column datatype "+entry.getValue()+" you can only use 'java.lang.Integer/Double/Date/String' ");
        }

        for (Map.Entry<String,String> entry : htblColNameType.entrySet()){
            String isClustering = "false";
            if(entry.getKey().equals(strClusteringKeyColumn))
                isClustering="true";
            String[] columns= {strTableName,entry.getKey(),entry.getValue(),isClustering,null,null,htblColNameMin.get(entry.getKey()),htblColNameMax.get(entry.getKey())};

            BufferedWriter br = new BufferedWriter( new FileWriter(path,true));
            br.write(String.join(",",columns));
            br.newLine();
            br.close();
            System.out.println("Data inserted successfully into metadata.csv");
        }

        Table table = new Table(strTableName,5);
        serializeTable(table);

    }


    // following method inserts one row only.
    // htblColNameValue must include a value for the primary key
    public static void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException {

        if(checkTableExists(strTableName) == false)
            throw new DBAppException("Table "+strTableName+" does not exist, Please create it first");

        validateDataTypes(strTableName,htblColNameValue);


    }

    public static void main(String[] args) throws DBAppException, IOException {
//        Table table = new Table("Student",5);
//        serializeTable(table);
//        System.out.println(deserializeTable("Student").toString());

//        Hashtable htblColNameValue = new Hashtable( );
//        htblColNameValue.put("id", new Integer( 2343432 ));
//        htblColNameValue.put("name", new String("Ahmed Noor" ) );
//        htblColNameValue.put("gpa", new Double( 1 ) );
//        insertIntoTable( "Student" , htblColNameValue );

    }


}
