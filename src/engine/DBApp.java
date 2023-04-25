package engine;

import java.io.BufferedReader;
import java.io.*;
import java.util.*;

public class DBApp {


    public  void init(){
        DBConfig.update(5,8);
    }

    public static int compareTo(String s1, String s2) {

        if (s1.matches("\\d+") && s2.matches("\\d+")) {
            if(Integer.parseInt(s1) > Integer.parseInt(s2))
                return 1;
            else if (Integer.parseInt(s1) == Integer.parseInt(s2))
                return 0;
            else
                return -1;
        }
        else if (s1.matches("[a-zA-Z]+") && s2.matches("[a-zA-Z]+")) {
            return s1.compareTo(s2);
        }
        else if(s1.indexOf(".") != -1 && s2.indexOf(".") != -1) {
            if(Double.parseDouble(s1) > Double.parseDouble(s2))
                return 1;
            else if (Double.parseDouble(s1) == Double.parseDouble(s2))
                return 0;
            else
                return -1;
        }

        System.out.println("Comparison failed between "+s1+" and "+s2);
        return -5;
    }

    public static String getPrimaryKey(String tableName) throws IOException {

        String line = "";
        String path = "metadata.csv";

       BufferedReader br = new BufferedReader(new FileReader(path));
       while ( (line = br.readLine()) != null ){
           String[] fields = line.split(",");
           if(fields[0].equals(tableName) && fields[3].equals("true"))
               return fields[1];
       }
       br.close();
       return null;
    }

    public static void sortPage(Page page,String toSortBy) {

        Comparator<Tuple> compareById = new Comparator<Tuple>() {
            @Override
            public int compare(Tuple t1, Tuple t2) {
                String pk1 = t1.getHtblColNameValue().get(toSortBy).toString();
                String pk2 = t2.getHtblColNameValue().get(toSortBy).toString();
                return compareTo(pk1, pk2);
            }
        };
        // Sort the vector by primary key
        Collections.sort(page.getPageTuples(), compareById);

    }

    public static String displayTablePages(String tableName) throws ClassNotFoundException, IOException {

        String str = "";
        Table table = deserializeTable(tableName);

        for(int i = 1 ; i<= table.getNumberOfPages() ; i++){
            Page page = deserializePage(tableName+i+".bin");
            str = str + page.toString()+"\n";
        }
        return  str;
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

//        if(allMetadata.size() != htblColNameValue.size())
//            throw new DBAppException("Missing a field or extra field specified!");

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


    public static boolean serializePage(String path, Page page){
            //write ===> FileOutputStream
            //Read ===> FileInputStream
        try {
            FileOutputStream fileOut = new FileOutputStream(path);
            ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
            objectOut.writeObject(page);
            //System.out.println("page updated " + path + " successfully!");
            objectOut.close();
            fileOut.close();
            return true;
        }
        catch (IOException e){
            e.printStackTrace();
        }
        return false;
    }

    public static Page deserializePage(String path) throws ClassNotFoundException, IOException{
            FileInputStream fileInputStream = new FileInputStream(path);
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            Page page = (Page) objectInputStream.readObject();

            objectInputStream.close();
            fileInputStream.close();
            return page;

    }

    public static boolean serializeTable(Table table)  {
        try(
            FileOutputStream fileOut = new FileOutputStream("tables.class");
            ObjectOutputStream objectOut = new ObjectOutputStream(fileOut)) {
            objectOut.writeObject(table);
            //System.out.println("Table stored successfully!" + "\n" + table.toString());
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

        Table table = new Table(strTableName,DBConfig.getPageMaximum());
        serializeTable(table);

    }


    // following method inserts one row only.
    // htblColNameValue must include a value for the primary key
    public static void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {
        //check id does not exist
        if(checkTableExists(strTableName) == false)
            throw new DBAppException("Table "+strTableName+" does not exist, Please create it first");

        validateDataTypes(strTableName,htblColNameValue);

        Table table = deserializeTable(strTableName);

        String pk = getPrimaryKey(strTableName); //"id"

        if(table.getNumberOfPages() == 0){
            Page page = new Page(1,DBConfig.getPageMaximum(),strTableName);
            page.addTuple(htblColNameValue);
            String path = strTableName+"1"+".bin";
            table.setNumberOfPages(1);
            serializeTable(table);
            serializePage(path,page);
            return;
        }


        for(int i = 1 ; i<= table.getNumberOfPages() ; i++){
            Page currentPage = deserializePage(table.getTableName()+i+".bin");
            Vector<Tuple> pageVector = currentPage.getPageTuples();
            String pageMax = pageVector.lastElement().getHtblColNameValue().get(pk).toString();
            String pageMin = pageVector.firstElement().getHtblColNameValue().get(pk).toString();
            String key = htblColNameValue.get(pk).toString();



            if(table.getNumberOfPages() == i && compareTo(key, pageMax) > 0 && pageVector.size() == pageVector.capacity()) {

                int pageNumber = table.getNumberOfPages()+1;
                Page newPage = new Page(pageNumber, table.getN(), strTableName);
                newPage.addTuple(htblColNameValue);
                table.setNumberOfPages(table.getNumberOfPages()+1);
                serializeTable(table);
                serializePage(table.getTableName()+newPage.getPageNumber()+".bin", newPage);
                return;

            }
            else if(pageVector.size() < pageVector.capacity()) {

                if(i == table.getNumberOfPages()) {
                    currentPage.addTuple(htblColNameValue);
                    sortPage(currentPage, pk);
                    serializePage(table.getTableName()+i+".bin", currentPage);
                    return;
                }
                else {
                    int nextPageNum = i+1;
                    Page nextPage = deserializePage(table.getTableName()+nextPageNum+".bin");
                    if(compareTo(key,nextPage.getPageTuples().firstElement().getHtblColNameValue().get(pk).toString()) < 0) {
                        currentPage.addTuple(htblColNameValue);
                        sortPage(currentPage, pk);
                        serializePage(table.getTableName()+i+".bin", currentPage);
                        return;
                    }
                }

            }
            else if(pageVector.size() == pageVector.capacity() && compareTo(key, pageMax) < 0 ){

                Tuple popedTuple = (Tuple)pageVector.remove(pageVector.size()-1);
                currentPage.addTuple(htblColNameValue);
                sortPage(currentPage, pk);
                serializePage(table.getTableName()+i+".bin", currentPage);
                insertIntoTable(strTableName, popedTuple.getHtblColNameValue());
                return;
            }


        }

    }
    public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {
        if(!checkTableExists(strTableName)){
            throw new DBAppException("Table " +strTableName+ " does not exist.");
        }
        int pageNumber = getPageByBinarySearch(strTableName, strClusteringKeyValue);
        Page page = deserializePage(strTableName + pageNumber + ".bin");
        int tupleIndex = getTupleByBinarySearch(strTableName, strClusteringKeyValue);
        Tuple tuple = page.getPageTuples().get(tupleIndex);


        for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
            if(!tuple.getHtblColNameValue().containsKey(entry.getKey())){
                throw new DBAppException("Field " +entry.getKey()+  " does not exist.");
            }

            else if(!tuple.getHtblColNameValue().get(entry.getKey()).getClass().getName().equals(entry.getValue().getClass().getName())){
                throw new DBAppException("Cannot update column " +entry.getKey()+ " to be " + entry.getValue());
            }
            else {
                tuple.getHtblColNameValue().put(entry.getKey(),entry.getValue());
            }
        }
        serializePage(strTableName + pageNumber + ".bin",page);
    }
    public static int getTupleByBinarySearch(String strTableName, String value) throws ClassNotFoundException, IOException, DBAppException {
        int x = getPageByBinarySearch(strTableName,value);
        Page page = deserializePage(strTableName + x + ".bin");
        String pk = getPrimaryKey(strTableName);    //"ID"
        int low = 0;
        int high = page.getPageTuples().size() - 1;
        while(low <= high){
            int mid = (low + high)/2;
            String key = page.getPageTuples().get(mid).getHtblColNameValue().get(pk).toString();
            if(compareTo(key,value) == 0){
                return mid;
            }
            else if(compareTo(key,value) == -1){
                low = mid + 1;
            }
            else{
                high = mid - 1;
            }

        }
        throw new DBAppException("Record with primary key " + value + " is not found");
    }
    public static int getPageByBinarySearch(String strTableName, String value) throws IOException, ClassNotFoundException, DBAppException {
        Table table = deserializeTable(strTableName);
        int low = 1;
        int high = table.getNumberOfPages();
        String pk = getPrimaryKey(strTableName);
        while(low <= high){
            int mid = (low + high)/2;
            Page page = deserializePage(strTableName + mid + ".bin");
            String min = page.getPageTuples().firstElement().getHtblColNameValue().get(pk).toString();
            String max = page.getPageTuples().lastElement().getHtblColNameValue().get(pk).toString();

            if((compareTo(value, min) >= 0) && (compareTo(value,max) <= 0)){
                return mid;
            }
            else if(compareTo(value,max) == 1){
                low = mid + 1;
            }
            else{
                high = mid - 1;
            }

        }
        throw new DBAppException("Record with primary key " + value + " is not found");
    }

    public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException, ClassNotFoundException, IOException {
        if(!checkTableExists(strTableName)){
            throw new DBAppException("Table " +strTableName+ " does not exist.");
        }
        Table table = deserializeTable(strTableName);
        for (int i = 1; i <= table.getNumberOfPages(); i++) {
            Page page = deserializePage(strTableName +i+ ".bin");
            Vector<Tuple> pageVector = page.getPageTuples();
            Iterator<Tuple> tupleItearator = pageVector.iterator();
            while (tupleItearator.hasNext()) {
                Tuple tuple = tupleItearator.next();
                Boolean flag=true;
                for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
                    if(!tuple.getHtblColNameValue().containsKey(entry.getKey())){
                        throw new DBAppException("Field " +entry.getKey()+  " does not exist.");
                    }
                    else if(!tuple.getHtblColNameValue().get(entry.getKey()).getClass().getName().equals(entry.getValue().getClass().getName())){
                        throw new DBAppException("Invalid data type.");
                    }
                    else if(!tuple.getHtblColNameValue().get(entry.getKey()).equals(entry.getValue())){
                        flag = false;
                    }

                }
                if(flag==true){
                    tupleItearator.remove();
                }
            }
            serializePage(strTableName +i+ ".bin",page);
            if(page.getPageTuples().isEmpty()){
                File file = new File(strTableName +i+".bin");
                file.delete();
                updatePagesNumber(strTableName,i+1);
                table.setNumberOfPages(table.getNumberOfPages()-1);
                serializeTable(table);
            }
        }
        serializeTable(table);

    }

    public void updatePagesNumber(String tablename, int currentpage) throws ClassNotFoundException, IOException {
        Page page;
        try{
            page = deserializePage(tablename + currentpage + ".bin");
        }
        catch(IOException e){
            return;
        }
        int newPageNumber = currentpage - 1;
        File oldFile = new File(tablename + currentpage + ".bin");
        File newFile = new File(tablename + newPageNumber + ".bin");
        oldFile.renameTo(newFile);
        page.setPageNumber(newPageNumber);
        serializePage(tablename + newPageNumber + ".bin",page);
        updatePagesNumber(tablename,currentpage+1);
    }



    public static void main(String[] args) throws DBAppException, IOException, ClassNotFoundException {
        DBApp dbApp = new DBApp();
//        Table table = new Table("Student",5);
//        serializeTable(table);
//        System.out.println(deserializeTable("Student").toString());

//        Hashtable htblColNameValue = new Hashtable( );
//        htblColNameValue.put("id", new Integer( 18 ));
//        htblColNameValue.put("name", new String("Ashry" ) );
//        htblColNameValue.put("gpa", new Double( 0.7 ) );
//        insertIntoTable( "Student" , htblColNameValue );



//        Table table = deserializeTable("Student");
//        table.setNumberOfPages(0);
//        serializeTable(table);

//       System.out.println(displayTablePages("Student"));
//
//       Hashtable htblColNameValue = new Hashtable( );
//        htblColNameValue.put("id", new Integer( 60 ));
//        htblColNameValue.put("name", new String("Toni Kroos" ) );
//        htblColNameValue.put("gpa", new Double( 2.1 ) );
//        insertIntoTable( "Student" , htblColNameValue );
//
//        htblColNameValue = new Hashtable( );
//        htblColNameValue.put("id", new Integer( 65 ));
//        htblColNameValue.put("name", new String("Nacho Fernandez" ) );
//        htblColNameValue.put("gpa", new Double( 2.1 ) );
//        insertIntoTable( "Student" , htblColNameValue );
////
//        htblColNameValue = new Hashtable( );
//        htblColNameValue.put("id", new Integer( 70 ));
//        htblColNameValue.put("name", new String("Busquets" ) );
//        htblColNameValue.put("gpa", new Double( 1.5 ) );
//        insertIntoTable( "Student" , htblColNameValue );
//
//        htblColNameValue = new Hashtable( );
//        htblColNameValue.put("id", new Integer( 75 ));
//        htblColNameValue.put("name", new String("Sergio Ramos" ) );
//        htblColNameValue.put("gpa", new Double( 3.2 ) );
//        insertIntoTable( "Student" , htblColNameValue );

        Hashtable htblColNameValue = new Hashtable();
        htblColNameValue.put("gpa", "Ebram");

        dbApp.deleteFromTable( "Arwa" , htblColNameValue );


        String displayTables = displayTablePages("Student");
        System.out.println(displayTables);

//        Table table = deserializeTable("Student");
//        table.setNumberOfPages(1);
//        serializeTable(table);







//       int x = getTupleByBinarySearch("Student","55");
//       System.out.println(x);


//        Hashtable table = new Hashtable();
//        table.put("name","Ebram");
//        table.put("gpa", 0.3);
//        dbApp.updateTable("Arwa","45", table);
//        System.out.println(displayTablePages("Student"));

//
//		Hashtable htblColNameValue = new Hashtable( );
//		htblColNameValue.put("id", new Integer( 21 ));
//		htblColNameValue.put("name", new String("Arwa" ) );
//		htblColNameValue.put("gpa", new Double( 0.7 ) );
//		insertIntoTable( "Student" , htblColNameValue );

//		htblColNameValue = new Hashtable( );
//		htblColNameValue.put("id", new Integer( 25 ));
//		htblColNameValue.put("name", new String("Maya" ) );
//		htblColNameValue.put("gpa", new Double( 0.7 ) );
//		insertIntoTable( "Student" , htblColNameValue );
//
//		htblColNameValue = new Hashtable( );
//		htblColNameValue.put("id", new Integer( 30 ));
//		htblColNameValue.put("name", new String("Jana" ) );
//		htblColNameValue.put("gpa", new Double( 0.7 ) );
//		insertIntoTable( "Student" , htblColNameValue );
//
//		htblColNameValue = new Hashtable( );
//		htblColNameValue.put("id", new Integer( 35 ));
//		htblColNameValue.put("name", new String("Nour" ) );
//		htblColNameValue.put("gpa", new Double( 0.7 ) );
//		insertIntoTable( "Student" , htblColNameValue );


//
//		htblColNameValue = new Hashtable( );
//		htblColNameValue.put("id", new Integer( 40 ));
//		htblColNameValue.put("name", new String("Sergio Ramos" ) );
//		htblColNameValue.put("gpa", new Double( 0.7 ) );
//		insertIntoTable( "Student" , htblColNameValue );
//
//		htblColNameValue = new Hashtable( );
//		htblColNameValue.put("id", new Integer( 41 ));
//		htblColNameValue.put("name", new String("Gerard Pique" ) );
//		htblColNameValue.put("gpa", new Double( 0.7 ) );
//		insertIntoTable( "Student" , htblColNameValue );
//
//		htblColNameValue = new Hashtable( );
//		htblColNameValue.put("id", new Integer( 45 ));
//		htblColNameValue.put("name", new String("Isco Alarcon" ) );
//		htblColNameValue.put("gpa", new Double( 0.7 ) );
//		insertIntoTable( "Student" , htblColNameValue );
//
//		htblColNameValue = new Hashtable( );
//		htblColNameValue.put("id", new Integer( 50 ));
//		htblColNameValue.put("name", new String("Marcello Viera" ) );
//		htblColNameValue.put("gpa", new Double( 0.7 ) );
//		insertIntoTable( "Student" , htblColNameValue );
//
//		htblColNameValue = new Hashtable( );
//		htblColNameValue.put("id", new Integer( 55 ));
//		htblColNameValue.put("name", new String("Daniel Carvajal" ) );
//		htblColNameValue.put("gpa", new Double( 0.7 ) );
//		insertIntoTable( "Student" , htblColNameValue );
    }


}
