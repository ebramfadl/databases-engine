package main.engine;

import java.io.IOException;

public class DBAppException extends  Exception {

    public DBAppException() {
    }

    public DBAppException(String message) {
        super(message);
    }

    public DBAppException(Exception e) {
        e.printStackTrace();
    }
}
