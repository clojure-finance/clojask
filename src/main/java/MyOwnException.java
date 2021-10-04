package com.stackoverflow.clojure;

import java.lang.RuntimeException;

public class MyOwnException extends RuntimeException {

    private static final long serialVersionUID = 3020795659981708312L;

    public MyOwnException(String s) {
        super("My own exception says: " + s);
    }

}