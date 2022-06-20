package com.clojask.exception;

import java.lang.RuntimeException;

public class OperationException extends Exception {

    public OperationException(String s) {
        super("Failed in running operation: " + s);
    }

    public OperationException(String s, Throwable err) {
        super("Failed in running operation: " + s, err);
        // super.fillInStackTrace();
    }

    // @Override
    // public String toString() {
    //     return this.getMessage() + "\n" + super.toString();
    // }
}