package com.clojask.exception;

import java.lang.Exception;

public class ExecutionException extends Exception {

    public ExecutionException(String s) {
        super("Execution Error: " + s);
    }

    public ExecutionException(String s, Throwable err) {
        super("Execution Error: " + s, err);
    }

    // @Override
    // public String toString() {
    //     return this.getMessage() + "\n" + super.toString();
    // }

}