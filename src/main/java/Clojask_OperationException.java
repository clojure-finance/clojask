package com.clojask.exception;

import java.lang.RuntimeException;

public class Clojask_OperationException extends RuntimeException {

    public Clojask_OperationException(String s) {
        super("Failed in running operation: " + s);
    }

}