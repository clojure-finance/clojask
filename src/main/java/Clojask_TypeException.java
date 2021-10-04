package com.clojask.exception;

import java.lang.RuntimeException;

public class Clojask_TypeException extends RuntimeException {

    public Clojask_TypeException(String s) {
        super("Type assertion error: " + s);
    }

}