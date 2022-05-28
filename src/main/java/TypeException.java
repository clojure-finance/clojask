package com.clojask.exception;

import java.lang.RuntimeException;

public class TypeException extends RuntimeException {

    public TypeException(String s) {
        super("Type assertion error: " + s);
    }

    public TypeException(String s, Throwable err) {
        super("Type assertion error: " + s, err);
    }

    // @Override
    // public String toString() {
    //     return this.getMessage() + "\n" + super.toString();
    // }
}