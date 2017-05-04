package cn.edu.thu.tsfile.avro.common;

public class FieldNotFoundException extends Exception {

    private static final long serialVersionUID = -9025706991954480694L;

    public FieldNotFoundException(String message) {
        super(message);
    }
}

