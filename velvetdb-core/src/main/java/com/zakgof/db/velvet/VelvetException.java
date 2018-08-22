package com.zakgof.db.velvet;

/**
 * Velvetdb exception.
 */
@SuppressWarnings("serial")
public class VelvetException extends RuntimeException {

    public VelvetException(Throwable e) {
        super(e);
    }

    public VelvetException(String message) {
        super(message);
    }
}
