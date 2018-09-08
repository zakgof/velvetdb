package com.zakgof.db.velvet;

/**
 * Velvetdb exception.
 */
@SuppressWarnings("serial")
public class VelvetException extends RuntimeException {

    /**
     * Exception wrapping a downstream exception.
     * @param e downstream exception
     */
    public VelvetException(Throwable e) {
        super(e);
    }

    /**
     * Exception with a message.
     * @param message message
     */
    public VelvetException(String message) {
        super(message);
    }
}
