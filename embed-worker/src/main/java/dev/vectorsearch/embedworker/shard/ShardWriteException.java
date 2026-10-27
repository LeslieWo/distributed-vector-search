package dev.vectorsearch.embedworker.shard;

/**
 * Retriable = the target is down/slow/overloaded, the same request may succeed later.
 * Not retriable = the request itself is bad (e.g. INVALID_ARGUMENT), resending it cannot help.
 */
public class ShardWriteException extends RuntimeException {
    private final boolean retriable;

    public ShardWriteException(String message, boolean retriable, Throwable cause) {
        super(message, cause);
        this.retriable = retriable;
    }

    public boolean retriable() {
        return retriable;
    }
}
