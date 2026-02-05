package com.balsajraft.transport;

import java.io.IOException;
import java.util.Random;
import java.util.logging.Logger;

/**
 * A Transport decorator that simulates network unreliability by dropping outgoing messages.
 */
public class UnreliableTransport implements Transport {
    private static final Logger logger = Logger.getLogger(UnreliableTransport.class.getName());

    private final Transport delegate;
    private final Random random = new Random();
    private volatile double dropRate = 0.0;

    public UnreliableTransport(Transport delegate) {
        this.delegate = delegate;
    }

    /**
     * Sets the probability of dropping a message (0.0 to 1.0).
     * @param dropRate The probability that any given send() call will be ignored.
     */
    public void setDropRate(double dropRate) {
        if (dropRate < 0.0 || dropRate > 1.0) {
            throw new IllegalArgumentException("Drop rate must be between 0.0 and 1.0");
        }
        this.dropRate = dropRate;
    }

    @Override
    public void start(int port) throws IOException {
        delegate.start(port);
    }

    @Override
    public void send(Object message, String destinationId) {
        if (dropRate > 0 && random.nextDouble() < dropRate) {
            // Drop message
            return;
        }
        delegate.send(message, destinationId);
    }

    @Override
    public void registerHandler(TransportHandler handler) {
        delegate.registerHandler(handler);
    }

    @Override
    public void stop() throws IOException {
        delegate.stop();
    }
}
