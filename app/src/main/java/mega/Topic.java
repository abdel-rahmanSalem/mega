package mega;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Topic {
    private final String name;
    private final Queue<Message> messages;
    private AtomicInteger currentOffset;
    private static final int MAX_MESSAGES = 1_000_000;

    public Topic(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Topic name cannot be null");
        }
        this.name = name;
        this.messages = new ConcurrentLinkedQueue<>();
        this.currentOffset = new AtomicInteger(0);
    }

    public int produce(Message message) {
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }

        if (currentOffset.get() >= MAX_MESSAGES) {
            throw new IllegalStateException("Topic has reached maximum capacity");
        }
        if (!messages.offer(message)) {
            throw new IllegalStateException("Failed to add message to topic");
        }
        return currentOffset.getAndIncrement();
    }

    public Message consume(int offset) {
        if (offset < 0 || offset >= currentOffset.get()) {
            return null;
        }

        return messages.stream()
                .skip(offset)
                .findFirst()
                .orElse(null);

    }

    public String getName() {
        return name;
    }

    public int getCurrentOffset() {
        return currentOffset.get();
    }
}
