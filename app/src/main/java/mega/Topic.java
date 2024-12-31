package mega;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Topic {
    private final String name;
    private final Queue<Message> messages;
    private AtomicInteger currentOffset;

    public Topic(String name) {
        this.name = name;
        this.messages = new ConcurrentLinkedQueue<>();
        this.currentOffset = new AtomicInteger(0);
    }

    // Add a message to the topic
    public int produce(Message message) {
        messages.offer(message);
        return currentOffset.getAndIncrement();
    }

    public Message consume(int offset) {
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
