package mega;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Broker {
    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private static volatile Broker instance;

    private Broker() {
        System.out.println("[Broker] Initializing broker instance");
    }

    public static Broker getInstance() {
        if (instance == null) {
            synchronized (Broker.class) {
                if (instance == null) {
                    instance = new Broker();
                }
            }
        }
        return instance;
    }

    public void createTopic(String topicName) {
        lock.writeLock().lock();
        try {
            System.out.println("[Broker] Attempting to create topic: " + topicName);
            topics.computeIfAbsent(topicName, Topic::new);
            System.out.println("[Broker] Topic created successfully: " + topicName);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int produce(String topicName, Message message) {
        lock.readLock().lock();
        try {
            System.out.println("[Broker] Producing message for topic: " + topicName);
            Topic topic = topics.get(topicName);
            if (topic == null) {
                System.out.println("[Broker] ERROR: Topic not found: " + topicName);
                throw new TopicNotFoundException(topicName);
            }
            int offset = topic.produce(message);
            System.out.println("[Broker] Message produced successfully at offset: " + offset);
            return offset;
        } finally {
            lock.readLock().unlock();
        }
    }

    public Message consume(String topicName, int offset) {
        lock.readLock().lock();
        try {
            System.out.println("[Broker] Consuming message from topic: " + topicName + " at offset: " + offset);
            Topic topic = topics.get(topicName);
            if (topic == null) {
                System.out.println("[Broker] ERROR: Topic not found: " + topicName);
                throw new TopicNotFoundException(topicName);
            }
            Message message = topic.consume(offset);
            if (message != null) {
                System.out.println("[Broker] Retrieved message from topic: " + topicName);
            } else {
                System.out.println("[Broker] No message found at offset: " + offset);
            }
            return message;
        } finally {
            lock.readLock().unlock();
        }
    }
}