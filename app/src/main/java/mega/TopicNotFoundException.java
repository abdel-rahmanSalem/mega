package mega;

public class TopicNotFoundException extends RuntimeException {
    public TopicNotFoundException(String topicName) {
        super("Topic not found: " + topicName);
    }
}