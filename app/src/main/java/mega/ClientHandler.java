// Refactored ClientHandler.java
package mega;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientHandler implements Runnable, AutoCloseable {
    private final Socket clientSocket;
    private final Broker broker;
    private final AtomicBoolean running;
    private DataInputStream input;
    private DataOutputStream output;
    private final String clientId;
    private static final int READ_TIMEOUT_MS = 30000; // 30 seconds
    // private static final int WRITE_TIMEOUT_MS = 30000; // 30 seconds
    private static final int MAX_MESSAGE_SIZE = 1024 * 1024; // 1MB

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
        this.broker = Broker.getInstance();
        this.running = new AtomicBoolean(true);
        this.clientId = clientSocket.getInetAddress() + ":" + clientSocket.getPort();
        logInfo("New client connected");
    }

    @Override
    public void run() {
        try {
            initializeStreams();
            handleClientRequests();
        } catch (IOException e) {
            handleError("Error handling client", e);
        } finally {
            close();
        }
    }

    private void initializeStreams() throws IOException {
        logInfo("Initializing streams for client");
        try {
            clientSocket.setSoTimeout(READ_TIMEOUT_MS);
            input = new DataInputStream(new BufferedInputStream(clientSocket.getInputStream()));
            output = new DataOutputStream(new BufferedOutputStream(clientSocket.getOutputStream()));
        } catch (IOException e) {
            handleError("Failed to initialize streams", e);
            throw e;
        }
    }

    private void handleClientRequests() throws IOException {
        while (running.get()) {
            try {
                Message message = new Message(input);
                logInfo("Received message: " + message);
                processMessage(message);
            } catch (EOFException e) {
                logInfo("Client disconnected");
                break;
            } catch (IOException e) {
                handleError("Network error", e);
                sendErrorResponse(0, ErrorCode.NETWORK_ERROR);
                break;
            }
        }
    }

    private void processMessage(Message message) throws IOException {
        if (message.getPayloadLength() > MAX_MESSAGE_SIZE) {
            handleError("Message size exceeds maximum allowed size", null);
            sendErrorResponse(message.getCorrelationId(), ErrorCode.MESSAGE_TOO_LARGE);
            return;
        }

        try {
            switch (message.getMessageType()) {
                case CREATE_TOPIC:
                    handleCreateTopic(message);
                    break;
                case PRODUCE:
                    handleProduce(message);
                    break;
                case CONSUME:
                    handleConsume(message);
                    break;
                default:
                    handleUnknownMessageType(message);
            }
            output.flush();
        } catch (TopicNotFoundException e) {
            handleError("Topic not found: " + message.getTopic(), e);
            sendErrorResponse(message.getCorrelationId(), ErrorCode.TOPIC_NOT_FOUND);
        } catch (IllegalArgumentException e) {
            handleError("Invalid request", e);
            sendErrorResponse(message.getCorrelationId(), ErrorCode.INVALID_REQUEST);
        } catch (IllegalStateException e) {
            handleError("Resource exhausted", e);
            sendErrorResponse(message.getCorrelationId(), ErrorCode.RESOURCE_EXHAUSTED);
        } catch (Exception e) {
            handleError("Error processing message", e);
            sendErrorResponse(message.getCorrelationId(), ErrorCode.INTERNAL_ERROR);
        }
    }

    private void handleCreateTopic(Message message) throws IOException {
        try {
            broker.createTopic(message.getTopic());
            sendCreateTopicResponse(message.getCorrelationId(), true, message.getTimestamp(), message.getTopic());
            logInfo("Topic created successfully: " + message.getTopic());
        } catch (Exception e) {
            handleError("Failed to create topic: " + message.getTopic(), e);
            sendErrorResponse(message.getCorrelationId(), ErrorCode.INTERNAL_ERROR);
        }
    }

    private void handleProduce(Message message) throws IOException {
        try {
            int offset = broker.produce(message.getTopic(), message);
            sendProduceResponse(message.getCorrelationId(), true, message.getTimestamp(), offset);
            logInfo("Message produced successfully at offset: " + offset);
        } catch (TopicNotFoundException e) {
            handleError("Topic not found for produce: " + message.getTopic(), e);
            sendErrorResponse(message.getCorrelationId(), ErrorCode.TOPIC_NOT_FOUND);
        } catch (Exception e) {
            handleError("Failed to produce message", e);
            sendErrorResponse(message.getCorrelationId(), ErrorCode.INTERNAL_ERROR);
        }
    }

    private void handleConsume(Message message) throws IOException {
        try {
            Message msg = broker.consume(message.getTopic(), message.getOffset());
            if (msg == null) {
                handleError("Invalid offset: " + message.getOffset(), null);
                sendErrorResponse(message.getCorrelationId(), ErrorCode.INVALID_OFFSET);
                return;
            }
            sendConsumeResponse(message.getCorrelationId(), true, message.getOffset(),
                    msg.getTimestamp(), msg.getPayload().length, msg.getPayload());
            logInfo("Message consumed successfully from offset: " + message.getOffset());
        } catch (TopicNotFoundException e) {
            handleError("Topic not found for consume: " + message.getTopic(), e);
            sendErrorResponse(message.getCorrelationId(), ErrorCode.TOPIC_NOT_FOUND);
        } catch (Exception e) {
            handleError("Failed to consume message", e);
            sendErrorResponse(message.getCorrelationId(), ErrorCode.INTERNAL_ERROR);
        }
    }

    private void handleUnknownMessageType(Message message) throws IOException {
        handleError("Unknown message type: " + message.getMessageType(), null);
        sendErrorResponse(message.getCorrelationId(), ErrorCode.INVALID_MESSAGE_TYPE);
    }

    private void sendCreateTopicResponse(int correlationId, boolean success, long timestamp, String topic)
            throws IOException {
        output.writeInt(correlationId);
        output.writeByte(success ? 1 : 0);
        output.writeLong(timestamp);
        output.writeUTF(topic);
    }

    private void sendProduceResponse(int correlationId, boolean success, long timestamp, int offset)
            throws IOException {
        output.writeInt(correlationId);
        output.writeByte(success ? 1 : 0);
        output.writeLong(timestamp);
        output.writeInt(offset);
    }

    private void sendConsumeResponse(int correlationId, boolean success, int offset, long timestamp,
            int payloadLength, byte[] payload)
            throws IOException {

        int nextOffset = offset + 1;
        output.writeInt(correlationId);
        output.writeByte(success ? 1 : 0);
        output.writeLong(timestamp);
        output.writeInt(nextOffset);
        output.writeInt(payloadLength);
        output.write(payload);
    }

    private void sendErrorResponse(int correlationId, ErrorCode errorCode) throws IOException {
        try {
            new ErrorResponse(correlationId, errorCode).writeTo(output);
            output.flush();
            handleError("Sent error response", null);
        } catch (IOException e) {
            handleError("Failed to send error response", e);
            throw e;
        }
    }

    private void handleError(String message, Exception e) {
        String errorMessage = String.format("[ClientHandler] %s - Client: %s", message, clientId);
        if (e != null) {
            System.err.println(errorMessage + " - Error: " + e.getMessage());
            e.printStackTrace();
        } else {
            System.err.println(errorMessage);
        }
    }

    private void logInfo(String message) {
        System.out.println(String.format("[ClientHandler] %s - Client: %s", message, clientId));
    }

    @Override
    public void close() {
        logInfo("Closing connection");
        running.set(false);

        if (output != null) {
            try {
                output.flush();
            } catch (IOException e) {
                handleError("Error flushing output stream", e);
            }
        }

        closeQuietly(input, "input stream");
        closeQuietly(output, "output stream");
        closeQuietly(clientSocket, "client socket");
    }

    private void closeQuietly(AutoCloseable resource, String resourceName) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception e) {
                handleError("Error closing " + resourceName, e);
            }
        }
    }
}
