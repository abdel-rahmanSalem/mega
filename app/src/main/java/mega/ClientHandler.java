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

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
        this.broker = Broker.getInstance();
        this.running = new AtomicBoolean(true);
        this.clientId = clientSocket.getInetAddress() + ":" + clientSocket.getPort();
        System.out.println("[ClientHandler] New client connected: " + clientId);
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
        System.out.println("[ClientHandler] Initializing streams for client: " + clientId);
        input = new DataInputStream(new BufferedInputStream(clientSocket.getInputStream()));
        output = new DataOutputStream(new BufferedOutputStream(clientSocket.getOutputStream()));
    }

    private void handleClientRequests() throws IOException {
        while (running.get()) {
            try {
                Message message = new Message(input);
                System.out.printf("[ClientHandler] Received message from client %s: %s%n",
                        clientId, message);
                processMessage(message);
            } catch (EOFException e) {
                System.out.printf("[ClientHandler] Client disconnected: %s%n", clientId);
                break;
            } catch (IOException e) {
                System.err.printf("[ClientHandler] Network error with client %s: %s%n",
                        clientId, e.getMessage());
                sendErrorResponse(0, ErrorCode.NETWORK_ERROR);
                break;
            }
        }
    }

    private void processMessage(Message message) throws IOException {
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
            System.out.println("[ClientHandler] Topic not found for client " + clientId + ": " + e.getMessage());
            sendErrorResponse(message.getCorrelationId(), ErrorCode.TOPIC_NOT_FOUND);
        } catch (Exception e) {
            handleError("Error processing message for client " + clientId, e);
            sendErrorResponse(message.getCorrelationId(), ErrorCode.INTERNAL_ERROR);
        }
    }

    private void handleCreateTopic(Message message) throws IOException {
        try {
            broker.createTopic(message.getTopic());
            sendCreateTopicResponse(message.getCorrelationId(), true, message.getTimestamp(), message.getTopic());
        } catch (Exception e) {
            System.err.printf("[ClientHandler] Failed to create topic %s - Client: %s - Error: %s%n",
                    message.getTopic(), clientId, e.getMessage());
            sendErrorResponse(message.getCorrelationId(), ErrorCode.INTERNAL_ERROR);
        }
    }

    private void handleProduce(Message message) throws IOException {
        try {
            int offset = broker.produce(message.getTopic(), message);
            sendProduceResponse(message.getCorrelationId(), true, message.getTimestamp(), offset);
        } catch (TopicNotFoundException e) {
            System.err.printf("[ClientHandler] Topic not found %s - Client: %s%n",
                    message.getTopic(), clientId);
            sendErrorResponse(message.getCorrelationId(), ErrorCode.TOPIC_NOT_FOUND);
        } catch (Exception e) {
            System.err.printf("[ClientHandler] Failed to produce message - Client: %s - Error: %s%n",
                    clientId, e.getMessage());
            sendErrorResponse(message.getCorrelationId(), ErrorCode.INTERNAL_ERROR);
        }
    }

    private void handleConsume(Message message) throws IOException {
        try {
            Message msg = broker.consume(message.getTopic(), message.getOffset());
            if (msg == null) {
                sendErrorResponse(message.getCorrelationId(), ErrorCode.INVALID_OFFSET);
                return;
            }
            sendConsumeResponse(message.getCorrelationId(), true, message.getOffset(),
                    msg.getTimestamp(), msg.getPayload().length, msg.getPayload());
        } catch (TopicNotFoundException e) {
            System.err.printf("[ClientHandler] Topic not found %s - Client: %s%n",
                    message.getTopic(), clientId);
            sendErrorResponse(message.getCorrelationId(), ErrorCode.TOPIC_NOT_FOUND);
        } catch (Exception e) {
            System.err.printf("[ClientHandler] Failed to consume message - Client: %s - Error: %s%n",
                    clientId, e.getMessage());
            sendErrorResponse(message.getCorrelationId(), ErrorCode.INTERNAL_ERROR);
        }
    }

    private void handleUnknownMessageType(Message message) throws IOException {
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
        new ErrorResponse(correlationId, errorCode).writeTo(output);
        output.flush();

        System.err.printf("[ClientHandler] Error occurred - Client: %s, CorrelationId: %d, ErrorCode: %s%n",
                clientId, correlationId, errorCode);
    }

    private void handleError(String message, Exception e) {
        System.err.println("[ClientHandler] " + message + " - Client: " + clientId);
        System.err.println("[ClientHandler] Error details: " + e.getMessage());
    }

    @Override
    public void close() {
        System.out.println("[ClientHandler] Closing connection for client: " + clientId);
        running.set(false);
        try {
            if (input != null)
                input.close();
            if (output != null)
                output.close();
            if (clientSocket != null)
                clientSocket.close();
        } catch (IOException e) {
            System.err
                    .println("[ClientHandler] Error closing resources for client " + clientId + ": " + e.getMessage());
        }
    }
}
