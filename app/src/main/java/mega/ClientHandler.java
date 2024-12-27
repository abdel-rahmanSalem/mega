package mega;

import java.io.*;
import java.net.Socket;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try (DataInputStream input = new DataInputStream(clientSocket.getInputStream())) {

            while (true) {
                // Parse the message
                Message message = new Message(input);

                // Process based on the message type
                switch (message.getMessageType()) {
                    case PRODUCE:
                        System.out.println("Received PRODUCE message: " + message);
                        break;
                    case CONSUME:
                        System.out.println("Received CONSUME message: " + message);
                        break;
                    default:
                        System.out.println("Unknown message type:");
                }
            }

        } catch (IOException e) {
            System.err.println("Error handling client: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing client socket: " + e.getMessage());
            }
        }
    }
}
