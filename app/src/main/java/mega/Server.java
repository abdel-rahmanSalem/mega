package mega;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Server implements AutoCloseable {
    private final int port;
    private final ExecutorService executorService;
    private volatile boolean running;
    private ServerSocket serverSocket;

    public Server(int port, int poolSize) {
        this.port = port;
        this.executorService = Executors.newFixedThreadPool(poolSize);
        this.running = true;
        System.out.println("[Server] Initializing server on port " + port + " with pool size " + poolSize);
    }

    public void start() {
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("[Server] Started successfully on port " + port);

            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("[Server] New client connection accepted from: " +
                            clientSocket.getInetAddress() + ":" + clientSocket.getPort());
                    executorService.submit(new ClientHandler(clientSocket));
                } catch (IOException e) {
                    if (running) {
                        System.err.println("[Server] Error accepting client connection: " + e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("[Server] Fatal error starting server: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        System.out.println("[Server] Initiating server shutdown");
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
                System.out.println("[Server] Server socket closed");
            }

            executorService.shutdown();
            System.out.println("[Server] Waiting for executor service to terminate");

            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                System.out.println("[Server] Forcing executor service shutdown");
                executorService.shutdownNow();
            }

            System.out.println("[Server] Server shutdown completed");
        } catch (IOException | InterruptedException e) {
            System.err.println("[Server] Error during server shutdown: " + e.getMessage());
            executorService.shutdownNow();
        }
    }
}