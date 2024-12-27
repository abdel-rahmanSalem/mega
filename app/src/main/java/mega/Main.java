package mega;

public class Main {
    public static void main(String[] args) {
        int port = 8080;
        int poolSize = 10;

        try {
            Server server = new Server(port, poolSize);
            server.start();
        } catch (Exception e) {
            System.err.println("Server failed to start: " + e.getMessage());
        }
    }
}
