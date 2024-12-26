package mega;

public class Main {
    public static void main(String[] args) {
        try {
            Server server = new Server(8080);
            server.start();
        } catch (Exception e) {
            System.err.println("Server failed to start: " + e.getMessage());
        }
    }
}
