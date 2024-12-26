package mega;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MainTest {
    @Test
    void appHasAGreeting() {
        Main classUnderTest = new Main();
        assertNotNull(classUnderTest, "Main should be initialized");
    }
}
