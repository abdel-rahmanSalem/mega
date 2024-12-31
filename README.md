# MEGA (Microservices Event Gateway Architecture)

A lightweight, binary-based message broker system inspired by Apache Kafka, designed for learning and prototyping. MEGA provides a communication layer protocol for microservices with support for concurrent clients and topic-based message patterns.

⚠️ **Note**: This project is for learning purposes only and is not intended for production use.

## Key Features

- Binary-based protocol for efficient message transmission
- Topic-based produce/consume messaging
- Support for concurrent clients
- Java-based broker with Multiple clients implementation

## Core Components

- **Broker**: Central message coordinator
- **Topics**: Message categories
- **Producers**: Message senders
- **Consumers**: Message receivers
- **ClientHandler**: Manages client connections
- **Protocol**: Binary-based message format

## Quick Start

### Prerequisites

- Java 11 or higher
- Gradle

### Building the Project

```bash
# Clone the repository
git clone https://github.com/abdel-rahmanSalem/mega.git
cd mega

# Build with Gradle
gradle build
```

### Running the Broker

```bash
gradle run
```

The broker will start on port 8080 by default.

## Learn More

- [Protocol Documentation](docs/PROTOCOL.md)
- [Client Documentation](docs/CLIENTS.md)
- [Use Cases](docs/USE_CASES.md)
- [Contributing Guide](docs/CONTRIBUTING.md)

## Project Status

This project is under active development. Some features are still being developed.

## Support Us ⭐

If you find MEGA useful or interesting, please consider giving it a star on GitHub! Your support helps make the project more visible to others who might benefit from it.

```
🌟 Star us on GitHub
```

## Connect With Us

Follow us on X (Twitter) for updates, tips, and announcements:

- [@Z_MEGATR0N](https://twitter.com/Z_MEGATR0N)

Feel free to reach out with questions, suggestions, or just to say hello!
