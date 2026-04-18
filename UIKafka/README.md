# UIKafka

A premium React dashboard for monitoring the `minimal_Kafaka` cluster.

## Features
- **Cluster Health**: Real-time broker status and connectivity.
- **Topic Monitoring**: Live list of topics, partitions, and replication status.
- **Telemetry**: Prometheus-style metrics visualization (Produce/Consume throughput).
- **Premium Design**: Dark mode, glassmorphism, and responsive layout.

## Setup & Running

1. **Install Dependencies**:
   ```bash
   cd UIKafka
   npm install
   ```

2. **Start the Dashboard**:
   ```bash
   npm run dev
   ```
   The dashboard will be available at `http://localhost:3000`.

3. **Broker Connection**:
   The dashboard is configured to proxy requests to `http://localhost:9092` (Broker 1). Ensure your broker is running.

## Project Structure
- `src/App.tsx`: Core logic and dashboard layout.
- `src/index.css`: Design system and UI tokens.
- `vite.config.ts`: Proxy settings for seamless API communication.
