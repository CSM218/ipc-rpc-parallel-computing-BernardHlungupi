package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 *
 * CHALLENGE: You must handle 'Stragglers' (slow workers) and 'Partitions'
 * (disconnected workers). A simple sequential loop will not pass the advanced
 * autograder performance checks.
 */
public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final Map<Socket, DataOutputStream> workers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService heartbeatScheduler = new ScheduledThreadPoolExecutor(1);

    /**
     * Entry point for a distributed computation.
     *
     * Students must: 1. Partition the problem into independent 'computational
     * units'. 2. Schedule units across a dynamic pool of workers. 3. Handle
     * result aggregation while maintaining thread safety.
     *
     * @param operation A string descriptor of the matrix operation (e.g.
     * "BLOCK_MULTIPLY")
     * @param data The raw matrix data to be processed
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        // Simple task sender: partition rows among available workers and send TASK messages.
        try {
            List<DataOutputStream> available = new ArrayList<>(workers.values());
            if (available.isEmpty()) {
                return null;
            }

            int nWorkers = Math.min(workerCount, available.size());
            if (nWorkers <= 0) {
                return null;
            }

            int totalRows = data.length;
            int chunkSize = Math.max(1, totalRows / nWorkers);
            int start = 0;
            int idx = 0;

            List<DataOutputStream> workerList = new ArrayList<>(available);
            Collections.shuffle(workerList);

            while (start < totalRows && idx < workerList.size()) {
                int end = Math.min(totalRows, start + chunkSize);
                // serialize chunk as simple CSV rows using '|' as row separator
                StringBuilder sb = new StringBuilder();
                sb.append(operation).append(";");
                sb.append(start).append("-").append(end).append(";");
                for (int r = start; r < end; r++) {
                    for (int c = 0; c < data[r].length; c++) {
                        if (c > 0) {
                            sb.append(',');
                        }
                        sb.append(data[r][c]);
                    }
                    if (r < end - 1) {
                        sb.append('|');
                    }
                }

                byte[] payload = sb.toString().getBytes(StandardCharsets.UTF_8);
                Message msg = new Message();
                msg.type = "TASK";
                msg.payload = payload;
                sendMessage(workerList.get(idx), msg);

                start = end;
                idx++;
            }

            return null; // aggregation not implemented here
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Start the communication listener. Use your custom protocol designed in
     * Message.java.
     */
    public void listen(int port) throws IOException {
        ServerSocket server = new ServerSocket(port);

        // accept connections asynchronously
        systemThreads.submit(() -> {
            while (!server.isClosed()) {
                try {
                    Socket s = server.accept();
                    DataOutputStream dos = new DataOutputStream(s.getOutputStream());
                    DataInputStream dis = new DataInputStream(s.getInputStream());
                    workers.put(s, dos);

                    // spawn reader for this worker
                    systemThreads.submit(() -> {
                        try {
                            while (!s.isClosed()) {
                                int len;
                                try {
                                    len = dis.readInt();
                                } catch (IOException ioe) {
                                    break;
                                }
                                if (len <= 0) {
                                    continue;
                                }
                                byte[] data = new byte[len];
                                dis.readFully(data);
                                Message m = Message.unpack(data);
                                if (m == null) {
                                    continue;
                                }
                                if ("HEARTBEAT".equals(m.type)) {
                                    // update last seen timestamp (could store state)
                                } else if ("RESULT".equals(m.type)) {
                                    // result handling could be implemented here
                                }
                            }
                        } catch (IOException | RuntimeException e) {
                            // worker disconnected
                        } finally {
                            workers.remove(s);
                            try {
                                s.close();
                            } catch (IOException ignored) {
                            }
                        }
                    });
                } catch (IOException e) {
                    // accept failed, continue or break
                    break;
                }
            }
        });

        // schedule heartbeats to workers
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            Message hb = new Message();
            hb.type = "HEARTBEAT";
            hb.payload = new byte[0];
            for (Map.Entry<Socket, DataOutputStream> e : workers.entrySet()) {
                try {
                    sendMessage(e.getValue(), hb);
                } catch (IOException ex) {
                    // ignore per-worker failures
                }
            }
        }, 1, 2, TimeUnit.SECONDS);
    }

    /**
     * System Health Check. Detects dead workers and re-integrates recovered
     * workers.
     */
    public void reconcileState() {
        // Lightweight reconciliation: remove closed sockets from the map.
        List<Socket> toRemove = new ArrayList<>();
        for (Socket s : workers.keySet()) {
            if (s.isClosed() || !s.isConnected()) {
                toRemove.add(s);
            }
        }
        for (Socket s : toRemove) {
            workers.remove(s);
        }
    }

    /**
     * Send a message to a worker through the provided output stream.
     *
     * @param output The DataOutputStream connected to the worker
     * @param msg The Message object to send
     * @throws IOException if writing fails
     */
    private void sendMessage(DataOutputStream output, Message msg) throws IOException {
        byte[] packed = msg.pack();
        output.writeInt(packed.length);
        output.write(packed);
        output.flush();
    }

}
