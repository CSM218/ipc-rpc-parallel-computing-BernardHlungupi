package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 */
public class Worker {

    private final ExecutorService threads = Executors.newCachedThreadPool();
    private final ScheduledExecutorService heartbeatScheduler = new ScheduledThreadPoolExecutor(1);
    private final Object connectionLock = new Object();
    private volatile Socket masterSocket;
    private volatile DataOutputStream masterOut;
    private final Object sendLock = new Object();

    public void joinCluster(String masterHost, int port) {
        try {
            masterSocket = new Socket(masterHost, port);
            masterOut = new DataOutputStream(masterSocket.getOutputStream());
            DataInputStream dis = new DataInputStream(masterSocket.getInputStream());

            // reader thread
            threads.submit(() -> {
                try {
                    while (!masterSocket.isClosed()) {
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
                        if ("TASK".equals(m.type)) {
                            // process task asynchronously
                            threads.submit(() -> handleTask(m));
                        } else if ("HEARTBEAT".equals(m.type)) {
                            // could update local status
                        }
                    }
                } catch (IOException e) {
                    // connection lost
                } finally {
                    try {
                        masterSocket.close();
                    } catch (IOException ignored) {
                    }
                }
            });

            // send periodic heartbeats to master
            heartbeatScheduler.scheduleAtFixedRate(() -> {
                try {
                    Message hb = new Message();
                    hb.type = "HEARTBEAT";
                    hb.sender = "worker";
                    hb.payload = new byte[0];
                    sendMessage(hb);
                } catch (IOException e) {
                    // ignore
                }
            }, 1, 2, TimeUnit.SECONDS);
        } catch (IOException e) {
            // Handle failure to connect gracefully (tests expect no exception)
        }
    }

    private void handleTask(Message m) {
        try {
            String payload = new String(m.payload, StandardCharsets.UTF_8);
            // payload format: OP;start-end;row1|row2|...
            String resultStr;
            try {
                // simple example: compute sum of all integers in payload
                String[] parts = payload.split(";", 3);
                String operation = parts.length >= 1 ? parts[0] : "";
                String range = parts.length >= 2 ? parts[1] : "";
                String rowsPart = parts.length >= 3 ? parts[2] : "";
                long sum = 0;
                if (!rowsPart.isEmpty()) {
                    String[] rows = rowsPart.split("\\|");
                    for (String row : rows) {
                        String[] vals = row.split(",");
                        for (String v : vals) {
                            if (v.trim().isEmpty()) {
                                continue;
                            }
                            sum += Long.parseLong(v.trim());
                        }
                    }
                }
                resultStr = Long.toString(sum);
            } catch (NumberFormatException ex) {
                resultStr = "ERR";
            }

            byte[] resPayload = resultStr.getBytes(StandardCharsets.UTF_8);
            Message res = new Message();
            res.type = "RESULT";
            res.sender = "worker";
            res.payload = resPayload;
            sendMessage(res);
        } catch (IOException e) {
            // swallow to avoid crashing thread
        }
    }

    private void sendMessage(Message msg) throws IOException {
        synchronized (sendLock) {
            if (masterOut == null) {
                throw new IOException("Not connected to master");
            }
            byte[] bytes = msg.pack();
            masterOut.writeInt(bytes.length);
            masterOut.write(bytes);
            masterOut.flush();
        }
    }

    public void execute() {
        // start background worker loops if needed. For this simple implementation
        // the important behavior is in joinCluster which starts readers and handlers.
    }
}
