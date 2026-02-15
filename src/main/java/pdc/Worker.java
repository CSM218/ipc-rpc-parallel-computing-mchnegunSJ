package pdc;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {

    private final ExecutorService taskPool = Executors.newFixedThreadPool(4);
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<String, String> capabilities = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, JobData> jobData = new ConcurrentHashMap<>();
    private volatile boolean running = true;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private final Object sendLock = new Object();
    private String studentId = resolveStudentId();
    private volatile boolean executeStarted = false;

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        int configuredPort = resolvePort(port);

        try {
            socket = new Socket(masterHost, configuredPort);
            socket.setSoTimeout(5000);
            in = new DataInputStream(socket.getInputStream());
            out = new DataOutputStream(socket.getOutputStream());
            Message register = new Message("REGISTER_WORKER", studentId, "ready".getBytes());
            sendMessage(register);
            Message capabilitiesMsg = new Message("REGISTER_CAPABILITIES", studentId, "capability=matrix".getBytes());
            sendMessage(capabilitiesMsg);

            // Start heartbeat thread for health monitoring
            Executors.newSingleThreadExecutor().submit(() -> sendHeartbeat(studentId));
            Executors.newSingleThreadExecutor().submit(this::readLoop);
        } catch (IOException e) {
            // Keep compatibility with scaffold tests: network absence should not throw.
            running = false;
        }
    }

    /**
     * Executes a received task block.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    public void execute() {
        if (executeStarted) {
            return;
        }
        executeStarted = true;
        Executors.newSingleThreadExecutor().submit(this::executeLoop);
    }

    private void executeLoop() {
        while (running) {
            try {
                Message msg = inbox.take();
                msg.validate();
                taskPool.submit(() -> handleMessage(msg));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                running = false;
            }
        }
    }

    public static void main(String[] args) {
        String host = System.getenv("MASTER_HOST");
        if (host == null || host.isEmpty()) {
            host = "localhost";
        }
        int port = resolvePort(9999);
        if (args != null && args.length > 0) {
            host = args[0];
        }
        if (args != null && args.length > 1) {
            try {
                port = Integer.parseInt(args[1]);
            } catch (NumberFormatException ignored) {
            }
        }
        Worker worker = new Worker();
        Runtime.getRuntime().addShutdownHook(new Thread(worker::shutdown));
        worker.joinCluster(host, port);
        worker.execute();
    }

    private void readLoop() {
        try {
            while (running) {
                try {
                    Message msg = Message.readFrom(in);
                    inbox.offer(msg);
                } catch (SocketTimeoutException timeout) {
                    // timeout detected, continue to wait for messages
                }
            }
        } catch (IOException e) {
            running = false;
        }
    }

    private void handleMessage(Message msg) {
        if ("TASK".equalsIgnoreCase(msg.messageType)) {
            try {
                TaskBlock task = parseTaskPayload(msg.payload);
                if (task.matrixA == null || task.matrixB == null) {
                    JobData cached = jobData.get(task.jobId);
                    if (cached != null) {
                        task = task.withMatrices(cached.matrixA, cached.matrixB);
                    }
                }
                if (task.matrixA == null || task.matrixB == null) {
                    sendTaskError(task, "Missing matrix");
                    return;
                }
                int[] values = computeTask(task);
                byte[] payload = buildResultPayload(task.jobId, task.taskId, task.startRow, task.endRow, task.resultCols, values);
                Message result = new Message("TASK_COMPLETE", studentId, payload);
                sendMessage(result);
            } catch (RuntimeException | IOException e) {
                sendTaskError(null, e.getMessage());
            }
            return;
        }

        if ("INIT".equalsIgnoreCase(msg.messageType)) {
            InitBlock init = parseInitPayload(msg.payload);
            jobData.put(init.jobId, new JobData(init.matrixA, init.matrixB));
            byte[] ackPayload = buildInitAckPayload(init.jobId);
            Message ack = new Message("INIT_ACK", studentId, ackPayload);
            try {
                sendMessage(ack);
            } catch (IOException e) {
                running = false;
            }
            return;
        }

        if ("CLEAR_JOB".equalsIgnoreCase(msg.messageType)) {
            int jobId = parseInitAckPayload(msg.payload);
            jobData.remove(jobId);
            return;
        }

        if ("HEARTBEAT".equalsIgnoreCase(msg.messageType)) {
            Message ack = new Message("HEARTBEAT_ACK", studentId, new byte[0]);
            try {
                sendMessage(ack);
            } catch (IOException e) {
                running = false;
            }
            return;
        }

        if ("REGISTER_ACK".equalsIgnoreCase(msg.messageType) || "WORKER_ACK".equalsIgnoreCase(msg.messageType)) {
            capabilities.put("registered", "true");
            return;
        }

        if ("RPC_REQUEST".equalsIgnoreCase(msg.messageType)) {
            // Placeholder for actual computation
            capabilities.put("lastRpc", "handled");
        }
    }

    private void sendHeartbeat(String studentId) {
        try {
            while (running) {
                Message heartbeat = new Message("HEARTBEAT", studentId, new byte[0]);
                sendMessage(heartbeat);
                Thread.sleep(1000);
            }
        } catch (IOException | InterruptedException e) {
            running = false;
        }
    }

    public void shutdown() {
        running = false;
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ignored) {
        }
        taskPool.shutdownNow();
    }

    private void sendMessage(Message message) throws IOException {
        synchronized (sendLock) {
            message.writeTo(out);
            out.flush();
        }
    }

    private void sendTaskError(TaskBlock task, String reason) {
        try {
            String msg = reason == null ? "TASK_ERROR" : reason;
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream payloadOut = new DataOutputStream(buffer);
            int jobId = task == null ? 0 : task.jobId;
            int taskId = task == null ? 0 : task.taskId;
            byte[] msgBytes = msg.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            payloadOut.writeInt(jobId);
            payloadOut.writeInt(taskId);
            payloadOut.writeInt(msgBytes.length);
            if (msgBytes.length > 0) {
                payloadOut.write(msgBytes);
            }
            payloadOut.flush();
            Message error = new Message("TASK_ERROR", studentId, buffer.toByteArray());
            sendMessage(error);
        } catch (IOException ignored) {
            running = false;
        }
    }

    private TaskBlock parseTaskPayload(byte[] payload) {
        try {
            DataInputStream payloadIn = new DataInputStream(new ByteArrayInputStream(payload));
            int opLen = payloadIn.readInt();
            byte[] opBytes = new byte[opLen];
            payloadIn.readFully(opBytes);
            String operation = new String(opBytes, java.nio.charset.StandardCharsets.UTF_8);

            int jobId = payloadIn.readInt();
            int taskId = payloadIn.readInt();
            int startRow = payloadIn.readInt();
            int endRow = payloadIn.readInt();
            boolean hasMatrices = payloadIn.readBoolean();

            int rowsA = 0;
            int colsA = 0;
            int rowsB = 0;
            int colsB = 0;
            int[][] matrixA = null;
            int[][] matrixB = null;
            if (hasMatrices) {
                rowsA = payloadIn.readInt();
                colsA = payloadIn.readInt();
                rowsB = payloadIn.readInt();
                colsB = payloadIn.readInt();
                matrixA = new int[rowsA][colsA];
                for (int i = 0; i < rowsA; i++) {
                    for (int j = 0; j < colsA; j++) {
                        matrixA[i][j] = payloadIn.readInt();
                    }
                }
                matrixB = new int[rowsB][colsB];
                for (int i = 0; i < rowsB; i++) {
                    for (int j = 0; j < colsB; j++) {
                        matrixB[i][j] = payloadIn.readInt();
                    }
                }
            }

            return new TaskBlock(operation, jobId, taskId, startRow, endRow, rowsA, colsA, rowsB, colsB, matrixA, matrixB);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse task payload", e);
        }
    }

    private int[] computeTask(TaskBlock task) {
        int[][] matrixA = task.matrixA;
        int[][] matrixB = task.matrixB;
        int colsA = matrixA[0].length;
        int colsB = matrixB[0].length;
        int[] values = new int[(task.endRow - task.startRow) * colsB];
        int idx = 0;

        for (int i = task.startRow; i < task.endRow; i++) {
            for (int j = 0; j < colsB; j++) {
                int sum = 0;
                for (int k = 0; k < colsA; k++) {
                    sum += matrixA[i][k] * matrixB[k][j];
                }
                values[idx++] = sum;
            }
        }

        return values;
    }

    private byte[] buildResultPayload(int jobId, int taskId, int startRow, int endRow, int cols, int[] values) {
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream payloadOut = new DataOutputStream(buffer);
            payloadOut.writeInt(jobId);
            payloadOut.writeInt(taskId);
            payloadOut.writeInt(startRow);
            payloadOut.writeInt(endRow);
            payloadOut.writeInt(cols);
            for (int value : values) {
                payloadOut.writeInt(value);
            }
            payloadOut.flush();
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to build result payload", e);
        }
    }

    private InitBlock parseInitPayload(byte[] payload) {
        try {
            DataInputStream payloadIn = new DataInputStream(new ByteArrayInputStream(payload));
            int jobId = payloadIn.readInt();
            int rowsA = payloadIn.readInt();
            int colsA = payloadIn.readInt();
            int rowsB = payloadIn.readInt();
            int colsB = payloadIn.readInt();
            int[][] matrixA = new int[rowsA][colsA];
            for (int i = 0; i < rowsA; i++) {
                for (int j = 0; j < colsA; j++) {
                    matrixA[i][j] = payloadIn.readInt();
                }
            }
            int[][] matrixB = new int[rowsB][colsB];
            for (int i = 0; i < rowsB; i++) {
                for (int j = 0; j < colsB; j++) {
                    matrixB[i][j] = payloadIn.readInt();
                }
            }
            return new InitBlock(jobId, matrixA, matrixB);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse init payload", e);
        }
    }

    private byte[] buildInitAckPayload(int jobId) {
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream payloadOut = new DataOutputStream(buffer);
            payloadOut.writeInt(jobId);
            payloadOut.flush();
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to build init ack payload", e);
        }
    }

    private int parseInitAckPayload(byte[] payload) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
            return in.readInt();
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse init ack payload", e);
        }
    }

    private static String resolveStudentId() {
        String id = System.getenv("STUDENT_ID");
        if (id == null || id.isEmpty()) {
            id = System.getenv("WORKER_ID");
        }
        if (id == null || id.isEmpty()) {
            return "WORKER";
        }
        return id;
    }

    private static int resolvePort(int defaultPort) {
        String portEnv = System.getenv("MASTER_PORT");
        if (portEnv != null && !portEnv.isEmpty()) {
            try {
                return Integer.parseInt(portEnv);
            } catch (NumberFormatException ignored) {
            }
        }
        portEnv = System.getenv("PORT");
        if (portEnv != null && !portEnv.isEmpty()) {
            try {
                return Integer.parseInt(portEnv);
            } catch (NumberFormatException ignored) {
            }
        }
        String baseEnv = System.getenv("CSM218_PORT_BASE");
        if (baseEnv != null && !baseEnv.isEmpty()) {
            try {
                return Integer.parseInt(baseEnv);
            } catch (NumberFormatException ignored) {
            }
        }
        return defaultPort;
    }

    private static final class TaskBlock {
        final String operation;
        final int jobId;
        final int taskId;
        final int startRow;
        final int endRow;
        final int rowsA;
        final int colsA;
        final int rowsB;
        final int colsB;
        final int resultCols;
        final int[][] matrixA;
        final int[][] matrixB;

        TaskBlock(String operation,
                int jobId,
                int taskId,
                int startRow,
                int endRow,
                int rowsA,
                int colsA,
                int rowsB,
                int colsB,
                int[][] matrixA,
                int[][] matrixB) {
            this.operation = operation;
            this.jobId = jobId;
            this.taskId = taskId;
            this.startRow = startRow;
            this.endRow = endRow;
            this.rowsA = rowsA;
            this.colsA = colsA;
            this.rowsB = rowsB;
            this.colsB = colsB;
            this.resultCols = colsB;
            this.matrixA = matrixA;
            this.matrixB = matrixB;
        }

        TaskBlock withMatrices(int[][] matrixA, int[][] matrixB) {
            return new TaskBlock(operation,
                    jobId,
                    taskId,
                    startRow,
                    endRow,
                    matrixA.length,
                    matrixA[0].length,
                    matrixB.length,
                    matrixB[0].length,
                    matrixA,
                    matrixB);
        }
    }

    private static final class InitBlock {
        final int jobId;
        final int[][] matrixA;
        final int[][] matrixB;

        InitBlock(int jobId, int[][] matrixA, int[][] matrixB) {
            this.jobId = jobId;
            this.matrixA = matrixA;
            this.matrixB = matrixB;
        }
    }

    private static final class JobData {
        final int[][] matrixA;
        final int[][] matrixB;

        JobData(int[][] matrixA, int[][] matrixB) {
            this.matrixA = matrixA;
            this.matrixB = matrixB;
        }
    }
}
