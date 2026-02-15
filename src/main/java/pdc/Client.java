package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class Client {

    public static void main(String[] args) {
        String host = System.getenv("MASTER_HOST");
        if (host == null || host.isEmpty()) {
            host = "localhost";
        }
        int port = resolvePort(9999);
        int rowsA = 3;
        int colsA = 3;
        int colsB = 3;
        int workerCount = 2;
        if (args.length >= 1) {
            host = args[0];
        }
        if (args.length >= 2) {
            port = parseIntOrDefault(args[1], port);
        }
        if (args.length >= 3) {
            rowsA = parseIntOrDefault(args[2], rowsA);
        }
        if (args.length >= 4) {
            colsA = parseIntOrDefault(args[3], colsA);
        }
        if (args.length >= 5) {
            colsB = parseIntOrDefault(args[4], colsB);
        }
        if (args.length >= 6) {
            workerCount = parseIntOrDefault(args[5], workerCount);
        }

        int[][] matrixA = MatrixGenerator.generateRandomMatrix(rowsA, colsA, 5);
        int[][] matrixB = MatrixGenerator.generateRandomMatrix(colsA, colsB, 5);

        try (Socket socket = new Socket(host, port)) {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            byte[] payload = buildJobRequestPayload("MULTIPLY", workerCount, matrixA, matrixB);
            Message request = new Message("JOB_REQUEST", resolveStudentId(), payload);
            out.write(request.pack());
            out.flush();

            Message response = readMessage(in);
            if (!"JOB_RESPONSE".equalsIgnoreCase(response.messageType)) {
                throw new IllegalStateException("Unexpected response type: " + response.messageType);
            }
            int[][] result = parseJobResponsePayload(response.payload);
            MatrixGenerator.printMatrix(result, "Result");
        } catch (IOException e) {
            throw new IllegalStateException("Client failed", e);
        }
    }

    private static Message readMessage(DataInputStream in) throws IOException {
        int frameLen = in.readInt();
        byte[] body = new byte[frameLen];
        in.readFully(body);
        ByteArrayOutputStream frameOut = new ByteArrayOutputStream(frameLen + 4);
        DataOutputStream wrapper = new DataOutputStream(frameOut);
        wrapper.writeInt(frameLen);
        wrapper.write(body);
        return Message.unpack(frameOut.toByteArray());
    }

    private static byte[] buildJobRequestPayload(String operation, int workerCount, int[][] matrixA, int[][] matrixB) {
        try {
            byte[] opBytes = operation.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            int rowsA = matrixA.length;
            int colsA = matrixA[0].length;
            int rowsB = matrixB.length;
            int colsB = matrixB[0].length;

            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buffer);
            out.writeInt(opBytes.length);
            out.write(opBytes);
            out.writeInt(workerCount);
            out.writeInt(rowsA);
            out.writeInt(colsA);
            out.writeInt(rowsB);
            out.writeInt(colsB);
            for (int[] row : matrixA) {
                for (int value : row) {
                    out.writeInt(value);
                }
            }
            for (int[] row : matrixB) {
                for (int value : row) {
                    out.writeInt(value);
                }
            }
            out.flush();
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to build job request payload", e);
        }
    }

    private static int[][] parseJobResponsePayload(byte[] payload) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
            int rows = in.readInt();
            int cols = in.readInt();
            int[][] result = new int[rows][cols];
            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < cols; j++) {
                    result[i][j] = in.readInt();
                }
            }
            return result;
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse job response", e);
        }
    }

    private static String resolveStudentId() {
        String id = System.getenv("STUDENT_ID");
        if (id == null || id.isEmpty()) {
            return "CLIENT";
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
        return defaultPort;
    }

    private static int parseIntOrDefault(String value, int fallback) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
