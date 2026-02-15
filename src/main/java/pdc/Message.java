package pdc;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */
public class Message {
    public static final String MAGIC = "CSM218";
    public static final int CURRENT_VERSION = 1;
    public static final int MAX_FRAME_SIZE = 64 * 1024 * 1024;

    public String magic;
    public int version;
    public String messageType;
    public String studentId;
    public long timestamp;
    public byte[] payload;

    public Message() {
    }

    public Message(String messageType, String studentId, byte[] payload) {
        this.magic = MAGIC;
        this.version = CURRENT_VERSION;
        this.messageType = messageType;
        this.studentId = studentId;
        this.timestamp = System.currentTimeMillis();
        this.payload = payload;
    }

    public void validate() {
        if (!MAGIC.equals(magic)) {
            throw new IllegalArgumentException("Invalid magic header");
        }
        if (version != CURRENT_VERSION) {
            throw new IllegalArgumentException("Unsupported protocol version");
        }
        if (messageType == null || messageType.isEmpty()) {
            throw new IllegalArgumentException("Missing messageType");
        }
        if (studentId == null || studentId.isEmpty()) {
            throw new IllegalArgumentException("Missing studentId");
        }
        if (payload == null) {
            payload = new byte[0];
        }
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Students must implement their own framing (e.g., length-prefixing).
     */
    public byte[] pack() {
        validate();

        byte[] magicBytes = magic.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] typeBytes = messageType.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] idBytes = studentId.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        int bodyLen = 4 + magicBytes.length
                + 4
                + 4 + typeBytes.length
                + 4 + idBytes.length
                + 8
                + 4 + payload.length;
        if (bodyLen <= 0 || bodyLen > MAX_FRAME_SIZE) {
            throw new IllegalArgumentException("Frame too large");
        }

        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(4 + bodyLen);
        buffer.putInt(bodyLen);
        putBytes(buffer, magicBytes);
        buffer.putInt(version);
        putBytes(buffer, typeBytes);
        putBytes(buffer, idBytes);
        buffer.putLong(timestamp);
        putBytes(buffer, payload);
        return buffer.array();
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        if (data == null || data.length < 4) {
            throw new IllegalArgumentException("Frame too small");
        }
        java.nio.ByteBuffer frame = java.nio.ByteBuffer.wrap(data);
        int bodyLength = frame.getInt();
        if (bodyLength < 0 || bodyLength > MAX_FRAME_SIZE) {
            throw new IllegalArgumentException("Invalid frame length");
        }
        if (data.length < 4 + bodyLength) {
            throw new IllegalArgumentException("Incomplete frame");
        }

        byte[] body = new byte[bodyLength];
        frame.get(body);
        return unpackBody(java.nio.ByteBuffer.wrap(body));
    }

    public static Message parse(byte[] data) {
        return unpack(data);
    }

    public void writeTo(java.io.DataOutputStream out) throws java.io.IOException {
        byte[] frame = pack();
        out.write(frame);
    }

    public static Message readFrom(java.io.DataInputStream in) throws java.io.IOException {
        int bodyLength = in.readInt();
        if (bodyLength < 0 || bodyLength > MAX_FRAME_SIZE) {
            throw new IllegalArgumentException("Invalid frame length");
        }
        byte[] body = new byte[bodyLength];
        readExactly(in, body, 0, bodyLength);
        return unpackBody(java.nio.ByteBuffer.wrap(body));
    }

    private static Message unpackBody(java.nio.ByteBuffer body) {
        byte[] magicBytes = readBytes(body);
        Message msg = new Message();
        msg.magic = new String(magicBytes, java.nio.charset.StandardCharsets.UTF_8);
        msg.version = body.getInt();
        msg.messageType = new String(readBytes(body), java.nio.charset.StandardCharsets.UTF_8);
        msg.studentId = new String(readBytes(body), java.nio.charset.StandardCharsets.UTF_8);
        msg.timestamp = body.getLong();
        msg.payload = readBytes(body);
        msg.validate();
        return msg;
    }

    private static void putBytes(java.nio.ByteBuffer buffer, byte[] value) {
        if (value == null) {
            buffer.putInt(0);
            return;
        }
        buffer.putInt(value.length);
        if (value.length > 0) {
            buffer.put(value);
        }
    }

    private static byte[] readBytes(java.nio.ByteBuffer body) {
        int len = body.getInt();
        if (len < 0 || len > body.remaining()) {
            throw new IllegalArgumentException("Invalid field length");
        }
        byte[] bytes = new byte[len];
        if (len > 0) {
            body.get(bytes);
        }
        return bytes;
    }

    private static void readExactly(java.io.DataInputStream in, byte[] target, int offset, int length)
            throws java.io.IOException {
        int cursor = offset;
        int remaining = length;
        while (remaining > 0) {
            int read = in.read(target, cursor, remaining);
            if (read < 0) {
                throw new java.io.EOFException("Stream ended mid-frame");
            }
            cursor += read;
            remaining -= read;
        }
    }
}
