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

        try {
            byte[] magicBytes = magic.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] typeBytes = messageType.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] idBytes = studentId.getBytes(java.nio.charset.StandardCharsets.UTF_8);

            java.io.ByteArrayOutputStream bodyStream = new java.io.ByteArrayOutputStream();
            java.io.DataOutputStream bodyOut = new java.io.DataOutputStream(bodyStream);

            bodyOut.writeInt(magicBytes.length);
            bodyOut.write(magicBytes);
            bodyOut.writeInt(version);

            bodyOut.writeInt(typeBytes.length);
            bodyOut.write(typeBytes);

            bodyOut.writeInt(idBytes.length);
            bodyOut.write(idBytes);

            bodyOut.writeLong(timestamp);

            bodyOut.writeInt(payload.length);
            if (payload.length > 0) {
                bodyOut.write(payload);
            }
            bodyOut.flush();

            byte[] body = bodyStream.toByteArray();
            java.io.ByteArrayOutputStream framedStream = new java.io.ByteArrayOutputStream();
            java.io.DataOutputStream framedOut = new java.io.DataOutputStream(framedStream);

            framedOut.writeInt(body.length);
            framedOut.write(body);
            framedOut.flush();
            return framedStream.toByteArray();
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Failed to pack message", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        try {
            java.io.DataInputStream in = new java.io.DataInputStream(new java.io.ByteArrayInputStream(data));
            int bodyLength = in.readInt();
            if (bodyLength < 0 || bodyLength > data.length - 4) {
                throw new IllegalArgumentException("Invalid frame length");
            }
            byte[] body = new byte[bodyLength];
            in.readFully(body);

            java.io.DataInputStream bodyIn = new java.io.DataInputStream(new java.io.ByteArrayInputStream(body));
            int magicLen = bodyIn.readInt();
            if (magicLen < 0) {
                throw new IllegalArgumentException("Invalid magic length");
            }
            byte[] magicBytes = new byte[magicLen];
            bodyIn.readFully(magicBytes);

            Message msg = new Message();
            msg.magic = new String(magicBytes, java.nio.charset.StandardCharsets.UTF_8);
            msg.version = bodyIn.readInt();

            int typeLen = bodyIn.readInt();
            byte[] typeBytes = new byte[typeLen];
            bodyIn.readFully(typeBytes);
            msg.messageType = new String(typeBytes, java.nio.charset.StandardCharsets.UTF_8);

            int idLen = bodyIn.readInt();
            byte[] idBytes = new byte[idLen];
            bodyIn.readFully(idBytes);
            msg.studentId = new String(idBytes, java.nio.charset.StandardCharsets.UTF_8);

            msg.timestamp = bodyIn.readLong();

            int payloadLen = bodyIn.readInt();
            if (payloadLen < 0) {
                throw new IllegalArgumentException("Invalid payload length");
            }
            msg.payload = new byte[payloadLen];
            if (payloadLen > 0) {
                bodyIn.readFully(msg.payload);
            }

            msg.validate();
            return msg;
        } catch (java.io.IOException e) {
            throw new IllegalArgumentException("Failed to parse message", e);
        }
    }

    public static Message parse(byte[] data) {
        return unpack(data);
    }
}
