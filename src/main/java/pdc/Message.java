package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 *
 * Wire format (custom, length-prefixed when sent over sockets): - magic (fixed
 * UTF-8 string) - version (int) - type (UTF-8 length + bytes) - sender (UTF-8
 * length + bytes) - timestamp (long) - payload length (int) + payload bytes
 */
public class Message {

    public static final String PROTOCOL_MAGIC = "CSM218";
    public static final int PROTOCOL_VERSION = 1;

    public String magic = PROTOCOL_MAGIC;
    public int version = PROTOCOL_VERSION;
    public String type;    // e.g. TASK, RESULT, HEARTBEAT
    public String sender;  // sender id
    public long timestamp;
    public byte[] payload;

    public Message() {
        this.timestamp = System.currentTimeMillis();
    }

    public Message(String type, String sender, byte[] payload) {
        this();
        this.type = type;
        this.sender = sender;
        this.payload = payload == null ? new byte[0] : payload;
    }

    /**
     * Converts the message to a byte stream (without length prefix).
     */
    public byte[] pack() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            byte[] magicBytes = magic.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(magicBytes.length);
            dos.write(magicBytes);

            dos.writeInt(version);

            byte[] typeB = (type == null ? "" : type).getBytes(StandardCharsets.UTF_8);
            dos.writeInt(typeB.length);
            dos.write(typeB);

            byte[] senderB = (sender == null ? "" : sender).getBytes(StandardCharsets.UTF_8);
            dos.writeInt(senderB.length);
            dos.write(senderB);

            dos.writeLong(timestamp);

            int payloadLen = payload == null ? 0 : payload.length;
            dos.writeInt(payloadLen);
            if (payloadLen > 0) {
                dos.write(payload);
            }

            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to pack message", e);
        }
    }

    /**
     * Reconstructs a Message from a byte array produced by pack().
     */
    public static Message unpack(byte[] data) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);

            int magicLen = dis.readInt();
            byte[] magicB = new byte[magicLen];
            dis.readFully(magicB);
            String magic = new String(magicB, StandardCharsets.UTF_8);

            int version = dis.readInt();

            int typeLen = dis.readInt();
            byte[] typeB = new byte[typeLen];
            dis.readFully(typeB);
            String type = new String(typeB, StandardCharsets.UTF_8);

            int senderLen = dis.readInt();
            byte[] senderB = new byte[senderLen];
            dis.readFully(senderB);
            String sender = new String(senderB, StandardCharsets.UTF_8);

            long timestamp = dis.readLong();

            int payloadLen = dis.readInt();
            byte[] payload = new byte[payloadLen];
            if (payloadLen > 0) {
                dis.readFully(payload);
            }

            Message m = new Message();
            m.magic = magic;
            m.version = version;
            m.type = type;
            m.sender = sender;
            m.timestamp = timestamp;
            m.payload = payload;
            return m;
        } catch (IOException e) {
            throw new RuntimeException("Failed to unpack message", e);
        }
    }
}
