
package ro.ubm.sd.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

public final class Protocol {

    private static final int BUFFER_SIZE = 64 * 1024;

    private Protocol() {
    }

    public static void writeHeader(
            DataOutputStream out,
            String header
    ) throws IOException {
        out.writeUTF(header);
        out.flush();
    }

    public static String readHeader(
            DataInputStream in
    ) throws IOException {
        return in.readUTF();
    }

    public static String checksum(Path file)
            throws IOException {
        MessageDigest digest = newSha256();

        try (InputStream in = Files.newInputStream(file)) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;

            while ((bytesRead = in.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesRead);
            }
        }

        return HexFormat.of().formatHex(digest.digest());
    }

    public static String receiveBytes(
            InputStream in,
            long size,
            Path target
    ) throws IOException {

        if (size < 0) {
            throw new IOException("Invalid file size");
        }

        MessageDigest digest = newSha256();
        byte[] buffer = new byte[BUFFER_SIZE];
        long remaining = size;

        try (OutputStream out = Files.newOutputStream(target)) {
            while (remaining > 0) {
                int requested = (int) Math.min(
                        buffer.length,
                        remaining
                );

                int count = in.read(buffer, 0, requested);

                if (count == -1) {
                    throw new EOFException(
                            "Connection closed. Received "
                                    + (size - remaining)
                                    + " of " + size + " bytes"
                    );
                }

                out.write(buffer, 0, count);
                digest.update(buffer, 0, count);
                remaining -= count;
            }
        }

        return HexFormat.of().formatHex(digest.digest());
    }

    public static void sendBytes(
            Path file,
            OutputStream out
    ) throws IOException {

        try (InputStream in = Files.newInputStream(file)) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int count;

            while ((count = in.read(buffer)) != -1) {
                out.write(buffer, 0, count);
            }
        }

        out.flush();
    }

    private static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(
                    "SHA-256 is not available", e
            );
        }
    }
}
