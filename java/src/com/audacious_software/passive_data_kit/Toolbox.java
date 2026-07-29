package com.audacious_software.passive_data_kit;

import android.util.Base64;

import com.goterl.lazysodium.LazySodiumAndroid;
import com.goterl.lazysodium.SodiumAndroid;
import com.goterl.lazysodium.interfaces.Box;
import com.goterl.lazysodium.interfaces.Hash;

import java.nio.charset.Charset;
import java.text.Normalizer;
import java.util.Locale;
import java.util.regex.Pattern;

public class Toolbox {
    private static final LazySodiumAndroid SODIUM = new LazySodiumAndroid(new SodiumAndroid());

    private static final Pattern NONLATIN = Pattern.compile("[^\\w-]");
    private static final Pattern WHITESPACE = Pattern.compile("[\\s]");
    private static final Pattern EDGESDHASHES = Pattern.compile("(^-|-$)");

    public static String toSlug(String input) {

        String nowhitespace = WHITESPACE.matcher(input).replaceAll("-");
        String normalized = Normalizer.normalize(nowhitespace, Normalizer.Form.NFD);
        String slug = NONLATIN.matcher(normalized).replaceAll("");
        slug = EDGESDHASHES.matcher(slug).replaceAll("");

        return slug.toLowerCase(Locale.ENGLISH);
    }

    public static byte[] decodeBase64(String encoded) {
        return Base64.decode(encoded, Base64.DEFAULT);
    }

    public static String encodeBase64(byte[] bytes) {
        return Base64.encodeToString(bytes, Base64.CRLF);
    }

    public static String encrypt(byte[] privateKey, byte[] publicKey, byte[] nonce, String payload) {
        byte[] message = payload.getBytes(Charset.forName("UTF-8"));

        byte[] ciphertext = new byte[Box.MACBYTES + message.length];

        if (Toolbox.SODIUM.cryptoBoxEasy(ciphertext, message, message.length, nonce, publicKey, privateKey)) {
            return Toolbox.encodeBase64(ciphertext);
        }

        return payload;
    }

    public static String hash(String payload) {
        byte[] message = payload.getBytes(Charset.forName("UTF-8"));
        byte[] hashText = new byte[Hash.SHA512_BYTES];
        Toolbox.SODIUM.cryptoHashSha512(hashText, message, message.length);

        return Toolbox.encodeBase64(hashText);
    }

    public static byte[] randomNonce() {
        return Toolbox.SODIUM.randomBytesBuf(Box.NONCEBYTES);
    }
}
