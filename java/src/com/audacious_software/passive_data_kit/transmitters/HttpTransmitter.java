package com.audacious_software.passive_data_kit.transmitters;

import android.annotation.SuppressLint;
import android.content.Context;
import android.content.SharedPreferences;
import android.content.pm.ApplicationInfo;
import android.content.pm.PackageInfo;
import android.content.pm.PackageManager;
import android.net.Uri;
import android.os.BadParcelableException;
import android.os.Bundle;
import android.os.Environment;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.Looper;
import android.os.Message;
import android.os.Parcel;
import android.preference.PreferenceManager;
import android.util.Log;

import com.audacious_software.passive_data_kit.DeviceInformation;
import com.audacious_software.passive_data_kit.Logger;
import com.audacious_software.passive_data_kit.PassiveDataKit;
import com.audacious_software.passive_data_kit.Toolbox;
import com.audacious_software.passive_data_kit.generators.Generator;
import com.audacious_software.passive_data_kit.generators.Generators;
import com.audacious_software.passive_data_kit.generators.device.DailyUsageAggregateGenerator;
import com.audacious_software.passive_data_kit.R;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;

import org.apache.commons.lang3.mutable.MutableInt;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.FileOutputStream;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okhttp3.tls.HandshakeCertificates;

@SuppressWarnings({"PointlessBooleanExpression", "unused"})
public class HttpTransmitter extends Transmitter implements Generators.DurableGeneratorUpdatedListener {
    public static final String UPLOAD_URI = "com.audacious_software.passive_data_kit.transmitters.HttpTransmitter.UPLOAD_URI";
    public static final String USER_ID = "com.audacious_software.passive_data_kit.transmitters.HttpTransmitter.USER_ID";
    private static final String HASH_ALGORITHM = "com.audacious_software.passive_data_kit.transmitters.HttpTransmitter.HASH_ALGORITHM";
    private static final String HASH_PREFIX = "com.audacious_software.passive_data_kit.transmitters.HttpTransmitter.HASH_PREFIX";
    public static final String STRICT_SSL_VERIFICATION = "com.audacious_software.passive_data_kit.transmitters.HttpTransmitter.STRICT_SSL_VERIFICATION";
    private static final String UPLOAD_INTERVAL = "com.audacious_software.passive_data_kit.transmitters.HttpTransmitter.UPLOAD_INTERVAL";
    public static final String WIFI_ONLY = "com.audacious_software.passive_data_kit.transmitters.HttpTransmitter.WIFI_ONLY";
    public static final String CHARGING_ONLY = "com.audacious_software.passive_data_kit.transmitters.HttpTransmitter.CHARGING_ONLY";
    public static final String USE_EXTERNAL_STORAGE = "com.audacious_software.passive_data_kit.transmitters.HttpTransmitter.USE_EXTERNAL_STORAGE";
    public static final String COMPRESS_PAYLOADS = "com.audacious_software.passive_data_kit.transmitters.HttpTransmitter.COMPRESS_PAYLOADS";
    public static final String MAX_BUNDLE_SIZE = "com.audacious_software.passive_data_kit.transmitters.HttpTransmitter.MAX_BUNDLE_SIZE";;

    private static final String STORAGE_FOLDER_NAME = "com.audacious_software.passive_data_kit.transmitters.HttpTransmitter.STORAGE_FOLDER_NAME";
    public static final String USER_AGENT_NAME = "com.audacious_software.passive_data_kit.transmitters.HttpTransmitter.USER_AGENT_NAME";

    public static final String PUBLIC_KEY = "com.audacious_software.passive_data_kit.transmitters.HttpTransmitter.PUBLIC_KEY";
    public static final String PRIVATE_KEY = "com.audacious_software.passive_data_kit.transmitters.HttpTransmitter.PRIVATE_KEY";

    private static final String LAST_SUCCESSFUL_TRANSMISSION = "com.audacious_software.passive_data_kit.transmitters.HttpTransmitter.LAST_SUCCESSFUL_TRANSMISSION";
    private static final String LAST_TRANSMISSION_ATTEMPT = "pdk-last-upload-attempt";
    private static final long MIN_RETRY_DELAY = 30_000L;
    private static final long MAX_RETRY_DELAY = 30 * 60_000L;
    private static final HashSet<String> ACTIVE_FILES = new HashSet<>();
    private static final HashMap<String, ReentrantLock> DRAIN_LOCKS = new HashMap<>();

    private static final String ERROR_FILE_EXTENSION = ".error";
    private static final String JSON_EXTENSION = ".json";
    private static final String TEMP_EXTENSION = ".in-progress";
    private static final String TOO_LARGE_FILE_EXTENSION = ".too-large";

    private static final int RESULT_SUCCESS = 0;
    private static final int RESULT_ERROR = 1;

    private static final String COMPRESSION_GZIP = "gzip";
    private static final String COMPRESSION_NONE = "none";

    protected Uri mUploadUri = null;
    protected String mUserId = null;
    private String mHashAlgorithm = null;
    private String mHashPrefix = null;
    protected boolean mStrictSsl = true;
    protected Context mContext = null;
    private long mUploadInterval = 300000;
    protected long mLastAttempt = 0;
    protected boolean mWifiOnly = false;
    protected boolean mChargingOnly = false;
    protected boolean mUseExternalStorage = false;
    private boolean mCompressPayloads = false;

    private String mFolderName = "http-transmitter";
    private String mUserAgent = "http-transmitter";

    protected JsonGenerator mJsonGenerator = null;
    protected File mCurrentFile = null;
    protected long mTransmitted = 0;
    private Thread mLooperThread = null;
    protected Handler mHandler = null;
    private byte[] mPublicKey = null;
    private byte[] mPrivateKey = null;

    private int mCurrentReadingCount = 0;
    private int mMaxReadingCount = 256;
    private int mCurrentBytesWritten = 0;
    private final HandlerThread mDiskThread = new HandlerThread("PDK-upload-disk");
    private final HandlerThread mNetworkThread = new HandlerThread("PDK-upload-network");
    private final Handler mNetworkHandler;
    private volatile boolean mInitialized;
    private boolean mDrainScheduled;
    private long mNextRetryAt;
    private long mRetryDelay;
    private final ArrayList<Runnable> mDrainCompletions = new ArrayList<>();
    private final Runnable mRetryUpload = () -> this.transmit(true);
    public HttpTransmitter() {
        this.mDiskThread.start();
        this.mNetworkThread.start();
        this.mHandler = new Handler(this.mDiskThread.getLooper());
        this.mNetworkHandler = new Handler(this.mNetworkThread.getLooper());
    }

    @SuppressWarnings({"StringConcatenationInLoop"})
    @Override
    public void initialize(Context context, HashMap<String, String> options) {
        this.mContext = context.getApplicationContext();

        if (!options.containsKey(HttpTransmitter.UPLOAD_URI)) {
            throw new HttpTransmitter.IncompleteConfigurationException("The upload URI is not specified.");
        }
        else if (!options.containsKey(HttpTransmitter.USER_ID)) {
            throw new HttpTransmitter.IncompleteConfigurationException("The user ID is not specified.");
        }

        this.mUploadUri = Uri.parse(options.get(HttpTransmitter.UPLOAD_URI));
        this.mUserId = options.get(HttpTransmitter.USER_ID);

        if (options.containsKey(HttpTransmitter.HASH_ALGORITHM)) {
            this.mHashAlgorithm = options.get(HttpTransmitter.HASH_ALGORITHM);
        }

        if (options.containsKey(HttpTransmitter.HASH_PREFIX)) {
            this.mHashPrefix = options.get(HttpTransmitter.HASH_PREFIX);
        }

        if (options.containsKey(HttpTransmitter.PUBLIC_KEY)) {
            this.mPublicKey = Toolbox.decodeBase64(options.get(HttpTransmitter.PUBLIC_KEY));
        }

        if (options.containsKey(HttpTransmitter.PRIVATE_KEY)) {
            this.mPrivateKey = Toolbox.decodeBase64(options.get(HttpTransmitter.PRIVATE_KEY));
        }

        if (options.containsKey(HttpTransmitter.MAX_BUNDLE_SIZE)) {
            this.mMaxReadingCount = Integer.parseInt(options.get(HttpTransmitter.MAX_BUNDLE_SIZE));
        }

        if (this.mHashAlgorithm != null) {
            try {
                MessageDigest md = MessageDigest.getInstance(this.mHashAlgorithm);

                if (this.mHashPrefix != null) {
                    this.mUserId = this.mHashPrefix + this.mUserId;
                }

                byte[] digest = md.digest(this.mUserId.getBytes(Charset.forName("UTF-8")));

                this.mUserId = (new BigInteger(1, digest)).toString(16);

                while (this.mUserId.length() < 64) {
                    this.mUserId = "0" + this.mUserId;
                }
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        if (options.containsKey(HttpTransmitter.STRICT_SSL_VERIFICATION)) {
            this.mStrictSsl = "true".equals(options.get(HttpTransmitter.STRICT_SSL_VERIFICATION).toLowerCase(Locale.ENGLISH));
        }

        if (options.containsKey(HttpTransmitter.UPLOAD_INTERVAL)) {
            this.mUploadInterval = Long.parseLong(options.get(HttpTransmitter.UPLOAD_INTERVAL));
        }

        if (options.containsKey(HttpTransmitter.WIFI_ONLY)) {
            this.mWifiOnly = "true".equals(options.get(HttpTransmitter.WIFI_ONLY).toLowerCase(Locale.ENGLISH));
        }

        if (options.containsKey(HttpTransmitter.CHARGING_ONLY)) {
            this.mChargingOnly = "true".equals(options.get(HttpTransmitter.CHARGING_ONLY).toLowerCase(Locale.ENGLISH));
        }

        if (options.containsKey(HttpTransmitter.COMPRESS_PAYLOADS)) {
            this.mCompressPayloads = "true".equals(options.get(HttpTransmitter.COMPRESS_PAYLOADS).toLowerCase(Locale.ENGLISH));
        }

        if (options.containsKey(HttpTransmitter.USE_EXTERNAL_STORAGE)) {
            this.mUseExternalStorage = "true".equals(options.get(HttpTransmitter.USE_EXTERNAL_STORAGE).toLowerCase(Locale.ENGLISH));
        }

        if (options.containsKey(HttpTransmitter.STORAGE_FOLDER_NAME)) {
            this.mFolderName = options.get(HttpTransmitter.STORAGE_FOLDER_NAME);
        }

        if (options.containsKey(HttpTransmitter.USER_AGENT_NAME)) {
            this.mUserAgent = options.get(HttpTransmitter.USER_AGENT_NAME);
        }

        this.mNextRetryAt = Math.min(System.currentTimeMillis() + MAX_RETRY_DELAY,
                PreferenceManager.getDefaultSharedPreferences(this.mContext).getLong(this.retryPreferenceKey(), 0));
        this.mLastAttempt = PreferenceManager.getDefaultSharedPreferences(this.mContext)
                .getLong(LAST_TRANSMISSION_ATTEMPT, 0);
        this.mInitialized = true;
        synchronized (ACTIVE_FILES) {
            this.recoverInterruptedFiles();
        }
        Generators.getInstance(this.mContext).addNewGeneratorUpdatedListener(this);
        this.mHandler.post(() -> {
            try {
                DailyUsageAggregateGenerator.getInstance(this.mContext).replayPending();
            } catch (RuntimeException error) {
                this.recordFailure(error);
            }
        });
        if (this.mNextRetryAt > 0) {
            // A persisted deadline alone cannot wake the new process after a restart.
            this.scheduleRetryAt(this.mNextRetryAt);
        }
    }

    public void deinitialize(Context context) {
        Generators.getInstance(this.mContext).removeGeneratorUpdatedListener(this);
        this.mInitialized = false;
        this.mNetworkHandler.removeCallbacks(this.mRetryUpload);
        this.mHandler.post(() -> {
            try {
                this.closeOpenSession();
            } catch (IOException error) {
                this.recordFailure(error);
            } finally {
                this.mDiskThread.quitSafely();
                this.mNetworkThread.quitSafely();
            }
        });
    }

    private boolean shouldAttemptUpload(boolean force) {
        if (force) {
            return true;
        }

        if (this.mWifiOnly) {
            if (DeviceInformation.wifiAvailable(this.mContext) == false) {
                return false;
            }
        }

        if (this.mChargingOnly) {
            if (DeviceInformation.isPluggedIn(this.mContext) == false) {
                return false;
            }
        }

        return true;
    }

    @Override
    public synchronized boolean transmit(boolean force) {
        long now = System.currentTimeMillis();
        if (!this.mInitialized || this.mDrainScheduled) {
            return false;
        }
        if (now < this.mNextRetryAt) {
            this.scheduleRetryAt(this.mNextRetryAt);
            return false;
        }
        if ((!force && now - this.mLastAttempt < this.mUploadInterval)
                || !this.shouldAttemptUpload(force)) {
            return false;
        }
        this.mDrainScheduled = true;
        // A disk barrier includes observations already waiting on the writer thread.
        this.mHandler.post(() -> this.mNetworkHandler.post(this::drainQueue));
        return true;
    }

    private synchronized void scheduleRetryAt(long deadline) {
        this.mNetworkHandler.removeCallbacks(this.mRetryUpload);
        if (this.mInitialized) {
            this.mNetworkHandler.postDelayed(this.mRetryUpload,
                    Math.max(0, deadline - System.currentTimeMillis()));
        }
    }

    private void drainQueue() {
        boolean failed = false;
        boolean more = false;
        ReentrantLock drainLock;
        synchronized (DRAIN_LOCKS) {
            String directory = this.getPendingFolder().getAbsolutePath();
            drainLock = DRAIN_LOCKS.get(directory);
            if (drainLock == null) {
                drainLock = new ReentrantLock();
                DRAIN_LOCKS.put(directory, drainLock);
            }
        }
        boolean ownsDrain = drainLock.tryLock();
        try {
            if (!ownsDrain) {
                more = true;
                return;
            }
            this.closeOpenSession();
            synchronized (ACTIVE_FILES) {
                this.recoverInterruptedFiles();
            }
            String[] names = this.getPendingFolder().list((dir, name) -> name.endsWith(JSON_EXTENSION));
            if (names == null) {
                return;
            }
            Arrays.sort(names);
            // Yield between batches, and stop on the first failed request.
            int count = Math.min(names.length, 2);
            for (int i = 0; i < count && this.mInitialized; i++) {
                File file = new File(this.getPendingFolder(), names[i]);
                if (!file.isFile()) {
                    continue;
                }
                if (file.length() > 8 * 1024 * 1024) {
                    quarantine(file, ERROR_FILE_EXTENSION);
                    this.recordFailure(new IOException("Upload bundle exceeds safe read size; retained for diagnosis"));
                    continue;
                }
                this.mLastAttempt = System.currentTimeMillis();
                PreferenceManager.getDefaultSharedPreferences(this.mContext).edit()
                        .putLong(LAST_TRANSMISSION_ATTEMPT, this.mLastAttempt).apply();
                String payload;
                // Commons IO's file reader calls File.toPath(), unavailable on API 24/25.
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                        new FileInputStream(file), java.nio.charset.StandardCharsets.UTF_8))) {
                    StringBuilder text = new StringBuilder();
                    char[] buffer = new char[8192];
                    int read;
                    while ((read = reader.read(buffer)) != -1) {
                        text.append(buffer, 0, read);
                        if (text.length() > 8 * 1024 * 1024) {
                            throw new IOException("Upload bundle grew beyond safe read size");
                        }
                    }
                    payload = text.toString();
                }
                try {
                    new JSONArray(payload);
                } catch (JSONException e) {
                    quarantine(file, ERROR_FILE_EXTENSION);
                    this.recordFailure(e);
                    continue;
                }
                if (this.transmitHttpPayload(payload) != RESULT_SUCCESS) {
                    failed = true;
                    break;
                }
                this.mTransmitted += file.length();
                if (!file.delete()) {
                    throw new IOException("Cannot remove acknowledged upload bundle");
                }
                PreferenceManager.getDefaultSharedPreferences(this.mContext).edit()
                        .putLong(LAST_SUCCESSFUL_TRANSMISSION, System.currentTimeMillis()).apply();
            }
            String[] eligible = this.getPendingFolder().list((dir, name) -> name.endsWith(JSON_EXTENSION));
            more = eligible != null && eligible.length > 0;
        } catch (IOException | RuntimeException e) {
            failed = true;
            this.recordFailure(e);
        } finally {
            if (ownsDrain) {
                drainLock.unlock();
            }
            ArrayList<Runnable> completions;
            synchronized (this) {
                this.mDrainScheduled = false;
                if (failed) {
                    this.mRetryDelay = Math.min(MAX_RETRY_DELAY, Math.max(MIN_RETRY_DELAY, this.mRetryDelay * 2));
                    this.mNextRetryAt = System.currentTimeMillis() + this.mRetryDelay;
                } else {
                    this.mRetryDelay = 0;
                    this.mNextRetryAt = 0;
                }
                PreferenceManager.getDefaultSharedPreferences(this.mContext).edit()
                        .putLong(this.retryPreferenceKey(), this.mNextRetryAt).apply();
                if (this.mInitialized && (failed || more)) {
                    this.scheduleRetryAt(System.currentTimeMillis()
                            + (failed ? this.mRetryDelay : (ownsDrain ? 1000L : 5000L)));
                }
                completions = new ArrayList<>(this.mDrainCompletions);
                this.mDrainCompletions.clear();
            }
            for (Runnable completion : completions) {
                completion.run();
            }
        }
    }

    public long lastTransmissionAttempt() {
        return this.mLastAttempt;
    }

    /** Keeps a JobService alive until this bounded drain (at most two 45-second calls) finishes. */
    public synchronized void transmitWithCompletion(boolean force, Runnable completion) {
        if (this.mDrainScheduled) {
            this.mDrainCompletions.add(completion);
        } else if (this.transmit(force)) {
            this.mDrainCompletions.add(completion);
        } else {
            completion.run();
        }
    }

    private String retryPreferenceKey() {
        return "pdk-upload-retry:" + this.mUploadUri + ":" + this.mUserId;
    }

    private void recordFailure(Exception error) {
        Log.e("PDK", "Upload queue operation failed; data retained", error);
        PreferenceManager.getDefaultSharedPreferences(this.mContext).edit()
                .putString(Transmitter.FAILURE_REASON, error.getClass().getSimpleName() + ": " + error.getMessage())
                .putLong(Transmitter.FAILURE_TIMESTAMP, System.currentTimeMillis()).apply();
    }

    private static void quarantine(File file, String suffix) throws IOException {
        File destination = new File(file.getAbsolutePath() + suffix);
        if (!file.renameTo(destination)) {
            throw new IOException("Unable to preserve interrupted upload file");
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private int transmitHttpPayload(String payload) {
        if (payload == null || payload.trim().length() == 0) {
            return HttpTransmitter.RESULT_SUCCESS;
        }

        SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(this.mContext);

        try {
            // Liberal HTTPS setup:
            // http://stackoverflow.com/questions/2012497/accepting-a-certificate-for-https-on-android

            HandshakeCertificates certificates = PassiveDataKit.getInstance(this.mContext).fetchTrustedCertificates();

            OkHttpClient client = new OkHttpClient.Builder()
                    .connectTimeout(15, TimeUnit.SECONDS)
                    .readTimeout(30, TimeUnit.SECONDS)
                    .writeTimeout(30, TimeUnit.SECONDS)
                    .callTimeout(45, TimeUnit.SECONDS)
                    .sslSocketFactory(certificates.sslSocketFactory(), certificates.trustManager())
                    .build();

            if (!this.mStrictSsl) {
                // Create a trust manager that does not validate certificate chains
                final TrustManager[] trustAllCerts = new TrustManager[] {
                        new X509TrustManager() {
                            @SuppressLint("TrustAllX509TrustManager")
                            @Override
                            public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) throws CertificateException {
                            }

                            @SuppressLint("TrustAllX509TrustManager")
                            @Override
                            public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) throws CertificateException {
                            }

                            @Override
                            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                                return new java.security.cert.X509Certificate[]{};
                            }
                        }
                };

                // Install the all-trusting trust manager
                final SSLContext sslContext = SSLContext.getInstance("SSL");
                sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
                // Create an ssl socket factory with our all-trusting manager
                final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

                client = new OkHttpClient.Builder()
                        .connectTimeout(15, TimeUnit.SECONDS)
                        .readTimeout(30, TimeUnit.SECONDS)
                        .writeTimeout(30, TimeUnit.SECONDS)
                        .callTimeout(45, TimeUnit.SECONDS)
                        .sslSocketFactory(sslSocketFactory, (X509TrustManager) trustAllCerts[0])
                        .build();
            }

            MultipartBody.Builder builder = new MultipartBody.Builder().setType(MultipartBody.FORM);

            ArrayList<File> toDelete = new ArrayList<>();

            if (payload.contains("\"" + Generator.MEDIA_ATTACHMENT_KEY + "\":")) {
                JSONArray payloadJson = new JSONArray(payload);

                for (int i = 0; i < payloadJson.length(); i++) {
                    JSONObject reading = payloadJson.getJSONObject(i);

                    if (reading.has(Generator.MEDIA_ATTACHMENT_KEY)) {
                        Uri u = Uri.parse(reading.getString(Generator.MEDIA_ATTACHMENT_KEY));

                        String mimeType = "application/octet-stream";

                        if (reading.has(Generator.MEDIA_CONTENT_TYPE_KEY)) {
                            mimeType = reading.getString(Generator.MEDIA_CONTENT_TYPE_KEY);
                        }

                        String guid = reading.getString(Generator.MEDIA_ATTACHMENT_GUID_KEY);
                        File file = new File(u.getPath());

                        if (file.exists()) {
                            builder = builder.addFormDataPart(guid, file.getName(), RequestBody.create(MediaType.parse(mimeType), file));
                            toDelete.add(file);
                        }
                    }
                }
            }

            boolean compressed = false;

            String payloadString = (new JSONArray(payload)).toString();

            if (this.mCompressPayloads) {
                // Compress payload string

                try {
                    byte[] input = payloadString.getBytes("UTF-8");

                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    GZIPOutputStream gzip = new GZIPOutputStream(baos);

                    gzip.write(input, 0, input.length);
                    gzip.flush();

                    gzip.close();
                    baos.close();

                    byte[] compressedBytes = baos.toByteArray();

                    payloadString = Toolbox.encodeBase64(compressedBytes);

                    compressed = true;
                } catch (java.io.UnsupportedEncodingException ex) {
                    // handle
                }
            }

            if (this.mPublicKey != null) {
                byte[] nonce = Toolbox.randomNonce();

                String encryptedString = Toolbox.encrypt(this.mPrivateKey, this.mPublicKey, nonce, payloadString);

                builder = builder.addPart(Headers.of("Content-Disposition", "form-data; name=\"payload\""), RequestBody.create(null, encryptedString));
                builder = builder.addPart(Headers.of("Content-Disposition", "form-data; name=\"encrypted\""), RequestBody.create(null, "true"));
                builder = builder.addPart(Headers.of("Content-Disposition", "form-data; name=\"nonce\""), RequestBody.create(null, Toolbox.encodeBase64(nonce)));
            } else {
                builder = builder.addPart(Headers.of("Content-Disposition", "form-data; name=\"payload\""), RequestBody.create(null, payloadString));
            }

            if (compressed) {
                builder = builder.addPart(Headers.of("Content-Disposition", "form-data; name=\"compression\""), RequestBody.create(null, HttpTransmitter.COMPRESSION_GZIP));
            } else {
                builder = builder.addPart(Headers.of("Content-Disposition", "form-data; name=\"compression\""), RequestBody.create(null, HttpTransmitter.COMPRESSION_NONE));
            }

            RequestBody requestBody = builder.build();

            if (this.mUserAgent == null) {
                String version = this.mContext.getPackageManager().getPackageInfo(this.mContext.getPackageName(), 0).versionName;
                String appName = this.mContext.getString(this.mContext.getApplicationInfo().labelRes);

                this.mUserAgent = appName + " " + version;
            }

            Request request = new Request.Builder()
                    .removeHeader("User-Agent")
                    .addHeader("User-Agent", this.mUserAgent)
                    .url(this.mUploadUri.toString())
                    .post(requestBody)
                    .build();

            int code;
            String bodyString;
            try (Response response = client.newCall(request).execute()) {
                code = response.code();
                ResponseBody body = response.body();
                bodyString = body == null ? "" : body.string();
            }

            if (code >= 200 && code < 300) {
                JSONObject responseObject = new JSONObject(bodyString);

                if (responseObject.has("added") && responseObject.getBoolean("added")) {
                    for (File f : toDelete) {
                        f.delete();
                    }

                    SharedPreferences.Editor e = prefs.edit();
                    e.remove(Transmitter.FAILURE_REASON);
                    e.remove(Transmitter.FAILURE_TIMESTAMP);
                    e.apply();

                    return HttpTransmitter.RESULT_SUCCESS;
                }
            }
            SharedPreferences.Editor e = prefs.edit();
            e.putString(Transmitter.FAILURE_REASON, this.mContext.getString(R.string.transmission_failed_server_code, code));
            e.putLong(Transmitter.FAILURE_TIMESTAMP, System.currentTimeMillis());
            e.apply();
        } catch (UnknownHostException ex) {
            SharedPreferences.Editor e = prefs.edit();
            e.putString(Transmitter.FAILURE_REASON, this.mContext.getString(R.string.transmission_failed_unknown_host, this.mUploadUri.getHost()));
            e.putLong(Transmitter.FAILURE_TIMESTAMP, System.currentTimeMillis());
            e.apply();
        } catch (Exception | LinkageError ex) {
            ex.printStackTrace();

            SharedPreferences.Editor e = prefs.edit();
            e.putString(Transmitter.FAILURE_REASON, this.mContext.getString(R.string.transmission_failed_exception, ex.getClass().toString(), ex.getLocalizedMessage()));
            e.putLong(Transmitter.FAILURE_TIMESTAMP, System.currentTimeMillis());
            e.apply();
        }

        return HttpTransmitter.RESULT_ERROR;
    }

    protected synchronized void closeOpenSession() throws IOException {
        if (this.mCurrentFile == null) {
            return;
        }
        File temporary = this.mCurrentFile;
        if (this.mJsonGenerator != null) {
            JsonGenerator writer = this.mJsonGenerator;
            this.mJsonGenerator = null;
            try {
                writer.writeEndArray();
                writer.close();
            } catch (IOException | RuntimeException error) {
                try { writer.close(); } catch (IOException ignored) { /* Preserve original failure. */ }
                synchronized (ACTIVE_FILES) {
                    ACTIVE_FILES.remove(temporary.getAbsolutePath());
                }
                // Keep interrupted bytes for recovery, but let a fresh writer make progress.
                this.mCurrentFile = null;
                this.mCurrentReadingCount = 0;
                this.mCurrentBytesWritten = 0;
                throw new IOException("Unable to finalize upload JSON; interrupted file retained", error);
            }
        }
        if (!temporary.isFile()) {
            synchronized (ACTIVE_FILES) {
                ACTIVE_FILES.remove(temporary.getAbsolutePath());
            }
            this.mCurrentFile = null;
            this.mCurrentReadingCount = 0;
            this.mCurrentBytesWritten = 0;
            throw new IOException("Owned upload file disappeared before finalization");
        }
        try (FileOutputStream output = new FileOutputStream(temporary, true)) {
            output.getFD().sync();
        }
        File destination = new File(temporary.getAbsolutePath().replace(TEMP_EXTENSION, JSON_EXTENSION));
        if (!temporary.renameTo(destination)) {
            // Retain mCurrentFile so the next finalization retries the same file.
            throw new IOException("Cannot publish upload bundle");
        }
        synchronized (ACTIVE_FILES) {
            ACTIVE_FILES.remove(temporary.getAbsolutePath());
        }
        this.mCurrentFile = null;
        this.mCurrentReadingCount = 0;
        this.mCurrentBytesWritten = 0;
    }

    private Bundle withMetadata(String identifier, long timestamp, Bundle data) {
        Bundle copy = new Bundle(data);
        Bundle metadata = data.getBundle(Generator.PDK_METADATA);
        metadata = metadata == null ? new Bundle() : new Bundle(metadata);
        metadata.putString(Generator.IDENTIFIER, identifier);
        metadata.putDouble(Generator.TIMESTAMP, timestamp / 1000.0);
        metadata.putString(Generator.GENERATOR, Generators.getInstance(this.mContext).getGeneratorFullName(identifier));
        metadata.putString(Generator.SOURCE, this.mUserId);
        TimeZone zone = TimeZone.getDefault();
        metadata.putString(Generator.TIMEZONE, zone.getID());
        metadata.putInt(Generator.TIMEZONE_OFFSET, zone.getOffset(timestamp) / 1000);
        copy.putBundle(Generator.PDK_METADATA, metadata);
        return copy;
    }

    @Override
    public synchronized boolean enqueueGeneratorUpdates(String identifier, List<Bundle> updates) {
        if (!this.mInitialized || updates.isEmpty()) {
            return false;
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (JsonGenerator writer = new JsonFactory().createGenerator(bytes)) {
                writer.writeStartArray();
                for (Bundle update : updates) {
                    writeBundle(this.mContext, writer, this.withMetadata(identifier,
                            update.getLong("observed", System.currentTimeMillis()), update));
                }
                writer.writeEndArray();
            }
            this.publishBundle(bytes.toByteArray());
            return true;
        } catch (IOException | RuntimeException error) {
            this.recordFailure(error);
            return false;
        }
    }

    private File publishBundle(byte[] bytes) throws IOException {
        File temporary = new File(this.getPendingFolder(), System.currentTimeMillis() + "-" + UUID.randomUUID() + TEMP_EXTENSION);
        synchronized (ACTIVE_FILES) {
            ACTIVE_FILES.add(temporary.getAbsolutePath());
        }
        try {
            try (FileOutputStream output = new FileOutputStream(temporary)) {
                output.write(bytes);
                output.getFD().sync();
            }
            File destination = new File(temporary.getAbsolutePath().replace(TEMP_EXTENSION, JSON_EXTENSION));
            if (!temporary.renameTo(destination)) {
                throw new IOException("Cannot publish durable upload bundle");
            }
            return destination;
        } finally {
            synchronized (ACTIVE_FILES) {
                ACTIVE_FILES.remove(temporary.getAbsolutePath());
            }
        }
    }

    /** Salvage complete records; keep the original interrupted bytes for diagnosis. */
    private void recoverInterruptedFiles() {
        File[] interrupted = this.getPendingFolder().listFiles((dir, name) -> name.endsWith(TEMP_EXTENSION));
        if (interrupted == null) {
            return;
        }
        for (File file : interrupted) {
            if (ACTIVE_FILES.contains(file.getAbsolutePath())) {
                continue;
            }
            try {
                if (file.length() > 8 * 1024 * 1024) {
                    quarantine(file, ERROR_FILE_EXTENSION);
                    continue;
                }
                JSONArray records = new JSONArray();
                ObjectMapper mapper = new ObjectMapper();
                try (com.fasterxml.jackson.core.JsonParser parser = new JsonFactory().createParser(file)) {
                    if (parser.nextToken() == com.fasterxml.jackson.core.JsonToken.START_ARRAY) {
                        while (parser.nextToken() == com.fasterxml.jackson.core.JsonToken.START_OBJECT) {
                            com.fasterxml.jackson.databind.JsonNode node = mapper.readTree(parser);
                            records.put(new JSONObject(node.toString()));
                        }
                    }
                } catch (IOException | JSONException malformedTail) {
                    Log.w("PDK", "Interrupted upload has an incomplete tail; retaining original", malformedTail);
                }
                if (records.length() > 0) {
                    this.publishBundle(records.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    quarantine(file, ".recovered");
                } else {
                    quarantine(file, ERROR_FILE_EXTENSION);
                }
            } catch (IOException | RuntimeException error) {
                this.recordFailure(error);
            }
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    protected File getPendingFolder() {
        File internalStorage = this.mContext.getFilesDir();

        if (internalStorage == null || this.mUseExternalStorage) {
            if (Environment.MEDIA_MOUNTED.equals(Environment.getExternalStorageState())) {
                internalStorage = this.mContext.getExternalFilesDir(null);
            }
        }

        if (internalStorage != null && !internalStorage.exists()) {
            internalStorage.mkdirs();
        }

        File pendingFolder = new File(internalStorage, this.mFolderName);

        if (!pendingFolder.exists()) {
            pendingFolder.mkdirs();
        }

        return pendingFolder;
    }

    protected static long getFileSize(final File file) {
        if (file == null || !file.exists()) {
            return 0;
        }

        if (!file.isDirectory()) {
            return file.length();
        }

        final List<File> dirs = new LinkedList<>();

        dirs.add(file);

        long result = 0;

        while (!dirs.isEmpty()) {
            final File dir = dirs.remove(0);

            if (!dir.exists()) {
                continue;
            }

            final File[] listFiles = dir.listFiles();

            if (listFiles == null || listFiles.length == 0) {
                continue;
            }

            for (final File child : listFiles) {
                result += child.length();

                if (child.isDirectory()) {
                    dirs.add(child);
                }
            }
        }

        return result;
    }

    @Override
    public long pendingSize() {
        return HttpTransmitter.getFileSize(this.getPendingFolder());
    }

    @Override
    public long pendingTransmissions() {
        File pendingFolder = this.getPendingFolder();

        if (pendingFolder == null || !pendingFolder.exists()) {
            return 0;
        }

        final List<File> dirs = new LinkedList<>();

        dirs.add(pendingFolder);

        long result = 0;

        while (!dirs.isEmpty()) {
            final File dir = dirs.remove(0);

            if (!dir.exists()) {
                continue;
            }

            final File[] listFiles = dir.listFiles();

            if (listFiles == null || listFiles.length == 0) {
                continue;
            }

            for (final File child : listFiles) {
                if (child.isDirectory()) {
                    dirs.add(child);
                } else {
                    if (child.getAbsolutePath().endsWith(HttpTransmitter.JSON_EXTENSION)) {
                        result += 1;
                    } if (child.getAbsolutePath().endsWith(HttpTransmitter.TOO_LARGE_FILE_EXTENSION)) {
                        result += 1;
                    }
                }
            }
        }

        return result;
    }

    @Override
    public long transmittedSize() {
        return this.mTransmitted;
    }

    @Override
    public void onGeneratorUpdated(final String identifier, final long timestamp, Bundle data) {
        if (!this.mInitialized) {
            return;
        }
        final Bundle copy = new Bundle(data);
        this.mHandler.post(() -> {
            synchronized (this) {
                try {
                    if (copy.isEmpty()) {
                        return;
                    }
                    if (this.mCurrentReadingCount >= this.mMaxReadingCount
                            || this.mCurrentBytesWritten >= 256 * 1024
                            || (this.mCurrentFile != null && this.mJsonGenerator == null)) {
                        this.closeOpenSession();
                    }
                    if (this.mCurrentFile == null) {
                        this.mCurrentFile = new File(this.getPendingFolder(),
                                System.currentTimeMillis() + "-" + UUID.randomUUID() + TEMP_EXTENSION);
                        synchronized (ACTIVE_FILES) {
                            ACTIVE_FILES.add(this.mCurrentFile.getAbsolutePath());
                        }
                        this.mJsonGenerator = new JsonFactory().createGenerator(this.mCurrentFile, JsonEncoding.UTF8);
                        this.mJsonGenerator.writeStartArray();
                    }
                    writeBundle(this.mContext, this.mJsonGenerator, this.withMetadata(identifier, timestamp, copy));
                    // Leave complete records visible to interrupted-file recovery after process death.
                    this.mJsonGenerator.flush();
                    this.mCurrentReadingCount++;
                    this.mCurrentBytesWritten = (int) this.mCurrentFile.length();
                } catch (IOException | RuntimeException error) {
                    this.recordFailure(error);
                }
            }
        });
    }

    private static Map<String, Object> getValues(Context context, final Bundle bundle) {
        HashMap<String, Object> values = new HashMap<>();

        if (bundle == null) {
            return values;
        }

        try {
            for (String key : bundle.keySet()) {
                values.put(key, bundle.get(key));
            }
        } catch (BadParcelableException e) {
            Logger.getInstance(context).logThrowable(e);
        }

        return values;
    }

    @SuppressWarnings("unchecked")
    protected static void writeBundle(Context context, JsonGenerator generator, Bundle bundle) {
        try {
            generator.writeStartObject();

            Map<String, Object> values = HttpTransmitter.getValues(context, bundle);

            for (String key : values.keySet()) {
                Object value = values.get(key);

                if (value != null && key != null) {
                    if (value instanceof String) {
                        generator.writeStringField(key, (String) value);
                    }
                    else if (value instanceof float[]) {
                        float[] floats = (float[]) value;

                        generator.writeArrayFieldStart(key);

                        for (float f : floats) {
                            generator.writeNumber(f);
                        }

                        generator.writeEndArray();
                    }
                    else if (value instanceof int[]) {
                        int[] ints = (int[]) value;

                        generator.writeArrayFieldStart(key);

                        for (int i : ints) {
                            generator.writeNumber(i);
                        }

                        generator.writeEndArray();
                    }
                    else if (value instanceof long[]) {
                        long[] longs = (long[]) value;

                        generator.writeArrayFieldStart(key);

                        for (long l : longs) {
                            generator.writeNumber(l);
                        }

                        generator.writeEndArray();
                    }
                    else if (value instanceof double[]) {
                        double[] doubles = (double[]) value;

                        generator.writeArrayFieldStart(key);

                        for (double d : doubles) {
                            generator.writeNumber(d);
                        }

                        generator.writeEndArray();
                    }
                    else if (value instanceof Float) {
                        Float f = (Float) value;

                        generator.writeNumberField(key, f);
                    }
                    else if (value instanceof Integer) {
                        Integer i = (Integer) value;

                        generator.writeNumberField(key, i);
                    }
                    else if (value instanceof Long) {
                        Long l = (Long) value;

                        generator.writeNumberField(key, l);
                    }
                    else if (value instanceof Boolean) {
                        Boolean b = (Boolean) value;

                        generator.writeBooleanField(key, b);
                    }
                    else if (value instanceof Short) {
                        Short s = (Short) value;

                        generator.writeNumberField(key, s);
                    }
                    else if (value instanceof Double) {
                        Double d = (Double) value;

                        if (d.isInfinite()) {
                            generator.writeNumberField(key, Double.MAX_VALUE);
                        }
                        else {
                            generator.writeNumberField(key, d);
                        }
                    }
                    else if (value instanceof List) {
                        List<Object> list = (List<Object>) value;

                        generator.writeArrayFieldStart(key);

                        for (Object o : list) {
                            if (o instanceof String) {
                                generator.writeString(o.toString());
                            }
                            else if (o instanceof Bundle) {
                                HttpTransmitter.writeBundle(context, generator, (Bundle) o);
                            }
                            else {
                                // Log.e("PDK", "LIST OBJ: " + o.getClass().getCanonicalName() + " IN " + key);
                            }
                        }

                        generator.writeEndArray();
                    }
                    else if (value instanceof Bundle) {
                        generator.writeFieldName(key);
                        HttpTransmitter.writeBundle(context, generator, (Bundle) value);
                    }
                    else {
                        // Log.e("PDK", "GOT TYPE " + value.getClass().getCanonicalName() + " FOR " + key);
                    }
                }
            }

            generator.writeEndObject();
        } catch (Exception e) {
            // Avoid reporting through the generator bus that is itself failing to serialize.
            Log.e("PDK", "Unable to serialize upload observation", e);
            throw new IllegalStateException("Unable to serialize upload observation", e);
        }
    }

    public void setUserId(String userId) {
        this.mUserId = userId;
    }

    public void setChargingOnly(boolean onlyCharging) {
        this.mChargingOnly = onlyCharging;
    }

    public void setWiFiOnly(boolean onlyWiFi) {
        this.mWifiOnly = onlyWiFi;
    }

    public void setPublicKey(byte[] publicKey) {
        this.mPublicKey = publicKey;
    }

    public void setPublicKey(String publicKey) {
        this.mPublicKey = Toolbox.decodeBase64(publicKey);
    }

    public void setPrivateKey(String privateKey) {
        this.mPrivateKey = Toolbox.decodeBase64(privateKey);
    }

    public void setMaxBundleSize(int size) {
        this.mMaxReadingCount = size;

        this.transmit(true);
    }

    @SuppressWarnings("unused")
    public static class IncompleteConfigurationException extends RuntimeException {
        public IncompleteConfigurationException(String message) {
            super(message);
        }
    }

    public long lastSuccessfulTransmission() {
        SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(this.mContext);

        return prefs.getLong(HttpTransmitter.LAST_SUCCESSFUL_TRANSMISSION, 0);
    }

    @Override
    public void testTransmission(Handler handler, boolean includeLocation, Runnable success, Runnable failure) {
        final HttpTransmitter me = this;

        Runnable r = new Runnable() {
            @Override
            public void run() {
                try {
                    String testPayload = me.generateTestPayload(includeLocation);

                    int result = me.transmitHttpPayload(testPayload);

                    if (result == HttpTransmitter.RESULT_SUCCESS) {
                        handler.post(success);
                    } else {
                        handler.post(failure);
                    }
                } catch (JSONException e) {
                    e.printStackTrace();

                    handler.post(failure);
                }
            }
        };

        this.mNetworkHandler.post(r);
    }

    private String generateTestPayload(boolean includeLocation) throws JSONException {
        JSONObject testReading = new JSONObject();

        JSONObject metadata = new JSONObject();

        long timestamp = System.currentTimeMillis();

        metadata.put(Generator.IDENTIFIER, "pdk-connection-test");
        metadata.put(Generator.TIMESTAMP, timestamp / 1000);
        metadata.put(Generator.GENERATOR, Generators.getInstance(this.mContext).getGeneratorFullName("pdk-connection-test"));
        metadata.put(Generator.SOURCE, this.mUserId);

        TimeZone timeZone = TimeZone.getDefault();

        metadata.put(Generator.TIMEZONE, timeZone.getID());
        metadata.put(Generator.TIMEZONE_OFFSET, timeZone.getOffset(timestamp) / 1000);

        /*
        if (includeLocation) {
            android.location.Location location = Location.getInstance(this.mContext).getLastKnownLocation();

            if (location != null) {
                testReading.put(Location.LATITUDE, location.getLatitude());
                testReading.put(Location.LONGITUDE, location.getLongitude());
                testReading.put(Location.HISTORY_MOCK_LOCATION_PROVIDER, location.isFromMockProvider());
            }

            JSONArray mockLocationApps = new JSONArray();
            HashSet<String> seenApps = new HashSet<>();

            PackageManager pm = this.mContext.getPackageManager();
            List<ApplicationInfo> packages = pm.getInstalledApplications(PackageManager.GET_META_DATA);

            for (ApplicationInfo applicationInfo : packages) {
                try {
                    PackageInfo packageInfo = pm.getPackageInfo(applicationInfo.packageName, PackageManager.GET_PERMISSIONS);

                    String[] requestedPermissions = packageInfo.requestedPermissions;

                    if (requestedPermissions != null) {
                        for (int i = 0; i < requestedPermissions.length; i++) {
                            if (requestedPermissions[i].equals("android.permission.ACCESS_MOCK_LOCATION")) {
                                if (seenApps.contains(applicationInfo.packageName) == false) {
                                    mockLocationApps.put(applicationInfo.packageName);

                                    seenApps.add(applicationInfo.packageName);
                                }
                            }
                        }
                    }
                } catch (PackageManager.NameNotFoundException e) {
                    Log.e("PDK" , "MOCK LOCATION STACK TRACE", e);
                }
            }

            testReading.put(Location.HISTORY_MOCK_LOCATION_APPS_COUNT, mockLocationApps.length());
            testReading.put(Location.HISTORY_MOCK_LOCATION_APPS, mockLocationApps);
        }

         */

        testReading.put("passive-data-metadata", metadata);

        JSONArray payload = new JSONArray();
        payload.put(testReading);

        return payload.toString(2);
    }
}
