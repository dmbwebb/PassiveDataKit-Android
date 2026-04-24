package com.audacious_software.passive_data_kit.generators.device;

import android.annotation.SuppressLint;
import android.app.AppOpsManager;
import android.app.Service;
import android.app.usage.UsageStats;
import android.app.usage.UsageStatsManager;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.os.Build;
import android.os.Bundle;
import android.preference.PreferenceManager;
import android.provider.Settings;
import android.util.Log;

import com.audacious_software.passive_data_kit.PassiveDataKit;
import com.audacious_software.passive_data_kit.R;
import com.audacious_software.passive_data_kit.diagnostics.DiagnosticAction;
import com.audacious_software.passive_data_kit.generators.Generator;
import com.audacious_software.passive_data_kit.generators.Generators;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Daily per-app usage aggregate generator.
 *
 * Uses UsageStatsManager.queryUsageStats(INTERVAL_DAILY) which has ~2-year retention
 * on Samsung (and all AOSP devices), unlike queryEvents which is purged after 7 days.
 *
 * Safety net for when Samsung Device Care / OPPO App Freeze puts the app into Deep Sleep
 * and our other polling generators miss data.
 */
public class DailyUsageAggregateGenerator extends Generator {
    private static final String GENERATOR_IDENTIFIER = "pdk-daily-usage-aggregate";
    private static final int DATABASE_VERSION = 1;
    private static final String DATABASE_PATH = "pdk-daily-usage-aggregate.sqlite";

    private static final String ENABLED = "com.audacious_software.passive_data_kit.generators.device.DailyUsageAggregateGenerator.ENABLED";
    private static final boolean ENABLED_DEFAULT = true;

    private static final String LOOKBACK_DAYS = "com.audacious_software.passive_data_kit.generators.device.DailyUsageAggregateGenerator.LOOKBACK_DAYS";
    private static final int LOOKBACK_DAYS_DEFAULT = 30;

    private static final String SAMPLE_INTERVAL = "com.audacious_software.passive_data_kit.generators.device.DailyUsageAggregateGenerator.SAMPLE_INTERVAL";
    private static final long SAMPLE_INTERVAL_DEFAULT = 24L * 60L * 60L * 1000L; // once per day

    private static final String DATA_RETENTION_PERIOD = "com.audacious_software.passive_data_kit.generators.device.DailyUsageAggregateGenerator.DATA_RETENTION_PERIOD";
    private static final long DATA_RETENTION_PERIOD_DEFAULT = (365L * 24L * 60L * 60L * 1000L);

    private static final String LAST_CONFIGURATION = "com.audacious_software.passive_data_kit.generators.device.DailyUsageAggregateGenerator.LAST_CONFIGURATION";

    private static final String TABLE_HISTORY = "history";
    private static final String HISTORY_OBSERVED = "observed";
    private static final String HISTORY_DAY_BUCKET = "day_bucket";
    private static final String HISTORY_PACKAGE = "package";
    private static final String HISTORY_TOTAL_MS = "total_ms";
    private static final String HISTORY_FIRST_TIMESTAMP = "first_timestamp";
    private static final String HISTORY_LAST_TIMESTAMP = "last_timestamp";
    private static final String HISTORY_LAST_TIME_USED = "last_time_used";

    private static DailyUsageAggregateGenerator sInstance;
    private final SQLiteDatabase mDatabase;
    private ScheduledThreadPoolExecutor mService;

    @Override
    public String getIdentifier() {
        return DailyUsageAggregateGenerator.GENERATOR_IDENTIFIER;
    }

    public static synchronized DailyUsageAggregateGenerator getInstance(Context context) {
        if (DailyUsageAggregateGenerator.sInstance == null) {
            DailyUsageAggregateGenerator.sInstance = new DailyUsageAggregateGenerator(context.getApplicationContext());
        }

        return DailyUsageAggregateGenerator.sInstance;
    }

    @SuppressWarnings("WeakerAccess")
    public DailyUsageAggregateGenerator(Context context) {
        super(context);

        synchronized (context.getApplicationContext()) {
            File path = PassiveDataKit.getGeneratorsStorage(this.mContext);

            path = new File(path, DailyUsageAggregateGenerator.DATABASE_PATH);

            this.mDatabase = SQLiteDatabase.openOrCreateDatabase(path, null);

            int version = this.getDatabaseVersion(this.mDatabase);

            switch (version) {
                case 0:
                    this.mDatabase.execSQL(this.mContext.getString(R.string.pdk_generator_daily_usage_aggregate_create_history_table));
            }

            if (version != DailyUsageAggregateGenerator.DATABASE_VERSION) {
                this.setDatabaseVersion(this.mDatabase, DailyUsageAggregateGenerator.DATABASE_VERSION);
            }
        }
    }

    public static void start(final Context context) {
        SharedPreferences prefs = Generators.getInstance(context).getSharedPreferences(context);

        if (prefs.getBoolean(DailyUsageAggregateGenerator.ENABLED, DailyUsageAggregateGenerator.ENABLED_DEFAULT)) {
            DailyUsageAggregateGenerator.getInstance(context).startGenerator();
        }
    }

    private void startGenerator() {
        if (this.mService != null) {
            return;
        }

        final DailyUsageAggregateGenerator me = this;

        this.mService = new ScheduledThreadPoolExecutor(1);

        SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(this.mContext);
        final long sampleInterval = prefs.getLong(DailyUsageAggregateGenerator.SAMPLE_INTERVAL, DailyUsageAggregateGenerator.SAMPLE_INTERVAL_DEFAULT);

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    me.runAggregation();
                } catch (Throwable t) {
                    Log.e("PDK", "DailyUsageAggregate failure", t);
                }

                if (me.mService != null && !me.mService.isShutdown()) {
                    me.mService.schedule(this, sampleInterval, TimeUnit.MILLISECONDS);
                }
            }
        };

        // Run shortly after start, then every sampleInterval ms
        this.mService.schedule(runnable, 10000, TimeUnit.MILLISECONDS);
    }

    private void stopGenerator() {
        if (this.mService == null) {
            return;
        }

        this.mService.shutdown();

        this.mService = null;
    }

    /**
     * Query queryUsageStats(INTERVAL_DAILY) for the last N days and insert
     * one row per (day_bucket, package) with totalTimeInForeground.
     *
     * Visible to tests.
     */
    void runAggregation() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.LOLLIPOP) {
            return;
        }

        UsageStatsManager usageStatsManager = (UsageStatsManager) this.mContext.getSystemService(Service.USAGE_STATS_SERVICE);

        if (usageStatsManager == null) {
            return;
        }

        SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(this.mContext);
        int lookbackDays = prefs.getInt(DailyUsageAggregateGenerator.LOOKBACK_DAYS, DailyUsageAggregateGenerator.LOOKBACK_DAYS_DEFAULT);

        if (lookbackDays <= 0) {
            return;
        }

        long now = System.currentTimeMillis();

        // Walk day-by-day at local-midnight boundaries.
        // day_bucket = epoch ms at local midnight at start of day.
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(now);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        long todayBucket = cal.getTimeInMillis();

        for (int i = 0; i < lookbackDays; i++) {
            long dayStart = todayBucket - (long) i * 24L * 60L * 60L * 1000L;
            long dayEnd = dayStart + 24L * 60L * 60L * 1000L;

            // For today, query up to "now" so we see in-progress usage.
            long queryEnd = (i == 0) ? now : dayEnd;

            List<UsageStats> stats;
            try {
                stats = usageStatsManager.queryUsageStats(UsageStatsManager.INTERVAL_DAILY, dayStart, queryEnd);
            } catch (Throwable t) {
                Log.e("PDK", "queryUsageStats failed for day " + dayStart, t);
                continue;
            }

            if (stats == null || stats.isEmpty()) {
                continue;
            }

            for (UsageStats us : stats) {
                String packageName = us.getPackageName();

                if (packageName == null) {
                    continue;
                }

                long totalMs = us.getTotalTimeInForeground();

                if (totalMs <= 0) {
                    continue;
                }

                long firstTs = us.getFirstTimeStamp();
                long lastTs = us.getLastTimeStamp();
                long lastTimeUsed = us.getLastTimeUsed();

                // Dedup: if a row already exists for this (day_bucket, package) with the
                // SAME total_ms, skip inserting/notifying. Otherwise the ON CONFLICT REPLACE
                // will update the row.
                if (rowUnchanged(dayStart, packageName, totalMs)) {
                    continue;
                }

                ContentValues values = new ContentValues();
                values.put(DailyUsageAggregateGenerator.HISTORY_OBSERVED, now);
                values.put(DailyUsageAggregateGenerator.HISTORY_DAY_BUCKET, dayStart);
                values.put(DailyUsageAggregateGenerator.HISTORY_PACKAGE, packageName);
                values.put(DailyUsageAggregateGenerator.HISTORY_TOTAL_MS, totalMs);
                values.put(DailyUsageAggregateGenerator.HISTORY_FIRST_TIMESTAMP, firstTs);
                values.put(DailyUsageAggregateGenerator.HISTORY_LAST_TIMESTAMP, lastTs);
                values.put(DailyUsageAggregateGenerator.HISTORY_LAST_TIME_USED, lastTimeUsed);

                this.mDatabase.insertWithOnConflict(DailyUsageAggregateGenerator.TABLE_HISTORY, null, values, SQLiteDatabase.CONFLICT_REPLACE);

                Bundle update = new Bundle();
                update.putLong(DailyUsageAggregateGenerator.HISTORY_OBSERVED, now);
                update.putLong(DailyUsageAggregateGenerator.HISTORY_DAY_BUCKET, dayStart);
                update.putString(DailyUsageAggregateGenerator.HISTORY_PACKAGE, packageName);
                update.putLong(DailyUsageAggregateGenerator.HISTORY_TOTAL_MS, totalMs);
                update.putLong(DailyUsageAggregateGenerator.HISTORY_FIRST_TIMESTAMP, firstTs);
                update.putLong(DailyUsageAggregateGenerator.HISTORY_LAST_TIMESTAMP, lastTs);
                update.putLong(DailyUsageAggregateGenerator.HISTORY_LAST_TIME_USED, lastTimeUsed);

                Generators.getInstance(this.mContext).notifyGeneratorUpdated(DailyUsageAggregateGenerator.GENERATOR_IDENTIFIER, update);
            }
        }
    }

    private boolean rowUnchanged(long dayBucket, String packageName, long totalMs) {
        String where = DailyUsageAggregateGenerator.HISTORY_DAY_BUCKET + " = ? AND " + DailyUsageAggregateGenerator.HISTORY_PACKAGE + " = ?";
        String[] args = { String.valueOf(dayBucket), packageName };

        Cursor c = this.mDatabase.query(DailyUsageAggregateGenerator.TABLE_HISTORY,
                new String[] { DailyUsageAggregateGenerator.HISTORY_TOTAL_MS },
                where, args, null, null, null);

        boolean unchanged = false;
        if (c.moveToFirst()) {
            long existing = c.getLong(0);
            unchanged = (existing == totalMs);
        }
        c.close();

        return unchanged;
    }

    public static boolean isEnabled(Context context) {
        SharedPreferences prefs = Generators.getInstance(context).getSharedPreferences(context);

        return prefs.getBoolean(DailyUsageAggregateGenerator.ENABLED, DailyUsageAggregateGenerator.ENABLED_DEFAULT);
    }

    @SuppressWarnings({"unused"})
    public static boolean isRunning(Context context) {
        if (DailyUsageAggregateGenerator.sInstance == null) {
            return false;
        }

        return DailyUsageAggregateGenerator.sInstance.mService != null;
    }

    @SuppressLint("InlinedApi")
    public static ArrayList<DiagnosticAction> diagnostics(final Context context) {
        ArrayList<DiagnosticAction> actions = new ArrayList<>();

        if (DailyUsageAggregateGenerator.hasPermissions(context) == false) {
            actions.add(new DiagnosticAction(context.getString(R.string.diagnostic_usage_stats_permission_required_title), context.getString(R.string.diagnostic_usage_stats_permission_required), new Runnable() {
                @Override
                public void run() {
                    DailyUsageAggregateGenerator.fetchPermissions(context);
                }
            }));
        }

        return actions;
    }

    public static void fetchPermissions(final Context context) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.LOLLIPOP) {
            Intent intent = new Intent(Settings.ACTION_USAGE_ACCESS_SETTINGS);
            intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
            context.startActivity(intent);
        }
    }

    public static boolean hasPermissions(final Context context) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.LOLLIPOP) {
            AppOpsManager appOps = (AppOpsManager) context.getSystemService(Context.APP_OPS_SERVICE);
            int mode = appOps.checkOpNoThrow("android:get_usage_stats", android.os.Process.myUid(), context.getPackageName());

            if (mode != AppOpsManager.MODE_ALLOWED) {
                return false;
            }
        }

        return true;
    }

    public static String getGeneratorTitle(Context context) {
        return context.getString(R.string.generator_usage_stats_application);
    }

    protected void flushCachedData() {
        SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(this.mContext);

        long retentionPeriod = prefs.getLong(DailyUsageAggregateGenerator.DATA_RETENTION_PERIOD, DailyUsageAggregateGenerator.DATA_RETENTION_PERIOD_DEFAULT);

        long cutoff = System.currentTimeMillis() - retentionPeriod;

        String where = DailyUsageAggregateGenerator.HISTORY_DAY_BUCKET + " < ?";
        String[] args = { String.valueOf(cutoff) };

        this.mDatabase.delete(DailyUsageAggregateGenerator.TABLE_HISTORY, where, args);
    }

    @Override
    public void setCachedDataRetentionPeriod(long period) {
        SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(this.mContext);
        SharedPreferences.Editor e = prefs.edit();

        e.putLong(DailyUsageAggregateGenerator.DATA_RETENTION_PERIOD, period);

        e.apply();
    }

    public long storageUsed() {
        File path = PassiveDataKit.getGeneratorsStorage(this.mContext);

        path = new File(path, DailyUsageAggregateGenerator.DATABASE_PATH);

        if (path.exists()) {
            return path.length();
        }

        return -1;
    }

    @Override
    public List<Bundle> fetchPayloads() {
        return null;
    }

    public Cursor queryHistory(String[] cols, String where, String[] args, String orderBy) {
        if (this.mDatabase != null) {
            return this.mDatabase.query(DailyUsageAggregateGenerator.TABLE_HISTORY, cols, where, args, null, null, orderBy);
        }

        if (cols == null) {
            cols = new String[0];
        }

        return new MatrixCursor(cols);
    }

    public void updateConfig(JSONObject config) {
        SharedPreferences prefs = Generators.getInstance(this.mContext).getSharedPreferences(this.mContext);
        SharedPreferences.Editor e = prefs.edit();

        e.putString(DailyUsageAggregateGenerator.LAST_CONFIGURATION, config.toString());

        e.apply();

        try {
            if (config.has("enabled")) {
                this.setEnabled(config.getBoolean("enabled"));

                config.remove("enabled");
            }

            if (config.has("lookback-days")) {
                this.setLookbackDays(config.getInt("lookback-days"));

                config.remove("lookback-days");
            }

            if (config.has("sample-interval")) {
                this.setSampleInterval(config.getLong("sample-interval"));

                config.remove("sample-interval");
            }
        } catch (JSONException ex) {
            ex.printStackTrace();
        }
    }

    private void setEnabled(boolean enabled) {
        SharedPreferences prefs = Generators.getInstance(this.mContext).getSharedPreferences(this.mContext);
        SharedPreferences.Editor e = prefs.edit();

        e.putBoolean(DailyUsageAggregateGenerator.ENABLED, enabled);

        e.apply();

        if (enabled) {
            this.startGenerator();
        } else {
            this.stopGenerator();
        }
    }

    public void setLookbackDays(int days) {
        SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(this.mContext);
        SharedPreferences.Editor e = prefs.edit();
        e.putInt(DailyUsageAggregateGenerator.LOOKBACK_DAYS, days);
        e.apply();
    }

    public void setSampleInterval(long interval) {
        SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(this.mContext);
        SharedPreferences.Editor e = prefs.edit();
        e.putLong(DailyUsageAggregateGenerator.SAMPLE_INTERVAL, interval);
        e.apply();
    }

    /**
     * Compute the local-midnight day bucket for a given epoch ms timestamp
     * in the provided timezone. Used by tests; also internally consistent.
     */
    static long computeDayBucket(long epochMs, TimeZone tz) {
        Calendar cal = Calendar.getInstance(tz);
        cal.setTimeInMillis(epochMs);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }
}
