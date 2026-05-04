package com.audacious_software.passive_data_kit.generators.device;

import android.annotation.SuppressLint;
import android.app.AlarmManager;
import android.app.AppOpsManager;
import android.app.PendingIntent;
import android.app.Service;
import android.app.usage.UsageStats;
import android.app.usage.UsageStatsManager;
import android.content.BroadcastReceiver;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.database.Cursor;
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
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Per-(day, package) totalTimeInForeground aggregate from
 * {@link UsageStatsManager#queryUsageStats(int, long, long)} with
 * INTERVAL_DAILY.
 */
public class DailyUsageAggregateGenerator extends Generator {
    private static final String GENERATOR_IDENTIFIER = "pdk-daily-usage-aggregate";
    private static final int DATABASE_VERSION = 1;
    private static final String DATABASE_PATH = "pdk-daily-usage-aggregate.sqlite";

    private static final String ENABLED = "com.audacious_software.passive_data_kit.generators.device.DailyUsageAggregateGenerator.ENABLED";
    private static final boolean ENABLED_DEFAULT = true;

    private static final String LOOKBACK_DAYS = "com.audacious_software.passive_data_kit.generators.device.DailyUsageAggregateGenerator.LOOKBACK_DAYS";
    private static final int LOOKBACK_DAYS_DEFAULT = 30;
    private static final long DAY_MS = 24L * 60L * 60L * 1000L;

    private static final String SAMPLE_INTERVAL = "com.audacious_software.passive_data_kit.generators.device.DailyUsageAggregateGenerator.SAMPLE_INTERVAL";
    private static final long SAMPLE_INTERVAL_DEFAULT = DAY_MS; // once per day

    private static final String LAST_OPPORTUNISTIC_RUN = "com.audacious_software.passive_data_kit.generators.device.DailyUsageAggregateGenerator.LAST_OPPORTUNISTIC_RUN";
    private static final long OPPORTUNISTIC_STARTUP_DELAY = 10L * 1000L;
    private static final long OPPORTUNISTIC_COOLDOWN = 6L * 60L * 60L * 1000L;

    /**
     * Intent action for the AlarmManager fire that triggers a backfill run.
     * Routed to {@link AlarmReceiver}.
     */
    public static final String ACTION_RUN_AGGREGATION =
            "com.audacious_software.passive_data_kit.generators.device.DailyUsageAggregateGenerator.RUN_AGGREGATION";

    /**
     * Hour-of-day (device-local) at which the daily alarm fires. 15:00 (3pm)
     * is mid-afternoon, when phones are nearly always powered on, on a network,
     * and not in an OEM "deep sleep" / Adaptive Battery / overnight Battery
     * Saver state — maximizing the probability that the alarm fires on its
     * intended day. The previous day's INTERVAL_DAILY bucket has been
     * finalized for ~15 hours by then, well within the OS write window.
     */
    private static final int ALARM_HOUR_LOCAL = 15;

    /**
     * Stable PendingIntent request code for the daily alarm. Anything unique
     * within the host app works; the value just has to be deterministic so
     * re-creating the PendingIntent matches the previously scheduled one.
     */
    private static final int ALARM_REQUEST_CODE = 0xDA15;

    private static final String DATA_RETENTION_PERIOD = "com.audacious_software.passive_data_kit.generators.device.DailyUsageAggregateGenerator.DATA_RETENTION_PERIOD";
    private static final long DATA_RETENTION_PERIOD_DEFAULT = 365L * DAY_MS;

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
    private static final AtomicBoolean sAggregationRunning = new AtomicBoolean(false);
    private static final AtomicBoolean sOpportunisticRunScheduled = new AtomicBoolean(false);
    private final SQLiteDatabase mDatabase;

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

        // Touch the singleton so the SQLite database opens and the
        // AlarmReceiver inner class has somewhere to write rows when it fires.
        DailyUsageAggregateGenerator.getInstance(context);

        if (prefs.getBoolean(DailyUsageAggregateGenerator.ENABLED, DailyUsageAggregateGenerator.ENABLED_DEFAULT)) {
            DailyUsageAggregateGenerator.scheduleNextAlarm(context);
            DailyUsageAggregateGenerator.scheduleOpportunisticAggregation(context);
        } else {
            DailyUsageAggregateGenerator.cancelAlarm(context);
        }
    }

    private static void scheduleOpportunisticAggregation(final Context context) {
        final Context appCtx = context.getApplicationContext();
        final SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(appCtx);
        long now = System.currentTimeMillis();
        long lastRun = prefs.getLong(DailyUsageAggregateGenerator.LAST_OPPORTUNISTIC_RUN, 0L);

        if (now - lastRun < DailyUsageAggregateGenerator.OPPORTUNISTIC_COOLDOWN) {
            return;
        }

        if (!DailyUsageAggregateGenerator.sOpportunisticRunScheduled.compareAndSet(false, true)) {
            return;
        }

        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.setRemoveOnCancelPolicy(true);
        executor.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    prefs.edit()
                            .putLong(DailyUsageAggregateGenerator.LAST_OPPORTUNISTIC_RUN, System.currentTimeMillis())
                            .apply();
                    DailyUsageAggregateGenerator.getInstance(appCtx).runAggregation();
                } catch (Throwable t) {
                    Log.e("PDK", "DailyUsageAggregate failure (opportunistic startup)", t);
                } finally {
                    DailyUsageAggregateGenerator.sOpportunisticRunScheduled.set(false);
                    executor.shutdown();
                }
            }
        }, DailyUsageAggregateGenerator.OPPORTUNISTIC_STARTUP_DELAY, TimeUnit.MILLISECONDS);
    }

    /**
     * Schedule (or re-schedule) the next daily-aggregate alarm for the next
     * {@link #ALARM_HOUR_LOCAL} hour in the device's local timezone, using
     * {@link AlarmManager#setAndAllowWhileIdle}. Idempotent: re-arming
     * overwrites any previously scheduled fire for the same PendingIntent.
     *
     * Inexact (±15 min in Doze) is intentional — the daily backfill tolerates
     * that slack and inexact alarms require no special permission, no Play
     * Console policy declaration, and aren't subject to the user-revocable
     * SCHEDULE_EXACT_ALARM grant.
     */
    public static void scheduleNextAlarm(Context context) {
        Context appCtx = context.getApplicationContext();
        AlarmManager am = (AlarmManager) appCtx.getSystemService(Context.ALARM_SERVICE);
        if (am == null) {
            return;
        }

        long now = System.currentTimeMillis();
        long nextFire = computeNextFireTime(now, TimeZone.getDefault());

        PendingIntent pi = createAlarmPendingIntent(appCtx, false);

        try {
            am.setAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, nextFire, pi);
            Log.i("PDK", "DailyUsageAggregate alarm scheduled for " + new Date(nextFire));
        } catch (Throwable t) {
            Log.e("PDK", "DailyUsageAggregate alarm schedule failed", t);
        }
    }

    /**
     * Cancel any pending daily-aggregate alarm.
     */
    public static void cancelAlarm(Context context) {
        Context appCtx = context.getApplicationContext();
        AlarmManager am = (AlarmManager) appCtx.getSystemService(Context.ALARM_SERVICE);
        if (am == null) {
            return;
        }

        PendingIntent existing = createAlarmPendingIntent(appCtx, true);
        if (existing != null) {
            am.cancel(existing);
            existing.cancel();
        }
    }

    /**
     * Build the canonical PendingIntent used for the daily alarm. Always
     * targets {@link AlarmReceiver} explicitly with {@link #ACTION_RUN_AGGREGATION}.
     *
     * @param noCreate when true, returns null if no PendingIntent is currently
     *                 registered (used for existence checks); when false,
     *                 always returns a non-null PendingIntent (FLAG_UPDATE_CURRENT).
     */
    private static PendingIntent createAlarmPendingIntent(Context ctx, boolean noCreate) {
        Intent i = new Intent(ctx, AlarmReceiver.class).setAction(ACTION_RUN_AGGREGATION);
        int flags = PendingIntent.FLAG_IMMUTABLE
                | (noCreate ? PendingIntent.FLAG_NO_CREATE : PendingIntent.FLAG_UPDATE_CURRENT);
        return PendingIntent.getBroadcast(ctx, ALARM_REQUEST_CODE, i, flags);
    }

    /**
     * Compute the next strictly-future {@link #ALARM_HOUR_LOCAL}:00 timestamp
     * in the given timezone, in epoch millis.
     */
    static long computeNextFireTime(long now, TimeZone tz) {
        Calendar cal = Calendar.getInstance(tz);
        cal.setTimeInMillis(now);
        cal.set(Calendar.HOUR_OF_DAY, ALARM_HOUR_LOCAL);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        if (cal.getTimeInMillis() <= now) {
            cal.add(Calendar.DAY_OF_MONTH, 1);
        }
        return cal.getTimeInMillis();
    }

    public void runAggregation() {
        if (!DailyUsageAggregateGenerator.sAggregationRunning.compareAndSet(false, true)) {
            Log.i("PDK", "DailyUsageAggregate run already in progress; skipping");
            return;
        }

        try {
            this.runAggregationLocked();
        } finally {
            DailyUsageAggregateGenerator.sAggregationRunning.set(false);
        }
    }

    private void runAggregationLocked() {
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

        long todayBucket = computeDayBucket(now, TimeZone.getDefault());

        for (int i = 0; i < lookbackDays; i++) {
            long dayStart = todayBucket - (long) i * DAY_MS;
            long dayEnd = dayStart + DAY_MS;

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
        // "Running" now means a daily AlarmManager fire is pending for the
        // canonical PendingIntent.
        return createAlarmPendingIntent(context.getApplicationContext(), true) != null;
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
        return context.getString(R.string.generator_daily_usage_aggregate);
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
        return this.mDatabase.query(DailyUsageAggregateGenerator.TABLE_HISTORY, cols, where, args, null, null, orderBy);
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
            DailyUsageAggregateGenerator.scheduleNextAlarm(this.mContext);
            DailyUsageAggregateGenerator.scheduleOpportunisticAggregation(this.mContext);
        } else {
            DailyUsageAggregateGenerator.cancelAlarm(this.mContext);
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

    /**
     * Manifest-declared receiver for the daily AlarmManager fire and for
     * BOOT_COMPLETED / MY_PACKAGE_REPLACED re-arming. The alarm fire path
     * runs the SQLite/UsageStats aggregation via {@link #goAsync()} on a
     * worker thread (well under the 10-second receiver budget) and re-arms
     * the next fire. Boot/replace just re-arms without running aggregation
     * since UsageStats may not be ready immediately post-boot.
     */
    public static class AlarmReceiver extends BroadcastReceiver {
        @Override
        public void onReceive(final Context context, Intent intent) {
            final String action = (intent != null) ? intent.getAction() : null;

            if (Intent.ACTION_BOOT_COMPLETED.equals(action)
                    || "android.intent.action.LOCKED_BOOT_COMPLETED".equals(action)
                    || Intent.ACTION_MY_PACKAGE_REPLACED.equals(action)) {
                DailyUsageAggregateGenerator.scheduleNextAlarm(context);
                return;
            }

            if (!ACTION_RUN_AGGREGATION.equals(action)) {
                return;
            }

            final PendingResult result = goAsync();
            final Context appCtx = context.getApplicationContext();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        DailyUsageAggregateGenerator.getInstance(appCtx).runAggregation();
                    } catch (Throwable t) {
                        Log.e("PDK", "DailyUsageAggregate failure (alarm fire)", t);
                    } finally {
                        try {
                            DailyUsageAggregateGenerator.scheduleNextAlarm(appCtx);
                        } catch (Throwable t) {
                            Log.e("PDK", "DailyUsageAggregate reschedule failed", t);
                        }
                        result.finish();
                    }
                }
            }, "PDK-DailyUsageAggregate").start();
        }
    }
}
