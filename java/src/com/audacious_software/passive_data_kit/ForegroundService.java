package com.audacious_software.passive_data_kit;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.content.pm.ServiceInfo;
import android.os.Build;
import android.os.IBinder;
import android.util.Log;

import androidx.core.app.NotificationCompat;
import androidx.core.app.NotificationManagerCompat;

import com.audacious_software.passive_data_kit.R;

public class ForegroundService extends Service {
    private static final String TAG = "PDK-ForegroundService";
    public static final String ACTION_START_SERVICE = "com.audacious_software.passive_data_kit.ForegroundService.ACTION_START_SERVICE";

    public static final int NOTIFICATION_ID = 778642;

    public IBinder onBind(Intent intent) {
        return null;
    }

    public int onStartCommand(Intent intent, int flags, int startId) {
        super.onCreate();

        Notification note = ForegroundService.getForegroundNotification(this, intent);

        NotificationManagerCompat noteManager = NotificationManagerCompat.from(this);

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {

            if (noteManager.getNotificationChannel(this.getString(R.string.foreground_channel_id)) == null) {
                NotificationChannel channel = new NotificationChannel(this.getString(R.string.foreground_channel_id), this.getString(R.string.app_name), NotificationManager.IMPORTANCE_LOW);
                channel.setShowBadge(false);
                channel.setLockscreenVisibility(Notification.VISIBILITY_PUBLIC);

                noteManager.createNotificationChannel(channel);
            }
        }

        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
                this.startForeground(ForegroundService.NOTIFICATION_ID, note, ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE);
            } else if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                this.startForeground(ForegroundService.NOTIFICATION_ID, note, ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC);
            } else {
                this.startForeground(ForegroundService.NOTIFICATION_ID, note);
            }
        } catch (Exception e) {
            Log.e(TAG, "Failed to start foreground service: " + e.getMessage(), e);
        }

        return Service.START_STICKY;
    }

    public static int getNotificationId() {
        return ForegroundService.NOTIFICATION_ID;
    }

    public static Notification getForegroundNotification(Context context, Intent intent) {
        return ForegroundService.getForegroundNotification(context, intent, R.drawable.ic_foreground_service);
    }

    public static Notification getForegroundNotification(Context context, Intent intent, int iconResource) {
        if (intent == null) {
            intent = PackageManagerUtils.safeGetLaunchIntentForPackage(
                    context.getPackageManager(),
                    context.getApplicationContext().getPackageName());
        }

        PassiveDataKit.getInstance(context).annotateForegroundIntent(intent);

        NotificationCompat.Builder builder = new NotificationCompat.Builder(context, context.getString(R.string.foreground_channel_id));
        builder.setContentTitle(context.getString(R.string.foreground_service_title));
        builder.setContentText(context.getString(R.string.foreground_service_message));
        builder.setAutoCancel(false);
        builder.setOngoing(true);
        builder.setPriority(NotificationCompat.PRIORITY_MIN);
        builder.setSound(null);

        int notificationIcon = iconResource;

        if (intent != null) {
            notificationIcon = intent.getIntExtra(PassiveDataKit.NOTIFICATION_ICON_ID, iconResource);
        }

        builder.setSmallIcon(notificationIcon);

        if (intent != null && intent.hasExtra(PassiveDataKit.NOTIFICATION_COLOR)) {
            builder.setColor(intent.getIntExtra(PassiveDataKit.NOTIFICATION_COLOR, 0));
        }

        PendingIntent pendingIntent = PassiveDataKit.getInstance(context).getForegroundPendingIntent();

        if (pendingIntent == null) {
            Intent launchIntent = PackageManagerUtils.safeGetLaunchIntentForPackage(
                    context.getPackageManager(),
                    context.getApplicationContext().getPackageName());

            PassiveDataKit.getInstance(context).annotateForegroundIntent(launchIntent);

            if (launchIntent != null) {
                pendingIntent = PendingIntent.getActivity(context, 0, launchIntent, PendingIntent.FLAG_IMMUTABLE);
            }
        }

        if (pendingIntent != null) {
            builder.setContentIntent(pendingIntent);
        }

        return builder.build();
    }
}
