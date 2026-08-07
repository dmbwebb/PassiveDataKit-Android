package com.audacious_software.passive_data_kit;

import android.content.Intent;
import android.content.pm.ActivityInfo;
import android.content.pm.PackageManager;
import android.content.pm.ResolveInfo;

import java.util.List;

public final class PackageManagerUtils {
    private PackageManagerUtils() {

    }

    public static Intent safeGetLaunchIntentForPackage(PackageManager packageManager, String packageName) {
        if (packageManager == null || PackageManagerUtils.hasText(packageName) == false) {
            return null;
        }

        Intent infoIntent = new Intent(Intent.ACTION_MAIN);
        infoIntent.addCategory(Intent.CATEGORY_INFO);
        infoIntent.setPackage(packageName);

        Intent launchIntent = PackageManagerUtils.resolveValidActivity(packageManager, infoIntent);

        if (launchIntent != null) {
            return launchIntent;
        }

        Intent launcherIntent = new Intent(Intent.ACTION_MAIN);
        launcherIntent.addCategory(Intent.CATEGORY_LAUNCHER);
        launcherIntent.setPackage(packageName);

        return PackageManagerUtils.resolveValidActivity(packageManager, launcherIntent);
    }

    private static Intent resolveValidActivity(PackageManager packageManager, Intent queryIntent) {
        List<ResolveInfo> matches = packageManager.queryIntentActivities(queryIntent, 0);

        if (matches == null) {
            return null;
        }

        for (ResolveInfo match : matches) {
            if (match == null) {
                continue;
            }

            ActivityInfo activityInfo = match.activityInfo;

            if (activityInfo == null
                    || PackageManagerUtils.hasText(activityInfo.packageName) == false
                    || PackageManagerUtils.hasText(activityInfo.name) == false) {
                continue;
            }

            Intent launchIntent = new Intent(queryIntent);
            launchIntent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
            launchIntent.setClassName(activityInfo.packageName, activityInfo.name);

            return launchIntent;
        }

        return null;
    }

    private static boolean hasText(String value) {
        return value != null && value.trim().isEmpty() == false;
    }
}
