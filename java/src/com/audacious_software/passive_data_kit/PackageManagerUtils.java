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

        Intent launchIntent = PackageManagerUtils.findLaunchIntent(
                packageManager,
                packageName,
                Intent.CATEGORY_INFO);

        if (launchIntent != null) {
            return launchIntent;
        }

        return PackageManagerUtils.findLaunchIntent(
                packageManager,
                packageName,
                Intent.CATEGORY_LAUNCHER);
    }

    private static Intent findLaunchIntent(PackageManager packageManager, String packageName, String category) {
        Intent queryIntent = new Intent(Intent.ACTION_MAIN);
        queryIntent.addCategory(category);
        queryIntent.setPackage(packageName);

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
