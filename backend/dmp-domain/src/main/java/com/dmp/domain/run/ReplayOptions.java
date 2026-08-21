package com.dmp.domain.run;

/**
 * How to re-deliver the records a run rejected.
 *
 * <p>Both choices exist because both answers are right some of the time and neither is safe as a
 * silent default.
 *
 * @param throughLatestVersion whether to send the records through the pipeline's currently
 *                             published version instead of the one the original run executed
 * @param acknowledgeRedaction confirmation that replaying redacted payloads is intended, required
 *                             only when the pipeline redacts something
 */
public record ReplayOptions(boolean throughLatestVersion, boolean acknowledgeRedaction) {

    /**
     * Send the records again exactly as they were, through the version that rejected them.
     *
     * <p>The right choice when the fix was at the destination — a missing picklist value, a
     * constraint relaxed, a permission granted. The records were always correct; the target was
     * not, so nothing about them should change on the way back through.
     */
    public static ReplayOptions asIs() {
        return new ReplayOptions(false, false);
    }

    /**
     * Send the records through the pipeline as it is published now.
     *
     * <p>The right choice when the fix was in the pipeline — a transform that now maps the value
     * the target refused. Deliberately not the default: it runs records through logic that has
     * never been applied to the rest of the original migration, so the replayed records and their
     * successful siblings are no longer the product of the same definition.
     */
    public static ReplayOptions throughPublishedVersion() {
        return new ReplayOptions(true, false);
    }

    public ReplayOptions acknowledgingRedaction() {
        return new ReplayOptions(throughLatestVersion, true);
    }
}
