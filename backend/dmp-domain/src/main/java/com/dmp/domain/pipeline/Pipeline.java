package com.dmp.domain.pipeline;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.tenant.TenantId;

import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A pipeline: a named, versioned, tenant-scoped data movement definition.
 *
 * <p>The aggregate deliberately holds no DAG. The graph lives in {@link PipelineVersion}, which is
 * immutable once published, so that a run started yesterday executes exactly what it was started
 * with even after the pipeline has been edited since. Separating the mutable container from the
 * immutable content is what makes version comparison and rollback (Phase 12) possible at all.
 *
 * <p>Immutable. Mutators return a new instance, which keeps concurrent handling of the same
 * pipeline free of shared-state hazards and makes the optimistic-locking contract explicit.
 */
public record Pipeline(
        PipelineId id,
        TenantId tenantId,
        String name,
        String description,
        String folder,
        Set<String> tags,
        PipelineStatus status,
        /**
         * On the support team's daily screen.
         *
         * <p>A watchlist rather than every pipeline, because a screen that grows with the platform
         * stops being read: the nightly load that matters ends up among fifty experiments and
         * one-off migrations, and the person scanning it every morning learns to skim.
         */
        boolean monitored,
        Integer publishedVersion,
        int latestVersion,
        Instant createdAt,
        Instant updatedAt,
        long rowVersion) {

    private static final int MAX_NAME_LENGTH = 255;
    private static final int MAX_DESCRIPTION_LENGTH = 4_000;
    private static final int MAX_TAGS = 32;
    private static final Pattern TAG = Pattern.compile("^[a-z0-9][a-z0-9._-]{0,47}$");
    private static final Pattern FOLDER = Pattern.compile("^(/[A-Za-z0-9][A-Za-z0-9 ._-]{0,63})+$");

    public Pipeline {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");

        name = requireName(name);
        description = truncateOrReject(description);
        folder = normaliseFolder(folder);
        tags = normaliseTags(tags);

        if (latestVersion < 0) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED, "latestVersion must not be negative");
        }
        if (publishedVersion != null && (publishedVersion < 1 || publishedVersion > latestVersion)) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "publishedVersion must reference an existing version",
                    Map.of("publishedVersion", publishedVersion, "latestVersion", latestVersion));
        }
    }

    public static Pipeline create(TenantId tenantId, String name, String description,
                                  String folder, Set<String> tags, Instant now) {
        return new Pipeline(PipelineId.newId(), tenantId, name, description, folder, tags,
                PipelineStatus.DRAFT, false, null, 0, now, now, 0L);
    }

    public Pipeline updateMetadata(String newName, String newDescription, String newFolder,
                                   Set<String> newTags, Instant now) {
        requireMutable();
        return new Pipeline(id, tenantId, newName, newDescription, newFolder, newTags,
                status, monitored, publishedVersion, latestVersion, createdAt, now, rowVersion);
    }

    /** Records that a new draft version was created. Does not change what is published. */
    public Pipeline withNewVersion(int versionNumber, Instant now) {
        if (versionNumber != latestVersion + 1) {
            throw new DmpException(ErrorCode.CONCURRENT_MODIFICATION,
                    "Version numbers must be contiguous; expected " + (latestVersion + 1)
                            + " but was " + versionNumber,
                    Map.of("expected", latestVersion + 1, "actual", versionNumber));
        }
        return new Pipeline(id, tenantId, name, description, folder, tags,
                status, monitored, publishedVersion, versionNumber, createdAt, now, rowVersion);
    }

    /**
     * Records that the highest version is now lower, because a draft above it was deleted.
     *
     * <p>Needed because the counter is what {@link #withNewVersion} checks contiguity against, and
     * deleting the newest draft without lowering it leaves the pipeline unable to create another:
     * the next number available in the table is the one the counter has already used.
     *
     * <p>Never drops below the published version. A published version still exists and is still
     * referenced by every run that executed it, so the count of versions cannot go beneath it.
     */
    public Pipeline withHighestVersion(int highestRemaining, Instant now) {
        if (highestRemaining >= latestVersion) {
            return this;
        }
        int floor = Math.max(highestRemaining, publishedVersion == null ? 0 : publishedVersion);
        return new Pipeline(id, tenantId, name, description, folder, tags,
                status, monitored, publishedVersion, floor, createdAt, now, rowVersion);
    }

    /**
     * Promotes a version to published, activating the pipeline.
     *
     * <p>Publishing is what makes a pipeline runnable, so it doubles as the DRAFT to ACTIVE
     * transition. Republishing an earlier version is permitted and is the rollback mechanism.
     */
    public Pipeline publishVersion(int versionNumber, Instant now) {
        requireMutable();
        if (versionNumber < 1 || versionNumber > latestVersion) {
            throw new DmpException(ErrorCode.NOT_FOUND,
                    "Version " + versionNumber + " does not exist on this pipeline",
                    Map.of("versionNumber", versionNumber, "latestVersion", latestVersion));
        }
        PipelineStatus next = status == PipelineStatus.DRAFT ? PipelineStatus.ACTIVE : status;
        return new Pipeline(id, tenantId, name, description, folder, tags,
                next, monitored, versionNumber, latestVersion, createdAt, now, rowVersion);
    }

    public Pipeline archive(Instant now) {
        status.requireTransitionTo(PipelineStatus.ARCHIVED);
        return new Pipeline(id, tenantId, name, description, folder, tags,
                PipelineStatus.ARCHIVED, monitored, publishedVersion, latestVersion, createdAt, now, rowVersion);
    }

    public Pipeline restore(Instant now) {
        status.requireTransitionTo(PipelineStatus.ACTIVE);
        // A pipeline archived before anything was ever published returns to DRAFT, not ACTIVE.
        // Restoring must not make an unpublished pipeline runnable.
        PipelineStatus next = publishedVersion == null ? PipelineStatus.DRAFT : PipelineStatus.ACTIVE;
        return new Pipeline(id, tenantId, name, description, folder, tags,
                next, monitored, publishedVersion, latestVersion, createdAt, now, rowVersion);
    }

    /**
     * Puts this pipeline on the support team's daily screen, or takes it off.
     *
     * <p>Allowed in any state, including archived. Somebody investigating why an archived
     * pipeline's last run looked wrong should be able to watch it without reviving it, and
     * refusing would be the platform overruling its user for no benefit.
     */
    public Pipeline monitored(boolean watched, Instant now) {
        return new Pipeline(id, tenantId, name, description, folder, tags,
                status, watched, publishedVersion, latestVersion, createdAt, now, rowVersion);
    }

    public Optional<Integer> publishedVersionNumber() {
        return Optional.ofNullable(publishedVersion);
    }

    public boolean isRunnable() {
        return status.isRunnable() && publishedVersion != null;
    }

    private void requireMutable() {
        if (status == PipelineStatus.ARCHIVED) {
            throw new DmpException(ErrorCode.IMMUTABLE,
                    "Archived pipelines cannot be modified. Restore it first.",
                    Map.of("pipelineId", id.toString()));
        }
    }

    private static String requireName(String value) {
        if (value == null || value.isBlank()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED, "Pipeline name must not be blank");
        }
        String trimmed = value.strip();
        if (trimmed.length() > MAX_NAME_LENGTH) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Pipeline name exceeds " + MAX_NAME_LENGTH + " characters",
                    Map.of("length", trimmed.length()));
        }
        return trimmed;
    }

    private static String truncateOrReject(String value) {
        if (value == null) {
            return null;
        }
        if (value.length() > MAX_DESCRIPTION_LENGTH) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Description exceeds " + MAX_DESCRIPTION_LENGTH + " characters",
                    Map.of("length", value.length()));
        }
        return value;
    }

    private static String normaliseFolder(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String normalised = value.strip();
        if (!normalised.startsWith("/")) {
            normalised = "/" + normalised;
        }
        if (normalised.endsWith("/")) {
            normalised = normalised.substring(0, normalised.length() - 1);
        }
        if (!FOLDER.matcher(normalised).matches()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Folder must be a slash-separated path of alphanumeric segments, for example /finance/daily",
                    Map.of("folder", value));
        }
        return normalised;
    }

    private static Set<String> normaliseTags(Set<String> value) {
        if (value == null || value.isEmpty()) {
            return Set.of();
        }
        if (value.size() > MAX_TAGS) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "A pipeline may carry at most " + MAX_TAGS + " tags",
                    Map.of("count", value.size()));
        }
        Set<String> normalised = new LinkedHashSet<>();
        for (String tag : value) {
            if (tag == null || tag.isBlank()) {
                continue;
            }
            String lower = tag.strip().toLowerCase(Locale.ROOT);
            if (!TAG.matcher(lower).matches()) {
                throw new DmpException(ErrorCode.VALIDATION_FAILED,
                        "Tag '" + tag + "' must be 1-48 lowercase alphanumeric characters, "
                                + "dots, hyphens or underscores",
                        Map.of("tag", tag));
            }
            normalised.add(lower);
        }
        return Set.copyOf(normalised);
    }
}
