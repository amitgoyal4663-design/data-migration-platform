package com.dmp.application.port.out;

import java.time.Duration;

/**
 * A doorbell for workers: "there is claimable work now".
 *
 * <p>Exists because the poll interval, not the work, was the cost of a run. Measured on a
 * fifty-one chunk migration: 17.7 seconds of chunk execution inside 261.7 seconds of wall clock,
 * the difference being a worker asleep next to work it had already finished.
 *
 * <p><b>A nudge is not permission.</b> It carries no chunk, and a worker that receives one does
 * exactly what it does on a poll: query for claimable work and claim it atomically. That single
 * discipline is what makes the transport's guarantees irrelevant — and they need to be irrelevant,
 * because no message transport offers what a migration requires.
 *
 * <table border="1">
 *   <caption>Why the weak contract is the right one</caption>
 *   <tr><td>Delivered twice</td><td>Two workers try to claim; one loses. Harmless.</td></tr>
 *   <tr><td>Lost entirely</td><td>The poll finds the work. Slower, still correct.</td></tr>
 *   <tr><td>Delivered to a busy pod</td><td>Wake wasted. The poll covers it.</td></tr>
 *   <tr><td>Delivered late</td><td>Indistinguishable from a poll.</td></tr>
 * </table>
 *
 * <p>The corollary is the rule that must never be broken: <b>do not put a chunk id in the message
 * and act on it.</b> The moment a worker executes something because a message told it to, a
 * duplicate delivery becomes a duplicated write — which on a migration means duplicated data.
 * Ownership stays with the claim, where it can be made atomic.
 */
public interface WorkNudge {

    /**
     * Says that work has appeared. Never blocks, never throws.
     *
     * <p>Called on the path that just finished a chunk, so it must not be able to fail it: a
     * migration is not allowed to break because the doorbell is unplugged.
     */
    void publish();

    /**
     * Waits up to {@code timeout} for a nudge, returning early if one arrives.
     *
     * <p>Replaces a plain sleep in the worker loop, so a deployment with no transport configured
     * behaves exactly as it did before — the timeout is the poll interval, and the loop is
     * unchanged.
     *
     * @return true if a nudge arrived, false if the timeout elapsed. Advisory: a caller must
     *         re-query either way, because the answer says nothing about what is claimable.
     */
    boolean await(Duration timeout);

    /** Whether anything is actually listening, for a startup line that says which mode this is. */
    default boolean isEnabled() {
        return false;
    }
}
