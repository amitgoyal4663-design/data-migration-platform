package com.dmp.transform.api;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;

/**
 * Computes the range of data one scheduled run should cover.
 *
 * <p>A script rather than a set of configuration fields, because the arithmetic is two lines and
 * the vocabulary needed to express it in configuration is not. "The previous calendar day", "the
 * previous hour", "the last complete week", "yesterday unless it is Monday, then Friday" — a
 * {@code windowUnit} and {@code windowCount} scheme covers the first two and nothing else, and the
 * third request arrives about a week after the second.
 *
 * <p>Two properties make this safe to run on a schedule:
 *
 * <ul>
 *   <li><b>The fire time is given, not read.</b> A script that could read the wall clock would
 *       compute a different window every time it ran, so the same run re-executed an hour later
 *       would silently cover an hour later — and a rerun of the first of August would quietly
 *       become the second.</li>
 *   <li><b>The result is stored on the run.</b> It is computed once, when the run is created, and
 *       everything afterwards reads what was stored. The script never runs twice for one run.</li>
 * </ul>
 */
public interface WindowScript {

    /**
     * Evaluates a window script for one firing.
     *
     * @param script    the user's JavaScript, returning an object of values — typically
     *                  {@code { from, to }}
     * @param fireTime  the instant the schedule was due, which is the only clock the script sees
     * @param timezone  the schedule's zone, so "the previous day" means midnight where the schedule
     *                  lives rather than in UTC
     * @return the values to store on the run, as strings
     * @throws TransformException if the script does not compile, does not return an object, or
     *                            takes longer than the platform allows
     */
    Map<String, String> evaluate(String script, Instant fireTime, ZoneId timezone);
}
