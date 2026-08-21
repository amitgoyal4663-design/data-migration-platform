package com.dmp.transform.graaljs;

import com.dmp.transform.api.TransformException;
import com.dmp.transform.api.WindowScript;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.io.IOAccess;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Evaluates a schedule's window script.
 *
 * <p>Runs in the same kind of sandbox as a transform, with one deliberate difference: a small
 * host object is exposed so the script can do calendar arithmetic. Everything else is closed —
 * no filesystem, no network, no threads, no environment, no host classes.
 *
 * <p><b>There is no clock inside.</b> {@code Date} and {@code performance} are removed after the
 * script is loaded, so the only time the script can see is the {@code fireTime} it was handed. That
 * is what makes the result a pure function of the firing: the same run recomputed an hour later
 * would otherwise silently cover an hour later, and a rerun of the first of August would become the
 * second.
 */
@Component
public class GraalJsWindowScript implements WindowScript {

    /**
     * Wraps the user's body in a function.
     *
     * <p>Written as an expression so a bare {@code return} at the top level of what the user typed
     * is legal — the script is a body, not a whole program, and asking somebody to declare a
     * function to write two lines of date arithmetic is friction with no purpose.
     */
    private static final String WRAPPER = """
            (function (fireTime) {
            %s
            })
            """;

    /**
     * Only what the script needs.
     *
     * <p>An allowlist of the methods on one class, rather than {@code HostAccess.ALL} — which would
     * expose every public method of every object that reached the guest, including whatever those
     * return.
     */
    private static final HostAccess ACCESS = HostAccess.newBuilder()
            .allowAccessAnnotatedBy(HostAccess.Export.class)
            .allowMapAccess(true)
            .build();

    @Override
    public Map<String, String> evaluate(String script, Instant fireTime, ZoneId timezone) {
        if (script == null || script.isBlank()) {
            return Map.of();
        }

        try (Context context = sandbox()) {
            Value function;
            try {
                function = context.eval("js", WRAPPER.formatted(script));
            } catch (PolyglotException e) {
                throw new TransformException("window", "window script",
                        "The window script did not compile: " + e.getMessage(), e);
            }

            // Removed after loading rather than before: the wrapper itself is ordinary JavaScript
            // and nothing in it needs a clock, while anything the user wrote runs after this line.
            context.eval("js", "delete globalThis.Date; delete globalThis.performance;");

            Value result;
            try {
                result = function.execute(new Moment(fireTime, timezone));
            } catch (PolyglotException e) {
                throw new TransformException("window", "window script",
                        "The window script failed: " + e.getMessage(), e);
            }

            return toParameters(result);
        }
    }

    /**
     * Turns whatever the script returned into the values a run carries.
     *
     * <p>Everything becomes a string, because that is what a query parameter is and because the
     * connector infers the type from the shape of the value anyway. A {@link Moment} formats itself
     * as ISO-8601 with an offset; a number formats as itself.
     */
    private static Map<String, String> toParameters(Value result) {
        if (result == null || result.isNull()) {
            return Map.of();
        }
        if (!result.hasMembers()) {
            throw new TransformException("window", "window script",
                    "A window script must return an object of values, for example "
                            + "{ from: ..., to: ... }, but it returned " + describe(result) + ".",
                    null);
        }

        Map<String, String> parameters = new LinkedHashMap<>();
        for (String name : result.getMemberKeys()) {
            Value member = result.getMember(name);
            if (member == null || member.isNull()) {
                continue;
            }
            parameters.put(name, member.isHostObject()
                    ? member.asHostObject().toString()
                    : member.toString());
        }

        if (parameters.isEmpty()) {
            throw new TransformException("window", "window script",
                    "The window script returned an object with no values in it. It should return "
                            + "the parameters the query expects, for example { from: ..., to: ... }.",
                    null);
        }
        return parameters;
    }

    private static String describe(Value value) {
        if (value.isString()) {
            return "a string";
        }
        if (value.isNumber()) {
            return "a number";
        }
        return value.toString();
    }

    private static Context sandbox() {
        return Context.newBuilder("js")
                .allowAllAccess(false)
                .allowHostAccess(ACCESS)
                .allowHostClassLookup(className -> false)
                .allowIO(IOAccess.NONE)
                .allowNativeAccess(false)
                .allowCreateThread(false)
                .allowCreateProcess(false)
                .allowEnvironmentAccess(org.graalvm.polyglot.EnvironmentAccess.NONE)
                .allowPolyglotAccess(org.graalvm.polyglot.PolyglotAccess.NONE)
                .option("js.ecmascript-version", "2023")
                .build();
    }
}
