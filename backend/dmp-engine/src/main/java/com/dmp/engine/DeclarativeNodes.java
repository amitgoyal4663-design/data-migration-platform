package com.dmp.engine;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.pipeline.NodeDefinition;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Turns the nodes somebody configures with a form into the scripts the engine already runs.
 *
 * <p>A mapping and a validation rule are not new kinds of work — they are a transform whose logic
 * happens to be expressible as data. Compiling them to JavaScript rather than building two more
 * execution paths means they inherit, without any new code, everything the sandbox already
 * provides: the timeout, the per-node attribution in errors, the stage log, the dead-letter queue,
 * and the rule that a script failure is not retryable because a script is deterministic.
 *
 * <p><b>Why declarative nodes exist at all.</b> Every field rename in this platform was JavaScript,
 * and that puts the most common task in the platform behind the one skill most people doing
 * migrations do not have. It also throws away information: a script that renames twelve fields is
 * an opaque blob, while twelve mappings can be listed, diffed, validated against a preview and
 * reported on.
 *
 * <p>The generated source is never shown as the thing to edit. It is an implementation detail of
 * the node, and a user who edits it has a script node — which they can already have by choosing one.
 */
final class DeclarativeNodes {

    private DeclarativeNodes() {
    }

    // ---------------------------------------------------------------- mapper

    /**
     * Compiles a list of field mappings into a per-record script.
     *
     * <p>Reads and writes dotted paths, so {@code customer.address.city} works at both ends without
     * the user knowing anything about JavaScript's behaviour when an intermediate object is absent
     * — which is a thrown TypeError, one of the two commonest ways a hand-written transform fails.
     *
     * <p>Type coercion is the part that earns its keep. A warehouse hands back {@code "50.0"} as a
     * string and a destination declares the field numeric; the mapping says {@code NUMBER} and the
     * conversion happens once, in one place, rather than being remembered at every call site.
     */
    static String mapperScript(NodeDefinition node) {
        JsonNode mappings = node.config().path("mappings");
        if (!mappings.isArray() || mappings.isEmpty()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Mapper '" + node.name() + "' has no mappings. Add one, or remove the node.",
                    Map.of("nodeId", node.id()));
        }

        // Unmapped fields are dropped by default, and that is the safer direction. A mapper exists
        // to say what the destination receives; carrying everything else through by default means
        // a source column added months later silently starts arriving somewhere it was never
        // declared, which is how personal data ends up where nobody put it.
        boolean keepUnmapped = node.config().path("keepUnmapped").asBoolean(false);

        StringBuilder script = new StringBuilder(512);
        script.append(HELPERS);
        script.append("function transform(record) {\n");
        script.append(keepUnmapped
                ? "  var out = JSON.parse(JSON.stringify(record));\n"
                : "  var out = {};\n");

        for (JsonNode mapping : mappings) {
            String to = text(mapping, "to");
            JsonNode fallback = mapping.get("default");
            boolean hasDefault = fallback != null && !fallback.isNull();

            if (to == null) {
                throw new DmpException(ErrorCode.VALIDATION_FAILED,
                        "Every mapping in '" + node.name() + "' needs a target field",
                        Map.of("nodeId", node.id()));
            }

            script.append("  {\n");
            script.append(readStep(mapping, node, to, fallback, hasDefault));

            // Order matters and is fixed, because a mapping is data and the same data must mean the
            // same thing every time. Read, fill in, translate, tidy, decorate, fit, convert:
            //
            //   default   before everything, so nothing downstream sees a missing value
            //   values    on the source's own vocabulary, before it has been reshaped
            //   trim      before case and length, so neither is measured against padding
            //   case      before prefix, so a prefix keeps the case it was typed in
            //   prefix    before truncate, so the limit applies to what is actually sent
            //   truncate  before the type, so a length limit cannot produce an unparseable number
            //   type      last, because it is the destination's requirement and not a text edit
            if (mapping.has("values")) {
                script.append("    v = __translate(v, ").append(valueMap(mapping, node))
                        .append(", ").append(mapping.path("otherwise").isMissingNode()
                                ? "undefined" : mapping.path("otherwise").toString())
                        .append(");\n");
            }
            if (mapping.path("trim").asBoolean(false)) {
                script.append("    v = __trim(v);\n");
            }
            String letterCase = mapping.path("case").asText("");
            if (!letterCase.isBlank()) {
                script.append("    v = ").append(caseCall(letterCase, node)).append(";\n");
            }
            String prefix = text(mapping, "prefix");
            if (prefix != null) {
                script.append("    v = __affix(").append(quote(prefix)).append(", v, \"\");\n");
            }
            String suffix = text(mapping, "suffix");
            if (suffix != null) {
                script.append("    v = __affix(\"\", v, ").append(quote(suffix)).append(");\n");
            }
            if (mapping.has("replace")) {
                JsonNode replace = mapping.path("replace");
                String find = text(replace, "find");
                if (find == null) {
                    throw new DmpException(ErrorCode.VALIDATION_FAILED,
                            "A replace in '" + node.name() + "' has nothing to find",
                            Map.of("nodeId", node.id()));
                }
                // Literal, not a pattern. A regular expression in a form field is a support call
                // waiting to happen, and every case seen so far is "strip this prefix".
                script.append("    v = __replace(v, ").append(quote(find)).append(", ")
                        .append(quote(replace.path("with").asText(""))).append(");\n");
            }
            if (mapping.path("maxLength").isNumber()) {
                script.append("    v = __truncate(v, ")
                        .append(mapping.path("maxLength").asText()).append(");\n");
            }
            if (mapping.path("required").asBoolean(false)) {
                // Names both ends when they differ. The requirement belongs to the destination and
                // the fix belongs in the source, so a message with only one of them sends somebody
                // to the wrong system.
                JsonNode fromNode = mapping.path("from");
                String origin = fromNode.isArray()
                        ? "joined fields"
                        : (notBlank(fromNode) ? fromNode.asText().trim() : null);
                String complaint = origin == null || origin.equals(to)
                        ? to + " is required and had no value"
                        : to + " is required and had no value (from " + origin + ")";
                script.append("    if (v === undefined || v === null || v === '') throw new Error(")
                        .append(quote(complaint)).append(");\n");
            }
            script.append("    v = ").append(coercion(mapping.path("type").asText("AS_IS"), node))
                    .append(";\n");

            // OMIT is the default, because the two are not interchangeable at the destination: a
            // Salesforce update sent an explicit null clears the field, while an absent key leaves
            // whatever is there. Writing null by default would silently erase data on an upsert.
            if ("NULL".equals(mapping.path("onMissing").asText("OMIT"))) {
                // An explicit null, not undefined. Setting undefined creates the key and JSON then
                // drops it, so the field would be absent — which is the behaviour this option
                // exists to opt out of.
                script.append("    __set(out, ").append(quote(to))
                        .append(", v === undefined ? null : v);\n");
            } else {
                script.append("    if (v !== undefined && v !== null) __set(out, ")
                        .append(quote(to)).append(", v);\n");
            }
            script.append("  }\n");
        }

        script.append("  return out;\n}\n");
        return script.toString();
    }

    /**
     * How the value is obtained: one field, several joined, or a constant.
     *
     * <p>Joining exists because "first name and last name into one field" is not logic anybody
     * should open a script for, and it is asked for on nearly every migration.
     */
    private static String readStep(JsonNode mapping, NodeDefinition node, String to,
                                   JsonNode fallback, boolean hasDefault) {
        JsonNode from = mapping.path("from");
        StringBuilder step = new StringBuilder();

        if (from.isArray() && !from.isEmpty()) {
            List<String> paths = new ArrayList<>();
            from.forEach(entry -> paths.add(quote(entry.asText())));
            step.append("    var v = __join([").append(String.join(",", paths)).append("], record, ")
                    .append(quote(mapping.path("join").asText(" "))).append(");\n");

        } else if (notBlank(from)) {
            step.append("    var v = __get(record, ").append(quote(from.asText().trim()))
                    .append(");\n");

        } else if (hasDefault) {
            // A mapping with no source but a default is a constant, and it is one of the commonest
            // things a migration needs: stamp every record with the system it came from, a record
            // type, an owner, a load id.
            return "    var v = " + fallback.toString() + ";\n";

        } else {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "The mapping to '" + to + "' in '" + node.name() + "' has neither a source "
                            + "field nor a default value, so there is nothing to put there.",
                    Map.of("nodeId", node.id(), "to", to));
        }

        if (hasDefault) {
            // Before everything else, so nothing downstream has to cope with a missing value — and
            // so "default plus required" means "must end up with a value" rather than contradicting
            // itself.
            step.append("    if (v === undefined || v === null || v === '') v = ")
                    .append(fallback.toString()).append(";\n");
        }
        return step.toString();
    }

    private static boolean notBlank(JsonNode node) {
        return node != null && node.isTextual() && !node.asText().isBlank();
    }

    private static String caseCall(String letterCase, NodeDefinition node) {
        return switch (letterCase.toUpperCase()) {
            case "UPPER" -> "__upper(v)";
            case "LOWER" -> "__lower(v)";
            default -> throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Mapper '" + node.name() + "' asks for case '" + letterCase
                            + "', which is not UPPER or LOWER", Map.of("nodeId", node.id()));
        };
    }

    /** The translation table, as a JavaScript object literal keyed by the source's own values. */
    private static String valueMap(JsonNode mapping, NodeDefinition node) {
        JsonNode values = mapping.path("values");
        if (!values.isObject() || values.isEmpty()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "A value translation in '" + node.name() + "' is empty",
                    Map.of("nodeId", node.id()));
        }
        List<String> pairs = new ArrayList<>();
        values.properties().forEach(entry ->
                pairs.add(quote(entry.getKey()) + ":" + entry.getValue().toString()));
        return "{" + String.join(",", pairs) + "}";
    }

    /**
     * The conversion for a declared type.
     *
     * <p>An unconvertible value throws rather than becoming NaN or the string "undefined". Silent
     * coercion is how a destination ends up holding rows that look right and are not, and the
     * record is far more useful in the dead-letter queue with a reason attached.
     */
    private static String coercion(String type, NodeDefinition node) {
        return switch (type.toUpperCase()) {
            case "AS_IS" -> "v";
            case "STRING" -> "__string(v)";
            case "NUMBER" -> "__number(v)";
            case "INTEGER" -> "__integer(v)";
            case "BOOLEAN" -> "__boolean(v)";
            // ISO-8601, which is what every destination in this platform accepts and what the
            // sources already produce. A format-string option was considered and left out: it
            // would be a second date library nobody asked for, inside a sandbox with no clock.
            case "DATE" -> "__date(v)";
            default -> throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Mapper '" + node.name() + "' asks for type '" + type + "', which is not one of "
                            + "AS_IS, STRING, NUMBER, INTEGER, BOOLEAN or DATE",
                    Map.of("nodeId", node.id(), "type", type));
        };
    }

    /**
     * How many broken rules a message names before it summarises the rest.
     *
     * <p>Five is enough to see the shape of what is wrong with a record. Beyond that the message
     * outgrows the stores and the table that hold it, and the truncation would fall at the end —
     * losing the tail silently rather than saying how much was left.
     */
    private static final int MAX_REPORTED_RULES = 5;

    // ------------------------------------------------------------ validation

    /**
     * Compiles named rules into a per-record script that throws the rule's name.
     *
     * <p>The name is the whole point, and it is why this is not a script node. A hand-written
     * validation throws whatever message its author typed, and the console groups failures by that
     * message — so a thrown {@code TypeError} on line 4 groups every failing record under a stack
     * frame. A rule named "email must be present" produces an error group with that name and a
     * count beside it, which is a sentence somebody can act on.
     */
    static String validationScript(NodeDefinition node) {
        JsonNode rules = node.config().path("rules");
        if (!rules.isArray() || rules.isEmpty()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Validation '" + node.name() + "' has no rules. Add one, or remove the node.",
                    Map.of("nodeId", node.id()));
        }

        // What happens to a record that fails. REJECT sends it to the dead-letter queue with the
        // rule's name; DROP filters it out silently. Both are wanted — a row failing a business
        // rule is a rejection worth investigating, a row that is simply out of scope is not.
        String onFail = node.config().path("onFail").asText("REJECT");
        if (!onFail.equals("REJECT") && !onFail.equals("DROP")) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Validation '" + node.name() + "' must fail with REJECT or DROP, not '"
                            + onFail + "'", Map.of("nodeId", node.id()));
        }

        // Every rule is evaluated, not just up to the first that fails, and that is the whole
        // difference between one round trip and several. Stopping at the first meant a record
        // breaking two rules was only ever reported as breaking one — so the second rule's count
        // was short by exactly the records the first had already claimed. Somebody fixes what the
        // report showed, runs ten thousand records again, and meets a fault that was there the
        // first time.
        //
        // The cost is that a group is now a combination of rules rather than a single rule. That
        // is a fair trade and often the more useful reading: "1,200 records broke both amount and
        // region" says something about a segment of the source that two separate rows do not.
        boolean firstOnly = "FIRST".equals(node.config().path("report").asText("ALL"));

        StringBuilder script = new StringBuilder(512);
        script.append(HELPERS);
        script.append("function transform(record) {\n");
        script.append("  var broke = [];\n");

        for (JsonNode rule : rules) {
            String field = text(rule, "field");
            String check = rule.path("check").asText("REQUIRED").toUpperCase();
            String name = rule.path("name").asText(null);
            if (field == null) {
                throw new DmpException(ErrorCode.VALIDATION_FAILED,
                        "Every rule in '" + node.name() + "' needs a field",
                        Map.of("nodeId", node.id()));
            }
            String label = (name == null || name.isBlank()) ? field + " failed " + check : name;
            JsonNode value = rule.get("value");

            script.append("  if (");
            if (firstOnly) {
                script.append("broke.length === 0 && ");
            }
            script.append("!(").append(predicate(check, field, value, node)).append(")) ");
            script.append("broke.push(").append(quote(label)).append(");\n");
        }

        script.append("  if (broke.length) ");
        script.append(onFail.equals("DROP")
                // Null drops the record. Counted as filtered, not failed, which is what the
                // console then reports — and the difference between the two is exactly what
                // somebody reading a run wants to know.
                ? "return null;\n"
                // Capped, because twenty broken rules on one record produce a message longer than
                // anything that stores or displays it will keep, and the tail is where truncation
                // would cut. The count is still exact.
                : "throw new Error(broke.length > " + MAX_REPORTED_RULES + "\n"
                        + "      ? broke.slice(0, " + MAX_REPORTED_RULES + ").join('; ') + "
                        + "'; and ' + (broke.length - " + MAX_REPORTED_RULES + ") + ' more'\n"
                        + "      : broke.join('; '));\n");

        script.append("  return record;\n}\n");
        return script.toString();
    }

    private static String predicate(String check, String field, JsonNode value,
                                    NodeDefinition node) {
        String read = "__get(record, " + quote(field) + ")";
        return switch (check) {
            case "REQUIRED" -> "__present(%s)".formatted(read);
            case "NOT_BLANK" -> "__present(%s) && __string(%s).trim() !== ''".formatted(read, read);
            case "IS_NUMBER" -> "__isNumber(%s)".formatted(read);
            case "MIN" -> "__isNumber(%s) && Number(%s) >= %s".formatted(read, read, number(value, node));
            case "MAX" -> "__isNumber(%s) && Number(%s) <= %s".formatted(read, read, number(value, node));
            case "MAX_LENGTH" -> "!__present(%s) || __string(%s).length <= %s"
                    .formatted(read, read, number(value, node));
            case "ONE_OF" -> "%s.indexOf(%s === null || %s === undefined ? %s : __string(%s)) >= 0"
                    .formatted(list(value, node), read, read, read, read);
            case "MATCHES" -> "__present(%s) && new RegExp(%s).test(__string(%s))"
                    .formatted(read, quote(stringValue(value, node)), read);
            default -> throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Validation '" + node.name() + "' asks for check '" + check + "', which is not "
                            + "one of REQUIRED, NOT_BLANK, IS_NUMBER, MIN, MAX, MAX_LENGTH, "
                            + "ONE_OF or MATCHES",
                    Map.of("nodeId", node.id(), "check", check));
        };
    }

    // ----------------------------------------------------------- shared bits

    /**
     * Read a dotted path, write a dotted path, and convert.
     *
     * <p>Prepended to every generated script rather than kept in the runtime, so the sandbox stays
     * a plain JavaScript engine with nothing of the platform's injected into it. That is worth more
     * than the few hundred duplicated bytes: the day something is injected is the day a script can
     * reach it.
     */
    private static final String HELPERS = """
            function __get(o, path) {
              var parts = path.split('.');
              var v = o;
              for (var i = 0; i < parts.length; i++) {
                if (v === null || v === undefined) return undefined;
                v = v[parts[i]];
              }
              return v;
            }
            function __set(o, path, value) {
              var parts = path.split('.');
              var target = o;
              for (var i = 0; i < parts.length - 1; i++) {
                if (target[parts[i]] === null || typeof target[parts[i]] !== 'object') {
                  target[parts[i]] = {};
                }
                target = target[parts[i]];
              }
              target[parts[parts.length - 1]] = value;
            }
            function __present(v) { return v !== null && v !== undefined; }
            function __join(paths, record, separator) {
              var parts = [];
              for (var i = 0; i < paths.length; i++) {
                var piece = __get(record, paths[i]);
                if (__present(piece) && String(piece) !== '') parts.push(String(piece));
              }
              return parts.length === 0 ? undefined : parts.join(separator);
            }
            function __translate(v, table, otherwise) {
              if (!__present(v)) return v;
              var key = String(v);
              if (Object.prototype.hasOwnProperty.call(table, key)) return table[key];
              return otherwise === undefined ? v : otherwise;
            }
            function __trim(v) { return __present(v) ? String(v).trim() : v; }
            function __upper(v) { return __present(v) ? String(v).toUpperCase() : v; }
            function __lower(v) { return __present(v) ? String(v).toLowerCase() : v; }
            function __affix(before, v, after) {
              return __present(v) ? before + String(v) + after : v;
            }
            function __replace(v, find, with_) {
              return __present(v) ? String(v).split(find).join(with_) : v;
            }
            function __truncate(v, limit) {
              if (!__present(v)) return v;
              var s = String(v);
              return s.length <= limit ? s : s.substring(0, limit);
            }
            function __string(v) { return __present(v) ? String(v) : v; }
            function __isNumber(v) { return __present(v) && v !== '' && !isNaN(Number(v)); }
            function __number(v) {
              if (!__present(v)) return v;
              var n = Number(v);
              if (isNaN(n)) throw new Error('"' + v + '" is not a number');
              return n;
            }
            function __integer(v) {
              if (!__present(v)) return v;
              var n = __number(v);
              if (Math.floor(n) !== n) throw new Error('"' + v + '" is not a whole number');
              return n;
            }
            function __boolean(v) {
              if (!__present(v)) return v;
              if (typeof v === 'boolean') return v;
              var s = String(v).toLowerCase();
              if (s === 'true' || s === '1' || s === 'yes' || s === 'y') return true;
              if (s === 'false' || s === '0' || s === 'no' || s === 'n') return false;
              throw new Error('"' + v + '" is not true or false');
            }
            function __date(v) {
              if (!__present(v)) return v;
              var d = new Date(v);
              if (isNaN(d.getTime())) throw new Error('"' + v + '" is not a date');
              return d.toISOString();
            }
            """;

    private static String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isMissingNode() || value.isNull() || value.asText().isBlank()
                ? null : value.asText().trim();
    }

    private static String number(JsonNode value, NodeDefinition node) {
        if (value == null || !value.isNumber()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "A rule in '" + node.name() + "' needs a number to compare against",
                    Map.of("nodeId", node.id()));
        }
        return value.asText();
    }

    private static String stringValue(JsonNode value, NodeDefinition node) {
        if (value == null || value.isNull() || value.asText().isBlank()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "A rule in '" + node.name() + "' needs a value",
                    Map.of("nodeId", node.id()));
        }
        return value.asText();
    }

    private static String list(JsonNode value, NodeDefinition node) {
        if (value == null || !value.isArray() || value.isEmpty()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "A ONE_OF rule in '" + node.name() + "' needs a list of allowed values",
                    Map.of("nodeId", node.id()));
        }
        List<String> allowed = new ArrayList<>();
        value.forEach(entry -> allowed.add(quote(entry.asText())));
        return "[" + String.join(",", allowed) + "]";
    }

    /**
     * A JavaScript string literal.
     *
     * <p>Every value reaching the generated source goes through this. A field name is user input,
     * and a field named {@code a'); doSomething(} would otherwise be code — inside a sandbox, so
     * the blast radius is one record's transform, but a mapping that silently becomes a different
     * mapping is a correctness problem regardless of what it can reach.
     */
    private static String quote(String value) {
        StringBuilder quoted = new StringBuilder(value.length() + 2).append('"');
        for (char c : value.toCharArray()) {
            switch (c) {
                case '"' -> quoted.append("\\\"");
                case '\\' -> quoted.append("\\\\");
                case '\n' -> quoted.append("\\n");
                case '\r' -> quoted.append("\\r");
                case '\t' -> quoted.append("\\t");
                default -> {
                    if (c < 0x20 || c == ' ' || c == ' ') {
                        quoted.append(String.format("\\u%04x", (int) c));
                    } else {
                        quoted.append(c);
                    }
                }
            }
        }
        return quoted.append('"').toString();
    }
}
