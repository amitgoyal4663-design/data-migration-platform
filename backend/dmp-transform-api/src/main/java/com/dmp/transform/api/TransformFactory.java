package com.dmp.transform.api;

import java.util.List;

/**
 * Compiles a pipeline's transform nodes.
 *
 * <p>The engine holds this rather than a script runtime, so a change of evaluator — or a
 * deployment with none at all — is a wiring change rather than a rewrite of the executor.
 */
public interface TransformFactory {

    /**
     * Compiles the given nodes into something runnable for the life of one chunk.
     *
     * <p>Compilation happens per chunk rather than per record, because parsing a script twenty
     * thousand times to run it twenty thousand times is the obvious way to make this slow.
     *
     * @param specs transform nodes in graph order, source first; may be empty
     * @return a transform to close when the chunk ends; {@link RecordTransform#IDENTITY} when
     *         there is nothing to do
     * @throws TransformException if a script does not compile — raised here, at the start of the
     *         chunk, rather than on the first record
     */
    RecordTransform compile(List<TransformSpec> specs);

    /**
     * Verifies a script compiles and behaves, without running a migration.
     *
     * <p>Exists so the console can tell a user their script is broken while they are writing it.
     * Finding out at 3am when the nightly run fails is the alternative.
     *
     * @param spec   the node to check
     * @param sample a record to run it against, or null to compile only
     * @return what the script produced, or the failure
     */
    TestResult test(TransformSpec spec, com.fasterxml.jackson.databind.JsonNode sample);

    /**
     * The outcome of a trial run.
     *
     * @param ok       whether the script compiled and ran
     * @param output   what it produced, or null on failure
     * @param message  the failure, phrased for the person who wrote the script
     * @param elapsedMillis how long one invocation took, so a user can see a slow script
     */
    record TestResult(boolean ok, com.fasterxml.jackson.databind.JsonNode output, String message,
                      long elapsedMillis) {

        public static TestResult success(com.fasterxml.jackson.databind.JsonNode output,
                                         long elapsedMillis) {
            return new TestResult(true, output, null, elapsedMillis);
        }

        public static TestResult failure(String message) {
            return new TestResult(false, null, message, 0);
        }
    }
}
