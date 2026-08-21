package com.dmp.connector.runtime;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

/**
 * Child-first classloader isolating one connector's dependencies.
 *
 * <p>Child-first rather than the JVM's default parent-first, and the difference matters. With
 * parent-first, every connector shares one global dependency resolution: the Oracle driver and the
 * MongoDB driver eventually disagree about a transitive Netty or Guava version, and the platform
 * has to pin one — breaking whichever connector wanted the other. Child-first means each connector
 * gets the versions it was built against, and a conflict stays inside the connector that caused it.
 *
 * <p>Only three things are loaded from the parent, and the list is deliberately short: the JDK, the
 * connector API, and the shared JSON model. Everything crossing the boundary must be loaded by the
 * same classloader on both sides, or a {@code DataRecord} handed to a connector would be a
 * different class from the {@code DataRecord} the engine created — the classic and thoroughly
 * confusing {@code ClassCastException} between two identically named types.
 *
 * <p>Hot reload is not supported. Discarding a classloader whose classes are still referenced leaks
 * the entire loaded graph, and doing it safely needs machinery that a worker restart makes
 * unnecessary.
 */
public final class PluginClassLoader extends URLClassLoader {

    /**
     * Packages always resolved by the parent.
     *
     * <p>These are the types that travel between the engine and a connector. A connector bundling
     * its own copy of Jackson would otherwise produce a {@code JsonNode} the engine cannot accept.
     */
    private static final List<String> PARENT_FIRST_PREFIXES = List.of(
            "java.",
            "javax.",
            "jdk.",
            "sun.",
            "com.dmp.connector.api.",
            "com.dmp.common.",
            "com.fasterxml.jackson.",
            "org.slf4j.");

    private final String pluginName;

    static {
        registerAsParallelCapable();
    }

    public PluginClassLoader(String pluginName, URL[] jars, ClassLoader parent) {
        super("plugin-" + pluginName, jars, parent);
        this.pluginName = pluginName;
    }

    public String pluginName() {
        return pluginName;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> loaded = findLoadedClass(name);
            if (loaded != null) {
                return resolved(loaded, resolve);
            }

            if (isParentFirst(name)) {
                return resolved(super.loadClass(name, false), resolve);
            }

            try {
                // Child-first: prefer the connector's own jar before consulting the parent.
                return resolved(findClass(name), resolve);
            } catch (ClassNotFoundException notInPlugin) {
                return resolved(super.loadClass(name, false), resolve);
            }
        }
    }

    private Class<?> resolved(Class<?> type, boolean resolve) {
        if (resolve) {
            resolveClass(type);
        }
        return type;
    }

    private static boolean isParentFirst(String className) {
        for (String prefix : PARENT_FIRST_PREFIXES) {
            if (className.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}
