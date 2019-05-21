/*-
 * Copyright (C) 2011, 2018 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.impl.util;

import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Utility methods for performance tuning.
 */
public class PerfUtil {

    /**
     * A utility for profiling the time spent on sections of a method.
     *
     * To use instrument a method with:
     *      MethodPerf perf = new MethodPerf("methodName");
     *      perf.tag("section1");
     *      ...
     *      perf.endTag().tag("section2");
     *      ...
     *      perf.endTag().print();
     *
     * This utility is expected to be used for debugging purpose only.
     */
    public class MethodPerf {

        private final StringBuilder perfResult;
        private final long startTime;

        private Section currSection;

        /**
         * Constructs the perf.
         */
        public MethodPerf(String methodName) {
            this(methodName, "<MethodPerf>");
        }

        public MethodPerf(String methodName, String prefix) {
            this.perfResult = new StringBuilder(prefix);
            this.startTime = System.currentTimeMillis();
            perfResult.append(methodName).append(" ").
                append("start=").append(startTime);
        }

        /**
         * Tags the start of a section.
         */
        public void tag(String sectionTag) {
            currSection = new Section(sectionTag);
        }

        /**
         * Tags the end of the previous section.
         */
        public MethodPerf tagEnd() {
            if (currSection == null) {
                throw new IllegalStateException("no section tagged");
            }
            currSection.end();
            perfResult.append(" ").append(currSection);
            currSection = null;
            return this;
        }

        /**
         * Prints the result to system out.
         */
        public void print() {
            System.out.println(perfResult);
        }

        /**
         * Prints the result to the logger.
         */
        public void print(Logger logger, Level level) {
            logger.log(level, perfResult.toString());
        }

        /**
         * A section of a method.
         */
        public class Section {
            private final String tag;
            private final long start;
            private long end;

            private Section(String tag) {
                this.tag = tag;
                this.start = System.nanoTime();
            }

            private void end() {
                end = System.nanoTime();
            }

            @Override
            public String toString() {
                return String.format("%s=%.2f", tag, (end - start) / 1e6);
            }
        }
    }
}

