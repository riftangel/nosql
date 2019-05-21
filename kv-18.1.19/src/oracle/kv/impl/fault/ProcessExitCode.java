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

package oracle.kv.impl.fault;

/**
 * The enumeration of process exit codes used to communicate between a process
 * and its handler, e.g. the SNA, some shell script, etc.
 * <p>
 * Process exit codes must be in the range [0-255] and must not be one of the
 * following: 1-2, 64-113 (C/C++ standard), 126 - 165, and 255 since they are
 * reserved and have special meaning.
 */
public enum ProcessExitCode {

    RESTART() {

        @Override
        public short getValue() {
            return 200;
        }

        @Override
        public boolean needsRestart() {
            return true;
        }
    },

    NO_RESTART{

        @Override
        public short getValue() {
            return 201;
        }

        @Override
        public boolean needsRestart() {
            return false;
        }
    },

    /*
     * It's a variant of RESTART indicating that the process needs to be
     * started due to an OOME. It's a distinct OOME so that the SNA can log
     * this fact because the managed service can't.
     */
    RESTART_OOME {
        @Override
        public short getValue() {
            return 202;
        }

        @Override
        public boolean needsRestart() {
            return true;
        }
    };

    /**
     * Returns the numeric value associated with the process exit code.
     */
    public abstract short getValue();

    public abstract boolean needsRestart();

    /**
     * Returns true if the process exit code indicates that the process needs
     * to be restarted, or if the exit code is not one of the know exit codes
     * from the above enumeration.
     */
    static public boolean needsRestart(int exitCode) {
        for (ProcessExitCode v : ProcessExitCode.values()) {
            if (exitCode == v.getValue()) {
                return v.needsRestart();
            }
        }

        /*
         * Some unknown exitCode. Opt for availability, restart the process.
         * If its a recurring error, the SNA will eventually shut down the
         * managed service.
         */
        return true;
    }
}
