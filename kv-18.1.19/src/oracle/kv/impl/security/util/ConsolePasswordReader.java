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
package oracle.kv.impl.security.util;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * A simple implementation of PasswordReader based on java.io.Console.
 * If provided with a null console, falls back to System.in/out.
 */
public class ConsolePasswordReader implements PasswordReader {
    private final Console console;

    public ConsolePasswordReader() {
        this(System.console());
    }

    public ConsolePasswordReader(Console console) {
        this.console = console;
    }

    @Override
    public char[] readPassword(String prompt) throws IOException {
        if (console != null) {
            return console.readPassword(prompt);
        }

        System.out.print(prompt);
        System.out.flush();
        final InputStream in = System.in;
        final BufferedReader br = new BufferedReader(new InputStreamReader(in));
        final String input = br.readLine();
        return input == null ? null : input.toCharArray();
    }
}

