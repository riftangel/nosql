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

package oracle.kv.util.shell;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import oracle.kv.util.shell.Shell.CommandHistory;

public class ShellInputReader {
    /**
     * Used APIs for Jline2.0
     **/
    private static final String JL2_READER_CLS = "jline.console.ConsoleReader";
    private static final String JL2_TERMINAL_CLS = "jline.Terminal";
    private static final String JL2_FILE_HISTORY_CLS =
        "jline.console.history.FileHistory";
    private static final String JL2_HISTORY_CLS =
        "jline.console.history.History";
    private static final String METHOD_SET_HISTORY = "setHistory";

    private static final int MID_JL2_CR_READLINE = 0x00;
    private static final int MID_JL2_CR_GETTERM = 0x01;
    private static final int MID_JL2_CR_SETEXPANDEVENTS = 0x02;
    private static final String[] METHODS_JL2_READER = {
        "readLine", "getTerminal", "setExpandEvents"
    };
    private static final int MID_JL2_TERM_GETHEIGHT = 0x00;
    private static final String[] METHODS_JL2_TERM = {
        "getHeight"
    };
    private static final int MID_JL2_FILE_HISTORY_SETMAXSIZE = 0x00;
    private static final int MID_JL2_FILE_HISTORY_FLUSH = 0x01;
    private static final int MID_JL2_FILE_HISTORY_SIZE = 0x02;
    private static final int MID_JL2_FILE_HISTORY_GET = 0x03;
    private static final String[] METHODS_JL2_FILE_HISTORY = {
        "setMaxSize", "flush", "size", "get"
    };
    private static final MethodDef[] JL2ReaderMethodsDef = {
        new MethodDef(METHODS_JL2_READER[MID_JL2_CR_READLINE],
                      new Class[]{String.class, Character.class}),
        new MethodDef(METHODS_JL2_READER[MID_JL2_CR_GETTERM], null),
        new MethodDef(METHODS_JL2_READER[MID_JL2_CR_SETEXPANDEVENTS],
                      new Class[]{boolean.class})
    };
    private static final MethodDef[] JL2TermMethodsDef = {
        new MethodDef(METHODS_JL2_TERM[MID_JL2_TERM_GETHEIGHT], null)
    };
    private static final MethodDef[] JL2FileHistoryMethodsDef = {
        new MethodDef(METHODS_JL2_FILE_HISTORY[MID_JL2_FILE_HISTORY_SETMAXSIZE],
                      new Class[]{int.class}),
        new MethodDef(METHODS_JL2_FILE_HISTORY[MID_JL2_FILE_HISTORY_FLUSH],
                      null),
        new MethodDef(METHODS_JL2_FILE_HISTORY[MID_JL2_FILE_HISTORY_SIZE],
                      null),
        new MethodDef(METHODS_JL2_FILE_HISTORY[MID_JL2_FILE_HISTORY_GET],
                      new Class[]{int.class})
    };
    /* Default value for terminal height */
    private static final int TERMINAL_HEIGHT_DEFAULT = 25;

    /* Property name to disable JLine. */
    private static final String PROP_JLINE_DISABLE =
        "oracle.kv.shell.jline.disable";
    /* Property name of JLine history file. */
    private static final String PROP_HISTORY_FILE =
        "oracle.kv.shell.history.file";
    /* Property name of JLine history size. */
    private static final String PROP_HISTORY_SIZE =
        "oracle.kv.shell.history.size";

    private Object jReaderObj = null;
    private Object jFileHistoryObj = null;
    private BufferedReader inputReader = null;
    private PrintStream output = null;
    private Map<String, Method> jReaderMethods = null;
    private Map<String, Method> jTermMethods = null;
    private Map<String, Method> jFileHistoryMethods = null;
    private String prompt = "";

    public ShellInputReader(InputStream input,
                            PrintStream output) {
        this(input, output, null, true, null);
    }

    public ShellInputReader(Shell shell) {
        this(shell.input, shell.output, getHistoryFile(shell),
             shell.isJlineEventDesignatorDisabled(), shell.getMaskFlags());
        loadCommandHistory(shell);
    }

    public ShellInputReader(InputStream input,
                            PrintStream output,
                            File historyFile,
                            boolean disableExpandEvents,
                            String[] maskFlags) {
        initInputReader(input, output, historyFile,
                        disableExpandEvents, maskFlags);
        this.output = output;
    }

    private void initInputReader(InputStream input,
                                 PrintStream output1,
                                 File historyFile,
                                 boolean disableExpandEvents,
                                 String[] maskFlags) {
        if (isJlineCompatiblePlatform()) {
            try {
                Class<?> jReader = Class.forName(JL2_READER_CLS);
                Class<?> jTerminal = Class.forName(JL2_TERMINAL_CLS);
                Class<?> jFileHistory = Class.forName(JL2_FILE_HISTORY_CLS);
                jReaderMethods = new HashMap<String, Method>();
                for (MethodDef mdef: JL2ReaderMethodsDef) {
                    Method mtd = jReader.getMethod(mdef.getName(),
                                                   mdef.getArgTypes());
                    jReaderMethods.put(mdef.getName(), mtd);
                }

                jTermMethods = new HashMap<String, Method>();
                for (MethodDef mdef: JL2TermMethodsDef) {
                    Method mtd = jTerminal.getMethod(mdef.getName(),
                                                     mdef.getArgTypes());
                    jTermMethods.put(mdef.getName(), mtd);
                }

                jFileHistoryMethods = new HashMap<String, Method>();
                for (MethodDef mdef: JL2FileHistoryMethodsDef) {
                    Method mtd = jFileHistory.getMethod(mdef.getName(),
                                                        mdef.getArgTypes());
                    jFileHistoryMethods.put(mdef.getName(), mtd);
                }
                /* Initialize ConsoleReader instance. */
                Constructor<?> csr = jReader.getConstructor(InputStream.class,
                                                            OutputStream.class);
                jReaderObj = csr.newInstance(input, output1);

                /* Initialize FileHistory instance. */
                if (historyFile != null) {
                    csr = jFileHistory.getConstructor(File.class);
                    jFileHistoryObj = csr.newInstance(historyFile);
                    Object historyObj = (maskFlags == null) ? jFileHistoryObj :
                        FileHistoryProxy.create(jFileHistoryObj, maskFlags);
                    invokeMethod(jReaderObj,
                        jReader.getMethod(METHOD_SET_HISTORY,
                            new Class[]{Class.forName(JL2_HISTORY_CLS)}),
                        new Object[]{historyObj});
                    setHistoryFileSize();
                }

                if (disableExpandEvents) {
                    /**
                     * Disable the event designators, it is enabled by
                     * default.
                     */
                    enableExpandEvents(false);
                }
            } catch (Exception ignored)  /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
        if (jReaderObj == null) {
            /* Use normal inputStreamReader if failed to load jline library */
            inputReader = new BufferedReader(new InputStreamReader(input));
        }
    }

    /* Load command history to shell.CommandHistory. */
    private void loadCommandHistory(Shell shell) {
        if (jFileHistoryObj == null) {
            return;
        }
        CommandHistory history = shell.getHistory();
        try {
            int size = getHistorySize();
            if (size == 0) {
                return;
            }
            for (int i = 0; i < size; i++) {
                history.add(getHistoryCommand(i), null);
            }
        } catch (IOException ignored) /* CHECKSTYLE:OFF */  {
            /* Continue if loading command history from history file failed. */
        } /* CHECKSTYLE:ON */
    }

    /* Get number of commands in the history file. */
    private int getHistorySize()
        throws IOException {

        return (Integer)invokeJFileHistoryMethod(jFileHistoryObj,
            METHODS_JL2_FILE_HISTORY[MID_JL2_FILE_HISTORY_SIZE], null);
    }

    /* Get nth command in the history file. */
    private String getHistoryCommand(int index)
        throws IOException {

        CharSequence cs =
            (CharSequence)invokeJFileHistoryMethod(jFileHistoryObj,
            METHODS_JL2_FILE_HISTORY[MID_JL2_FILE_HISTORY_GET],
            new Object[]{Integer.valueOf(index)});
        return cs.toString();
    }

    /**
     * Return the file for JLine commands history.
     *
     * If the property is not set, return the default path
     * <user-home>/.jline-<main-class-name>.history.
     * Return null if the specified file is not readable or writable.
     */
    private static File getHistoryFile(Shell shell) {
        String path = System.getProperty(PROP_HISTORY_FILE);
        File file = null;
        if (path != null) {
            file = new File(path);
        } else {
            file = new File(System.getProperty("user.home"),
                        ".jline-" + shell.getClass().getName() + ".history");
        }
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                shell.verboseOutput("Failed to create the command line " +
                    "history file: " + file.getAbsolutePath() +
                    ", command history will be stored in memory.");
                return null;
            }
        } else {
            if (!file.canRead() || !file.canWrite()) {
                shell.verboseOutput("Cannot access the command line " +
                    "history file: " + file.getAbsolutePath() +
                    ", command history will be stored in memory.");
                return null;
            }
        }
        return file;
    }

    private void setHistoryFileSize()
        throws IOException {

        String historySize = System.getProperty(PROP_HISTORY_SIZE);
        if (historySize == null || jFileHistoryObj == null) {
            return;
        }

        int maxSize;
        try {
            maxSize = Integer.valueOf(historySize);
        } catch (NumberFormatException nfe) {
            return;
        }
        invokeJFileHistoryMethod(jFileHistoryObj,
            METHODS_JL2_FILE_HISTORY[MID_JL2_FILE_HISTORY_SETMAXSIZE],
            new Object[]{Integer.valueOf(maxSize)});
    }

    public void shutdown() {
        if (jFileHistoryObj != null) {
            try {
                invokeJFileHistoryMethod(jFileHistoryObj,
                    METHODS_JL2_FILE_HISTORY[MID_JL2_FILE_HISTORY_FLUSH], null);
            } catch (IOException ignored) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
    }

    private boolean isJlineCompatiblePlatform() {
        /* Check system property that whether jline is disabled. */
        if (Boolean.getBoolean(PROP_JLINE_DISABLE)) {
            return false;
        }

        String os = System.getProperty("os.name").toLowerCase();
        if (os.indexOf("windows") != -1) {
            /**
             * Disable jline on Windows because of a Cygwin problem:
             * https://github.com/jline/jline2/issues/62
             * This will be fixed in a later patch.
             */
            return false;
        }
        return true;
    }

    public void setDefaultPrompt(String prompt) {
        this.prompt = prompt;
    }

    public String getDefaultPrompt() {
        return this.prompt;
    }

    public String readLine()
        throws IOException {

        return readLine(null);
    }

    public String readLine(String promptString)
        throws IOException {

        String promptStr = (promptString != null)? promptString: this.prompt;
        if (jReaderObj != null) {
            String name = METHODS_JL2_READER[MID_JL2_CR_READLINE];
            return (String)invokeJReaderMethod(jReaderObj, name,
                                               new Object[]{promptStr, null});
        }
        if (promptStr != null) {
            output.print(promptStr);
        }
        return inputReader.readLine();
    }

    public char[] readPassword(String promptString) throws IOException {
        String input = null;
        final String pwdPrompt = (promptString != null) ? promptString :
                                                          this.prompt;

        if (jReaderObj != null) {
            final String name = METHODS_JL2_READER[MID_JL2_CR_READLINE];
            input = (String)invokeJReaderMethod(
                jReaderObj, name, new Object[]{pwdPrompt,
                                               new Character((char) 0)});
            return input == null ? null : input.toCharArray();
        }

        final Console console = System.console();
        if (console != null) {
            return console.readPassword(pwdPrompt);
        }

        output.print(pwdPrompt);
        input = inputReader.readLine();
        return input == null ? null : input.toCharArray();
    }

    public int getTerminalHeight() {
        if (jReaderObj != null) {
            try {
                String name = METHODS_JL2_READER[MID_JL2_CR_GETTERM];
                Object jTermObj = invokeJReaderMethod(jReaderObj, name, null);
                name = METHODS_JL2_TERM[MID_JL2_TERM_GETHEIGHT];
                return (Integer)invokeJTermMethod(jTermObj, name, null);
            } catch (IOException ignored)  /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
        return getTermHeightImpl();
    }

    private void enableExpandEvents(boolean enable) {
        if (jReaderObj != null) {
            String name = METHODS_JL2_READER[MID_JL2_CR_SETEXPANDEVENTS];
            try {
                invokeJReaderMethod(jReaderObj, name,
                                    new Object[]{Boolean.valueOf(enable)});
            } catch (IOException ignored)  /* CHECKSTYLE:OFF */ {
            }
        }
    }

    private Object invokeJReaderMethod(Object jReaderObj1,
                                       String name,
                                       Object[] args)
        throws IOException {

        Method method = null;
        if (!jReaderMethods.containsKey(name)) {
            throw new IOException("Method " + name +
                " of Jline.ConsoleReader is not initialized.");
        }
        method = jReaderMethods.get(name);
        return invokeMethod(jReaderObj1, method, args);
    }

    private Object invokeJTermMethod(Object jTermObj,
                                     String name,
                                     Object[] args)
        throws IOException {

        Method method = null;
        if (!jTermMethods.containsKey(name)) {
            throw new IOException("Method " + name + " of " +
                                  JL2_TERMINAL_CLS + " is not initialized.");
        }
        method = jTermMethods.get(name);
        return invokeMethod(jTermObj, method, args);
    }

    private Object invokeJFileHistoryMethod(Object obj,
                                            String name,
                                            Object[] args)
        throws IOException {

        Method method = null;
        if (!jFileHistoryMethods.containsKey(name)) {
            throw new IOException("Method " + name + " of " +
                                 JL2_FILE_HISTORY_CLS + " is not initialized.");
        }
        method = jFileHistoryMethods.get(name);
        return invokeMethod(obj, method, args);
    }

    private Object invokeMethod(Object obj, Method method, Object[] args)
        throws IOException {

        String name = method.getName();
        try {
            return method.invoke(obj, args);
        } catch (IllegalAccessException iae) {
            throw new IOException("Invoke method " + name +
                                  " of Jline.ConsoleReader failed", iae);
        } catch (IllegalArgumentException iae) {
            throw new IOException("Invoke method " + name +
                                  " of Jline.ConsoleReader failed", iae);
        } catch (InvocationTargetException ite) {
            String msg = ((ite.getCause() != null) ?
                          ite.getCause().getMessage() : ite.getMessage());
            throw new IOException("Invoke method " + name +
                                  " of Jline.ConsoleReader failed: " + msg,
                                  ite);
        }
    }

    private int getTermHeightImpl() {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.indexOf("windows") != -1) {
            return TERMINAL_HEIGHT_DEFAULT;
        }
        int height = getUnixTermHeight();
        if (height == -1) {
            height = TERMINAL_HEIGHT_DEFAULT;
        }
        return height;
    }

    /*
     * stty -a
     *  speed 38400 baud; rows 48; columns 165; line = 0; ...
     */
    private int getUnixTermHeight() {
        String ttyProps = null;
        final String name = "rows";
        try {
            ttyProps = getTermSttyProps();
            if (ttyProps != null && ttyProps.length() > 0) {
                return getTermSttyPropValue(ttyProps, name);
            }
        } catch (IOException ignored)  /* CHECKSTYLE:OFF */ {
        } catch (InterruptedException ignored) {
        } /* CHECKSTYLE:ON */
        return -1;
    }

    private String getTermSttyProps()
        throws IOException, InterruptedException {

        String[] cmd = {"/bin/sh", "-c", "stty -a </dev/tty"};
        Process proc = Runtime.getRuntime().exec(cmd);

        String s = null;
        StringBuilder sb = new StringBuilder();
        BufferedReader stdInput =
            new BufferedReader(new InputStreamReader(proc.getInputStream()));
        while ((s = stdInput.readLine()) != null) {
            sb.append(s);
        }

        BufferedReader stdError =
            new BufferedReader(new InputStreamReader(proc.getErrorStream()));
        while ((s = stdError.readLine()) != null) {
            sb.append(s);
        }

        proc.waitFor();
        return sb.toString();
    }

    private int getTermSttyPropValue(String props, String name) {
        StringTokenizer tokenizer = new StringTokenizer(props, ";\n");
        while (tokenizer.hasMoreTokens()) {
            String str = tokenizer.nextToken().trim();
            if (str.startsWith(name)) {
                return Integer.parseInt(
                        str.substring(name.length() + 1, str.length()));
            } else if (str.endsWith(name)) {
                return Integer.parseInt(
                        str.substring(0, (str.length() - name.length() - 1)));
            }
        }
        return 0;
    }

    private static class MethodDef {
        private final String name;
        private final Class<?>[] argsTypes;

        MethodDef(String name, Class<?>[] types) {
            this.name = name;
            this.argsTypes = types;
        }

        public String getName() {
            return this.name;
        }

        public Class<?>[] getArgTypes() {
            return this.argsTypes;
        }
    }
}
