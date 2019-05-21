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

package oracle.kv.impl.admin.client;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.AdminStatus;
import oracle.kv.impl.admin.CommandJsonUtils;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.CommandResult.CommandFails;
import oracle.kv.impl.admin.CommandResult.CommandSucceeds;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.Snapshot;
import oracle.kv.impl.admin.criticalevent.CriticalEvent;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.StatusReport;
import oracle.kv.impl.api.avro.AvroDdl;
import oracle.kv.impl.api.avro.AvroDdl.SchemaSummary;
import oracle.kv.impl.api.avro.AvroSchemaMetadata;
import oracle.kv.impl.api.avro.AvroSchemaStatus;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableJsonUtils;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterState.Info;
import oracle.kv.impl.security.metadata.KVStoreUser.UserDescription;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.JsonUtils;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.impl.util.TopologyPrinter.Filter;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.table.Index;
import oracle.kv.table.Table;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;
import oracle.kv.util.shell.ShowCommandBase;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

/*
 * show and its subcommands
 */
class ShowCommand extends ShowCommandBase {
    private static final List<? extends SubCommand> subs =
                                       Arrays.asList(new ShowParameters(),
                                                     new ShowAdmins(),
                                                     new ShowEvents(),
                                                     new ShowFaults(),
                                                     new ShowIndexes(),
                                                     new ShowPerf(),
                                                     new ShowPlans(),
                                                     new ShowPools(),
                                                     new ShowSchemas(),
                                                     new ShowSnapshots(),
                                                     new ShowTables(),
                                                     new ShowTopology(),
                                                     new ShowDatacenters(),
                                                     new ShowUpgradeOrder(),
                                                     new ShowUsers(),
                                                     new ShowZones(),
                                                     new ShowVersions());

    private static final String SHOW_COMMAND_NAME = "show";

    ShowCommand() {
        super(subs);
    }

    /*
     * ShowParameters
     */
    private static final class ShowParameters extends SubCommand {

        private ShowParameters() {
            super("parameters", 4);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            return new ShowParameterExecutor<String>() {

                @Override
                public String parameterMapResult(ParameterMap map,
                                                 boolean showHidden,
                                                 Info info)
                    throws ShellException {
                    return CommandUtils.formatParams(
                               map, showHidden, info);
                }

                @Override
                public String snParamResult(boolean showHidden,
                                            Info info,
                                            StorageNodeParams snp)
                   throws ShellException {
                   String result =
                       CommandUtils.formatParams(snp.getMap(), showHidden,
                                                 info);
                   final ParameterMap storageDirMap =
                       snp.getStorageDirMap();
                   if (storageDirMap != null && !storageDirMap.isEmpty()) {
                       result += "Storage directories:" + eol;
                       for (Parameter param : storageDirMap) {
                           result +=
                               Shell.makeWhiteSpace(4) +
                               "path=" + param.getName() +
                               Shell.makeWhiteSpace(1) +
                               "size=" + param.asString() +
                               eol;
                       }
                   }
                   final ParameterMap rnLogDirMap =
                       snp.getRNLogDirMap();
                   if (rnLogDirMap != null && !rnLogDirMap.isEmpty()) {
                       result += "RN Log directories:" + eol;
                       for (Parameter param : rnLogDirMap) {
                           result +=
                               Shell.makeWhiteSpace(4) +
                               "path=" + param.getName() +
                               eol;
                       }
                   }
                   final ParameterMap adminDirMap =
                       snp.getAdminDirMap();
                   if (adminDirMap != null && !adminDirMap.isEmpty()) {
                        result += "Admin directory:" + eol;
                        for (Parameter param : adminDirMap) {
                            result +=
                                Shell.makeWhiteSpace(4) +
                                "path=" + param.getName() +
                                Shell.makeWhiteSpace(1) +
                                "size=" + param.asString() +
                                eol;
                        }
                    }
                   return result;
                }
            }.commonExecute(args, shell);
        }

        private abstract class ShowParameterExecutor<T>
            implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                /*
                 * parameters -policy | -global | -security | -service <name>
                 */
                if (args.length < 2) {
                    shell.badArgCount(ShowParameters.this);
                }
                Shell.checkHelp(args, ShowParameters.this);
                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();
                String serviceName = null;
                boolean isPolicy = false;
                boolean isSecurity = false;
                boolean isGlobal = false;

                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];
                    if ("-policy".equals(arg)) {
                        isPolicy = true;
                    } else if ("-service".equals(arg)) {
                        serviceName =
                            Shell.nextArg(args, i++, ShowParameters.this);
                    } else if ("-security".equals(arg)) {
                        isSecurity = true;
                    } else if ("-global".equals(arg)) {
                        isGlobal = true;
                    } else {
                        shell.unknownArgument(arg, ShowParameters.this);
                    }
                }

                if (((isPolicy ? 1 : 0) +
                     (isSecurity ? 1 : 0) +
                     (isGlobal ? 1 : 0)) > 1) {
                    throw new ShellUsageException(
                        "-policy, -global and -security cannot be used " +
                        "together",
                        ShowParameters.this);
                }
                if (isPolicy) {
                    if (serviceName != null) {
                        throw new ShellUsageException(
                            "-policy cannot be combined with a service",
                            ShowParameters.this);
                    }
                    try {
                        final ParameterMap map = cs.getPolicyParameters();
                        return parameterMapResult(
                            map, cmd.getHidden(), ParameterState.Info.POLICY);
                    } catch (RemoteException re) {
                        cmd.noAdmin(re);
                    }
                }
                if (isSecurity) {
                    if (serviceName != null) {
                        throw new ShellUsageException
                            ("-security cannot be combined with a service",
                             ShowParameters.this);
                    }
                    try {
                        final ParameterMap map =
                            cs.getParameters().getGlobalParams().
                                getGlobalSecurityPolicies();
                        return parameterMapResult(map, cmd.getHidden(), null);
                    } catch (RemoteException re) {
                        cmd.noAdmin(re);
                    }
                }
                if (isGlobal) {
                    if (serviceName != null) {
                        throw new ShellUsageException(
                            "-global cannot be combined with a service",
                            ShowParameters.this);
                    }
                    try {
                        final ParameterMap map =
                            cs.getParameters().getGlobalParams().
                                getGlobalComponentsPolicies();
                        return parameterMapResult(map, cmd.getHidden(), null);
                    } catch (RemoteException re) {
                        cmd.noAdmin(re);
                    }
                }

                if (serviceName == null) {
                    shell.requiredArg("-service|-policy|-security|-global",
                                      ShowParameters.this);
                }

                RepNodeId rnid = null;
                AdminId aid = null;
                ArbNodeId anid = null;
                StorageNodeId snid = null;
                try {
                    rnid = RepNodeId.parse(serviceName);
                } catch (IllegalArgumentException ignored) {
                    try {
                        snid = StorageNodeId.parse(serviceName);
                    } catch (IllegalArgumentException ignored1) {
                        try {
                            aid = AdminId.parse(serviceName);
                        } catch (IllegalArgumentException ignored2) {
                            try {
                                anid = ArbNodeId.parse(serviceName);
                            } catch (IllegalArgumentException ignored3) {
                                invalidArgument(serviceName);
                            }
                        }
                    }
                }

                Parameters p;
                try {
                    p = cs.getParameters();
                    if (rnid != null) {
                        final RepNodeParams rnp = p.get(rnid);
                        if (rnp == null) {
                            noSuchService(rnid);
                        } else {
                            /*
                             * TODO : Need to check if we need to show
                             * rn log dir as part of show parameter
                             * -service rgx-rny
                             */
                            return parameterMapResult(
                                rnp.getMap(), cmd.getHidden(),
                                ParameterState.Info.REPNODE);
                        }
                    } else if (snid != null) {
                        final StorageNodeParams snp = p.get(snid);
                        if (snp == null) {
                            noSuchService(snid);
                        } else {
                            return snParamResult(cmd.getHidden(),
                                                 ParameterState.Info.SNA,
                                                 snp);
                        }
                    } else if (aid != null) {
                        final AdminParams ap = p.get(aid);
                        if (ap == null) {
                            noSuchService(aid);
                        } else {
                            return parameterMapResult(
                                ap.getMap(), cmd.getHidden(),
                                ParameterState.Info.ADMIN);
                        }
                    } else if (anid != null) {
                        final ArbNodeParams anp = p.get(anid);
                        if (anp == null) {
                            noSuchService(anid);
                        } else {
                            return parameterMapResult(
                                anp.getMap(), cmd.getHidden(),
                                ParameterState.Info.ARBNODE);
                        }
                    }
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T
                parameterMapResult(ParameterMap map,
                                   boolean showHidden,
                                   ParameterState.Info info)
                throws ShellException;

            public abstract T snParamResult(boolean showHidden,
                                            ParameterState.Info info,
                                            StorageNodeParams snp)
                throws ShellException;
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args,
                                                    Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show parameters");
            return new ShowParameterExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    parameterMapResult(ParameterMap map,
                                       boolean showHidden,
                                       Info info)
                    throws ShellException {
                    scr.setReturnValue(
                        CommandUtils.formatParamsJson(map, showHidden, info));
                    return scr;
                }

                @Override
                public ShellCommandResult snParamResult(boolean showHidden,
                                                        Info info,
                                                        StorageNodeParams snp)
                    throws ShellException {
                    final ObjectNode snNode =
                        CommandUtils.formatParamsJson(snp.getMap(),
                                                      showHidden, info);
                    final ParameterMap storageDirMap = snp.getStorageDirMap();
                    if (storageDirMap != null && !storageDirMap.isEmpty()) {
                        final ArrayNode storageDirArray =
                            snNode.putArray("storageDirs");
                        for (Parameter param : storageDirMap) {
                            final ObjectNode dirNode =
                                JsonUtils.createObjectNode();
                            dirNode.put("path", param.getName());
                            dirNode.put("size", param.asString());
                            storageDirArray.add(dirNode);
                        }
                    }
                    final ParameterMap rnLogDirMap = snp.getRNLogDirMap();
                    if (rnLogDirMap != null && !rnLogDirMap.isEmpty()) {
                        final ArrayNode rnLogDirArray =
                            snNode.putArray("rnlogDirs");
                        for (Parameter param : rnLogDirMap) {
                            final ObjectNode dirNode =
                                JsonUtils.createObjectNode();
                            dirNode.put("path", param.getName());
                            rnLogDirArray.add(dirNode);
                        }
                    }
                    final ParameterMap adminDirMap = snp.getAdminDirMap();
                    if (adminDirMap != null && !adminDirMap.isEmpty()) {
                        final ArrayNode adminDirArray =
                            snNode.putArray("adminDirs");
                        for (Parameter param : adminDirMap) {
                            final ObjectNode dirNode =
                                JsonUtils.createObjectNode();
                            dirNode.put("path", param.getName());
                            dirNode.put("size", param.asString());
                            adminDirArray.add(dirNode);
                        }
                    }
                    scr.setReturnValue(snNode);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "show parameters -policy | -global | -security |" +
                   " -service <name> " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Displays service parameters and state for the specified " +
                "service." + eolt + "The service may be a RepNode, " +
                "StorageNode, or Admin service," + eolt + "as identified " +
                "by any valid string, for example" + eolt + "rg1-rn1, sn1, " +
                "admin2, etc.  Use the -policy flag to show global policy" +
                eolt + "default parameters. Use the -security flag to show global " +
                "security parameters. Use the -global flag to show global " +
                "component parameters.";
        }

        private void noSuchService(ResourceId rid)
            throws ShellException {

            throw new ShellArgumentException
                ("No such service: " + rid);
        }
    }

    /*
     * ShowAdmins
     */
    private static final class ShowAdmins extends SubCommand {

        private ShowAdmins() {
            super("admins", 2);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            return new ShowAdminExecutor<String>() {

                @Override
                public String adminsResult(List<ParameterMap> admins,
                                           Topology t,
                                           String currentAdminHost,
                                           int currentAdminPort,
                                           RegistryUtils registryUtils)
                    throws ShellException {
                    final StringBuilder sb = new StringBuilder();
                    sb.append("");
                    for (ParameterMap map : admins) {
                        final AdminParams params = new AdminParams(map);
                        final StorageNodeId snId =
                            params.getStorageNodeId();
                        sb.append(params.getAdminId());
                        sb.append(": Storage Node ").append(snId);
                        sb.append(" type=").append(params.getType());

                        sb.append(" (");
                        final StorageNode sn = t.get(snId);
                        if (currentAdminHost.equals(sn.getHostname()) &&
                            currentAdminPort == sn.getRegistryPort()) {
                            sb.append("connected ");
                        }
                        AdminStatus adminStatus;
                        try {
                            adminStatus =
                                registryUtils.getAdmin(snId).
                                getAdminStatus();
                        } catch (Exception e) {
                            adminStatus = null;
                        }
                        if (adminStatus == null) {
                            sb.append("UNREACHABLE");
                        } else {
                            sb.append(adminStatus.getServiceStatus());
                            final State state =
                                adminStatus.getReplicationState();
                            if (state != null) {
                                sb.append(",").append(state);
                                if (state.isMaster() &&
                                    !adminStatus.
                                        getIsAuthoritativeMaster()) {
                                    sb.append(" (non-authoritative)");
                                }
                            }
                        }
                        sb.append(")");
                        sb.append(eol);
                    }
                    return sb.toString();
                }
            }.commonExecute(args, shell);
        }

        private abstract class ShowAdminExecutor<T>
            implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, ShowAdmins.this);
                if (args.length > 2) {
                    shell.badArgCount(ShowAdmins.this);
                }
                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();

                final String currentAdminHost = cmd.getAdminHostname();
                final int currentAdminPort = cmd.getAdminPort();

                try {
                    final List<ParameterMap> admins = cs.getAdmins();
                    final Topology t = cs.getTopology();
                    final RegistryUtils registryUtils = new RegistryUtils(t,
                        cmd.getLoginManager());
                    return adminsResult(admins, t, currentAdminHost,
                        currentAdminPort, registryUtils);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T adminsResult(List<ParameterMap> admins,
                                           Topology t,
                                           String currentAdminHost,
                                           int currentAdminPort,
                                           RegistryUtils registryUtils)
                throws ShellException;
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show admins");
            return new ShowAdminExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    adminsResult(List<ParameterMap> admins, Topology t,
                                 String currentAdminHost, int currentAdminPort,
                                 RegistryUtils registryUtils)
                    throws ShellException {

                    final ObjectNode top = JsonUtils.createObjectNode();
                    final ArrayNode adminArray = top.putArray("admins");
                    for (ParameterMap map : admins) {
                        final ObjectNode adminNode =
                            JsonUtils.createObjectNode();
                        final AdminParams params = new AdminParams(map);
                        final StorageNodeId snId = params.getStorageNodeId();
                        adminNode.put(
                            "adminId", params.getAdminId().toString());
                        adminNode.put("snId", snId.toString());
                        adminNode.put("type", params.getType().toString());

                        adminNode.put("connected", false);
                        final StorageNode sn = t.get(snId);
                        if (currentAdminHost.equals(sn.getHostname()) &&
                            currentAdminPort == sn.getRegistryPort()) {
                            adminNode.put("connected", true);
                        }

                        AdminStatus adminStatus;
                        try {
                            adminStatus =
                                registryUtils.getAdmin(snId).getAdminStatus();
                        } catch (Exception e) {
                            adminStatus = null;
                        }
                        if (adminStatus == null) {
                            adminNode.put("adminStatus", "UNREACHABLE");
                        } else {
                            adminNode.put("adminStatus",
                                          adminStatus.getServiceStatus().
                                              toString());
                            final State state =
                                adminStatus.getReplicationState();
                            if (state != null) {
                                adminNode.put(
                                    "replicationState", state.toString());
                                adminNode.put("authoritative", true);
                                if (state.isMaster() &&
                                    !adminStatus.getIsAuthoritativeMaster()) {
                                    adminNode.put("authoritative", false);
                                }
                            }
                        }
                        adminArray.add(adminNode);
                    }
                    scr.setReturnValue(top);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "show admins " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Displays basic information about deployed Admin services.";
        }
    }

    /*
     * ShowSchemas
     */
    static final class ShowSchemas extends SubCommand {

        private ShowSchemas() {
            super("schemas", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new ShowSchemaExecutor<String>() {

                @Override
                public String emptySchemaResult() {
                    return "";
                }

                @Override
                public String
                    multiSchemasResult(SortedMap<String,
                                       SchemaSummary> map)
                    throws ShellException {
                    final StringBuilder builder =
                        new StringBuilder(map.size() * 100);

                    for (final AvroDdl.SchemaSummary summary :
                         map.values()) {
                        if (builder.length() > 0) {
                            builder.append(eol);
                        }
                        builder.append(summary.getName());
                        formatAvroSchemaSummary(summary, builder);
                        AvroDdl.SchemaSummary prevVersion =
                            summary.getPreviousVersion();
                        while (prevVersion != null) {
                            formatAvroSchemaSummary(prevVersion, builder);
                            prevVersion = prevVersion.getPreviousVersion();
                        }
                    }
                    return builder.toString();
                }

                @Override
                public String singleSchemaResult(String schema,
                                                 CommandServiceAPI cs,
                                                 CommandShell cmd)
                    throws ShellException {
                    return showSingleSchema(schema, cs, cmd);
                }

            }.commonExecute(args, shell);
        }

        private abstract class ShowSchemaExecutor<T> implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, ShowSchemas.this);
                if (args.length > 3) {
                    shell.badArgCount(ShowSchemas.this);
                }

                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();

                try {

                    if (args.length == 3) {
                        if ("-name".equals(args[1])) {
                            return singleSchemaResult(args[2], cs, cmd);
                        }
                        shell.unknownArgument(args[1], ShowSchemas.this);
                    }
                    final boolean includeDisabled;
                    if (args.length == 2) {
                        if (!"-disabled".equals(args[1])) {
                            shell.unknownArgument(args[1], ShowSchemas.this);
                        }
                        includeDisabled = true;
                    } else {
                        includeDisabled = false;
                    }

                    final SortedMap<String, AvroDdl.SchemaSummary> map =
                        cs.getSchemaSummaries(includeDisabled);

                    if (map.isEmpty()) {
                        return emptySchemaResult();
                    }

                    return multiSchemasResult(map);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T emptySchemaResult();

            public abstract T
                singleSchemaResult(String schema,
                                   CommandServiceAPI cs,
                                   CommandShell cmd)
                throws ShellException, RemoteException;

            public abstract T
                multiSchemasResult(SortedMap<String, SchemaSummary> map)
                throws ShellException;

        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show schemas");
            final ObjectNode top = JsonUtils.createObjectNode();
            final ArrayNode schemaArray = top.putArray("schemas");
            return new ShowSchemaExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult emptySchemaResult() {
                    scr.setDescription("No schema.");
                    return scr;
                }

                @Override
                public ShellCommandResult
                    singleSchemaResult(String schema,
                                       CommandServiceAPI cs,
                                       CommandShell cmd)
                    throws ShellException, RemoteException {
                    final ObjectNode on = JsonUtils.createObjectNode();
                    final int id =
                        parseSchemaNameAndId(schema, false /*idRequired*/,
                                             cs, ShowSchemas.this);
                    final AvroDdl.SchemaDetails details =
                        cs.getSchemaDetails(id);
                    on.put("schemaId", details.getId());
                    on.put(name, details.getName());
                    on.put("status",
                           details.getMetadata().getStatus().name());
                    on.put("modified", FormatUtils.formatDateAndTime(
                        details.getMetadata().getTimeModified()));
                    on.put("from", details.getMetadata().getFromMachine());
                    if (!details.getMetadata().getByUser().isEmpty()) {
                        on.put("by", details.getMetadata().getByUser());
                    }

                    schemaArray.add(on);
                    scr.setReturnValue(top);
                    return scr;
                }

                @Override
                public ShellCommandResult
                    multiSchemasResult(SortedMap<String,
                                       SchemaSummary> map)
                    throws ShellException {

                    for (final AvroDdl.SchemaSummary summary :
                         map.values()) {
                        schemaArray.add(
                            formatAvroSchemaSummaryJson(summary));
                    }
                    scr.setReturnValue(top);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "show schemas [-disabled] | [-name <name>] " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Displays schema details of the named schema or a list of " +
                "schemas" + eolt + "registered with the store. The " +
                "-disabled flag enables listing of" + eolt + "disabled " +
                "schemas.";
        }

        private void formatAvroSchemaSummary(AvroDdl.SchemaSummary summary,
                                             StringBuilder builder) {
            builder.append(eol).append("  ID: ").append(summary.getId());
            final AvroSchemaMetadata metadata = summary.getMetadata();
            if (metadata.getStatus() == AvroSchemaStatus.DISABLED) {
                builder.append(" (disabled)");
            }
            builder.append("  Modified: ").append
                (FormatUtils.formatDateAndTime(metadata.getTimeModified()));
            builder.append(", From: ").append(metadata.getFromMachine());
            if (!metadata.getByUser().isEmpty()) {
                builder.append(", By: ").append(metadata.getByUser());
            }
        }

        private ObjectNode
            formatAvroSchemaSummaryJson(AvroDdl.SchemaSummary summary) {
            final ObjectNode on = JsonUtils.createObjectNode();
            on.put("schemaId", summary.getId());
            on.put("name", summary.getName());
            on.put("status", summary.getMetadata().getStatus().name());
            on.put("modified", FormatUtils.formatDateAndTime(
                summary.getMetadata().getTimeModified()));
            on.put("from", summary.getMetadata().getFromMachine());
            if (!summary.getMetadata().getByUser().isEmpty()) {
                on.put("by", summary.getMetadata().getByUser());
            }
            final AvroDdl.SchemaSummary previous =
                summary.getPreviousVersion();
            if (previous != null) {
                on.put("previous", formatAvroSchemaSummaryJson(previous));
            }
            return on;
        }

        /**
         * Returns a valid schema ID for the given schemaName[.ID] string, or
         * throws ShellUsageException if there is no such schema.  The .ID
         * is required if the schema is disabled.
         *
         * @param idRequired is true if the .ID portion is reqiured.
         *
         * This is static so that it can be called from the DdlCommand as well.
         */
        protected static int parseSchemaNameAndId(String nameAndId,
                                                  boolean idRequired,
                                                  CommandServiceAPI cs,
                                                  ShellCommand command)
            throws ShellException, RemoteException {

            /* Get trailing .ID value, if any. */
            final int offset = nameAndId.lastIndexOf(".");
            int id = 0;
            if (offset > 0 && offset < nameAndId.length() - 1) {
                final String idArg = nameAndId.substring(offset + 1);
                try {
                    id = Integer.parseInt(idArg);
                    if (id <= 0) {
                        throw new ShellUsageException
                            ("Illegal schema ID: " + id, command);
                    }
                } catch (NumberFormatException e) /* CHECKSTYLE:OFF */ {
                    /* The ID remains zero. */
                } /* CHECKSTYLE:ON */
            }

            /* Get a map of all schemas. */
            final SortedMap<String, AvroDdl.SchemaSummary> map =
                cs.getSchemaSummaries(true /*includeDisabled*/);

            /*
             * If there is no trailing .ID, use the first active schema with
             * the given name.
             */
            if (id == 0) {
                if (idRequired) {
                    throw new ShellUsageException
                        (nameAndId + " does not containing a trailing .ID " +
                         "value", command);

                }

                final String name = nameAndId;
                AvroDdl.SchemaSummary summary = map.get(name);
                while (summary != null) {
                    if (summary.getMetadata().getStatus() !=
                        AvroSchemaStatus.DISABLED) {
                        return summary.getId();
                    }
                    summary = summary.getPreviousVersion();
                }
                throw new ShellUsageException
                    ("Schema " + name + " does not exist or is disabled." +
                     eol + "Use <schemaName>.<ID> to refer to a disabled " +
                     "schema.", command);
            }

            /*
             * Return the ID if there is such a schema with the given name and
             * ID.
             */
            final String name = nameAndId.substring(0, offset);
            AvroDdl.SchemaSummary summary = map.get(name);
            while (summary != null) {
                if (id == summary.getId()) {
                    return id;
                }
                summary = summary.getPreviousVersion();
            }
            throw new ShellUsageException
                ("Schema " + name + " does not exist or does not have ID " +
                 id, command);
        }

        private String showSingleSchema(String nameAndId,
                                        CommandServiceAPI cs,
                                        CommandShell cmd)
            throws ShellException {

            try {
                final int id;
                id = parseSchemaNameAndId(nameAndId, false /*idRequired*/,
                                          cs, this);
                final AvroDdl.SchemaDetails details = cs.getSchemaDetails(id);
                return details.getText();
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return cantGetHere;
        }
    }

    /*
     * ShowTopology
     */
    static final class ShowTopology extends SubCommand {
        static final String rnFlag = "-rn";
        static final String snFlag = "-sn";
        static final String stFlag = "-store";
        static final String shFlag = "-shard";
        static final String statusFlag = "-status";
        static final String perfFlag = "-perf";
        static final String anFlag = "-an";
        static final String dcFlagsDeprecation =
            "The -dc flag is deprecated and has been replaced by -zn." +
            eol + eol;

        private ShowTopology() {
            super("topology", 4);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new ShowTopologyExecutor<String>() {
                @Override
                public String
                    createTopologyResult(Topology t, PrintStream out,
                                         Parameters p,
                                         EnumSet<Filter> filter,
                                         Map<ResourceId, ServiceChange>
                                             statusMap,
                                         Map<ResourceId, PerfEvent> perfMap,
                                         boolean verbose,
                                         ByteArrayOutputStream outStream,
                                         String deprecatedDcFlagPrefix)
                    throws ShellException{
                    TopologyPrinter.printTopology(t, out, p, filter,
                                                  statusMap,
                                                  perfMap,
                                                  verbose);
                    return deprecatedDcFlagPrefix + outStream;
                }
            }.commonExecute(args, shell);
        }

        private abstract class ShowTopologyExecutor<T> implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                EnumSet<TopologyPrinter.Filter> filter =
                    EnumSet.noneOf(Filter.class);
                Shell.checkHelp(args, ShowTopology.this);
                boolean hasComponents = false;
                boolean deprecatedDcFlag = false;
                if (args.length > 1) {
                    for (int i = 1; i < args.length; i++) {
                        if (CommandUtils.isDatacenterIdFlag(args[i])) {
                            filter.add(Filter.DC);
                            hasComponents = true;
                            if ("-dc".equals(args[i])) {
                                deprecatedDcFlag = true;
                            }
                        } else if (args[i].equals(rnFlag)) {
                            filter.add(Filter.RN);
                            hasComponents = true;
                        } else if (args[i].equals(snFlag)) {
                            filter.add(Filter.SN);
                            hasComponents = true;
                        } else if (args[i].equals(stFlag)) {
                            filter.add(Filter.STORE);
                            hasComponents = true;
                        } else if (args[i].equals(shFlag)) {
                            filter.add(Filter.SHARD);
                            hasComponents = true;
                        } else if (args[i].equals(statusFlag)) {
                            filter.add(Filter.STATUS);
                        } else if (args[i].equals(perfFlag)) {
                            filter.add(Filter.PERF);
                        } else if (args[i].equals(anFlag)) {
                            filter.add(Filter.AN);
                            hasComponents = true;
                        } else {
                            shell.unknownArgument(args[i], ShowTopology.this);
                        }
                    }
                } else {
                    filter = TopologyPrinter.all;
                    hasComponents = true;
                }

                if (!hasComponents) {
                    filter.addAll(TopologyPrinter.components);
                }

                final String deprecatedDcFlagPrefix =
                    !deprecatedDcFlag ? "" : dcFlagsDeprecation;

                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();
                try {
                    final ByteArrayOutputStream outStream =
                        new ByteArrayOutputStream();
                    final PrintStream out = new PrintStream(outStream);

                    final Topology t = cs.getTopology();
                    final Parameters p = cs.getParameters();
                    Map<ResourceId, ServiceChange> statusMap = null;
                    if (filter.contains(Filter.STATUS)) {
                        statusMap = cs.getStatusMap();
                    }
                    Map<ResourceId, PerfEvent> perfMap = null;
                    if (filter.contains(Filter.PERF)) {
                        perfMap = cs.getPerfMap();
                    }
                    return createTopologyResult(t, out, p, filter, statusMap,
                                                perfMap, shell.getVerbose(),
                                                outStream,
                                                deprecatedDcFlagPrefix);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T
                createTopologyResult(Topology t, PrintStream out,
                                     Parameters params,
                                     EnumSet<Filter> filter,
                                     Map<ResourceId, ServiceChange> statusMap,
                                     Map<ResourceId, PerfEvent> perfMap,
                                     boolean verbose,
                                     ByteArrayOutputStream outStream,
                                     String deprecatedDcFlagPrefix)
                throws ShellException;
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show topology");
            return new ShowTopologyExecutor<ShellCommandResult>() {
                @Override
                public ShellCommandResult
                    createTopologyResult(Topology t, PrintStream out,
                                         Parameters p,
                                         EnumSet<Filter> filter,
                                         Map<ResourceId, ServiceChange>
                                             statusMap,
                                         Map<ResourceId, PerfEvent> perfMap,
                                         boolean verbose,
                                         ByteArrayOutputStream outStream,
                                         String deprecatedDcFlagPrefix)
                    throws ShellException{
                    scr.setReturnValue(
                        TopologyPrinter.printTopologyJson(
                            t, p, filter, verbose));
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return
                "show topology [-zn] [-rn] [-an] [-sn] [-store] [-status]" +
                " [-perf] " +
                CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Displays the current, deployed topology. " +
                "By default show the entire " + eolt +
                "topology. The optional flags restrict the " +
                "display to one or more of" + eolt + "Zones, " +
                "RepNodes, StorageNodes and Storename," + eolt + "or " +
                "specify service status or performance.";
        }
    }

    /*
     * ShowEvents
     */
    private static final class ShowEvents extends SubCommand {

        private ShowEvents() {
            super("events", 2);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new ShowEventExecutor<String>() {

                @Override
                public String singleEvent(String[] shellArgs,
                                          CommandServiceAPI cs,
                                          CommandShell cmd,
                                          Shell commandShell)
                    throws ShellException {
                    return showSingleEvent(shellArgs, cs, cmd, commandShell);
                }

                @Override
                public String multiEvents(List<CriticalEvent> events)
                    throws ShellException {
                    String msg = "";
                    for (CriticalEvent ev : events) {
                        msg += ev.toString() + eol;
                    }
                    return msg;
                }
                
            }.commonExecute(args, shell);
        }

        private abstract class ShowEventExecutor<T>
            implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, ShowEvents.this);
                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();
                if (Shell.checkArg(args, "-id")) {
                    return singleEvent(args, cs, cmd, shell);
                }
                try {
                    Date fromTime = null;
                    Date toTime = null;
                    CriticalEvent.EventType et = CriticalEvent.EventType.ALL;

                    for (int i = 1; i < args.length; i++) {
                        if ("-from".equals(args[i])) {
                            if (++i >= args.length) {
                                shell.badArgCount(ShowEvents.this);
                            }
                            fromTime =
                                parseTimestamp(args[i], ShowEvents.this);
                            if (fromTime == null) {
                                throw new ShellArgumentException(
                                    "Can't parse " + args[i] +
                                    " as a timestamp.");
                            }
                        } else if ("-to".equals(args[i])) {
                            if (++i >= args.length) {
                                shell.badArgCount(ShowEvents.this);
                            }
                            toTime = parseTimestamp(args[i], ShowEvents.this);
                        } else if ("-type".equals(args[i])) {
                            if (++i >= args.length) {
                                shell.badArgCount(ShowEvents.this);
                            }
                            try {
                                final String etype = args[i].toUpperCase();
                                et = Enum.valueOf(
                                         CriticalEvent.EventType.class,
                                         etype);
                            } catch (IllegalArgumentException iae) {
                                throw new ShellUsageException
                                    ("Can't parse " + args[i] +
                                     " as an EventType.", ShowEvents.this);
                            }
                        } else {
                            shell.unknownArgument(args[i], ShowEvents.this);
                        }
                    }

                    final long from =
                        (fromTime == null ? 0L : fromTime.getTime());
                    final long to = (toTime == null ? 0L : toTime.getTime());

                    final List<CriticalEvent> events =
                        cs.getEvents(from, to, et);
                    return multiEvents(events);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T
                singleEvent(String[] args, CommandServiceAPI cs,
                            CommandShell cmd, Shell shell)
                throws ShellException;

            public abstract T multiEvents(List<CriticalEvent> events)
                throws ShellException;
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args,
                                                    Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show events");
            final ObjectNode top = JsonUtils.createObjectNode();
            final ArrayNode eventArray = top.putArray("events");
            return new ShowEventExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult singleEvent(String[] shellArgs,
                                                      CommandServiceAPI cs,
                                                      CommandShell cmd,
                                                      Shell commandShell)
                    throws ShellException {

                    if (args.length != 3) {
                        shell.badArgCount(ShowEvents.this);
                    }

                    if (!"-id".equals(args[1])) {
                        shell.unknownArgument(args[1], ShowEvents.this);
                    }
                    final String eventId = args[2];
                    try {
                        final CriticalEvent event =
                            cs.getOneEvent(eventId);
                        if (event == null) {
                            throw new ShellArgumentException(
                                "No event matches the id " + eventId);
                        }
                        eventArray.add(event.getDetailString());
                        scr.setReturnValue(top);
                        return scr;
                    } catch (RemoteException re) {
                        cmd.noAdmin(re);
                    }
                    return null;
                }

                @Override
                public ShellCommandResult
                    multiEvents(List<CriticalEvent> events)
                    throws ShellException {
                    for (CriticalEvent ev : events) {
                        eventArray.add(ev.getDetailString());
                    }
                    scr.setReturnValue(top);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "show events [-id <id>] | [-from <date>] " +
                "[-to <date>]" + eolt + "[-type <stat|log|perf>] " +
                CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Displays event details or list of store events.  Status " +
                "events indicate" + eolt + "changes in service status.  " +
                "Log events correspond to records written " + eolt + "to " +
                "the store's log, except that only records logged at " +
                "\"SEVERE\" are " + eolt + "displayed; which should be " +
                "investigated immediately.  To view records " + eolt +
                "logged at \"WARNING\" or lower consult the store's log " +
                "file." + eolt + "Performance events are not usually " +
                "critical but may merit investigation." + eolt + eolt +
                getDateFormatsUsage();
        }

        private String showSingleEvent(String[] args,
                                       CommandServiceAPI cs,
                                       CommandShell cmd,
                                       Shell shell)
            throws ShellException {

            if (args.length != 3) {
                shell.badArgCount(this);
            }

            if (!"-id".equals(args[1])) {
                shell.unknownArgument(args[1], this);
            }
            final String eventId = args[2];
            try {
                final CriticalEvent event = cs.getOneEvent(eventId);
                if (event == null) {
                    return "No event matches the id " + eventId;
                }
                return event.getDetailString();
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return cantGetHere;
        }
    }

    /*
     * ShowPlans
     *
     * show plans with no arguments: list the ten most recent plans.
     *
     * -last: show details of the most recently created plan.
     *
     * -id <id>: show details of the plan with the given sequence number, or
     *   If -num <n> is also given, list <n> plans, starting with plan #<id>.
     *
     * -num <n>: set the number of plans to list. Defaults to 10.
     *   If unaccompanied: list the n most recently created plans.
     *
     * -from <date>: list plans starting with those created after <date>.
     *
     * -to <date>: list plans ending with those created before <date>.
     *   Combining -from with -to describes the range between the two <dates>.
     *   Otherwise -num <n> applies; its absence implies the default of 10.
     */
    private static final class ShowPlans extends SubCommandJsonConvert {

        private static final String lastPlanNotFound =
            "Found no plans created by the current user.";

        private String operation;

        private ShowPlans() {
            super("plans", 2);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            operation = SHOW_COMMAND_NAME + " " + getCommandName();
            if ((args.length == 3 && Shell.checkArg(args, "-id")) ||
                 (args.length == 2 && Shell.checkArg(args, "-last"))) {
                return showSinglePlan(shell, args, cs, cmd);
            }

            int planId = 0;
            int howMany = 0;
            Date fromTime = null, toTime = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-id".equals(arg)) {
                    planId = parseUnsignedInt(Shell.nextArg(args, i++, this));
                } else if ("-num".equals(arg)) {
                    howMany = parseUnsignedInt(Shell.nextArg(args, i++, this));
                } else if ("-from".equals(arg)) {
                    fromTime =
                        parseTimestamp(Shell.nextArg(args, i++, this), this);
                } else if ("-to".equals(arg)) {
                    toTime =
                        parseTimestamp(Shell.nextArg(args, i++, this), this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (planId != 0 && !(fromTime == null && toTime == null)) {
                throw new ShellUsageException
                    ("-id cannot be used in combination with -from or -to",
                     this);
            }

            /* If no other range selector is given, default to most recent. */
            if (planId == 0 && fromTime == null && toTime == null) {
                toTime = new Date();
            }

            /* If no other range limit is given, default to 10. */
            if ((fromTime == null || toTime == null) && howMany == 0) {
                howMany = 10;
            }

            /*
             * If a time-based range is requested, we need to get plan ID range
             * information first.
             */
            if (! (fromTime == null && toTime == null)) {
                try {

                    int range[] =
                        cs.getPlanIdRange
                        (fromTime == null ? 0L : fromTime.getTime(),
                         toTime == null ? 0L : toTime.getTime(),
                         howMany);

                    planId = range[0];
                    howMany = range[1];
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
            }

            String msg = "";
            ObjectNode returnNode = JsonUtils.createObjectNode();
            ArrayNode planNodes = returnNode.putArray("plans");
            while (howMany > 0) {
                try {
                    SortedMap<Integer, Plan> sortedPlans =
                        new TreeMap<Integer, Plan>
                        (cs.getPlanRange(planId, howMany));

                    /* If we got zero plans back, we're out of plans. */
                    if (sortedPlans.size() == 0) {
                        break;
                    }

                    for (Integer k : sortedPlans.keySet()) {
                        final Plan p = sortedPlans.get(k);
                        if (shell.getJson()) {
                            ObjectNode node = JsonUtils.createObjectNode();
                            node.put("id", p.getId());
                            node.put("name", p.getName());
                            node.put("state", p.getState().name());
                            planNodes.add(node);
                        } else {
                            msg += String.format("%6d %-24s %s" + eol,
                                p.getId(),
                                p.getName(),
                                p.getState().toString());
                        }

                        howMany--;
                        planId = k.intValue() + 1;
                    }
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
            }
            if (shell.getJson()) {
                CommandResult result = new CommandSucceeds(
                    returnNode.toString());
                return Shell.toJsonReport(operation, result);
            }
            return msg;
        }

        @Override
        protected String getCommandSyntax() {
            return "show plans [-last] [-id <id>] [-from <date>] " +
                   "[-to <date>] [-num <howMany>] " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Shows details of the specified plan or lists all plans " +
                "that have been" + eolt + "created along with their " +
                "corresponding plan IDs and status." + eolt +
                eolt +
                "With no argument: lists the ten most recent plans." + eolt +
                "-last: shows details of the most recent plan" + eolt +
                "-id <id>: shows details of the plan with the given id;" + eolt+
                "    if -num <n> is also given, list <n> plans," + eolt +
                "    starting with plan #<id>." + eolt +
                "-num <n>: sets the number of plans to list." + eolt +
                "    Defaults to 10." + eolt +
                "-from <date>: lists plans after <date>." + eolt +
                "-to <date>: lists plans before <date>." + eolt +
                "    Combining -from with -to describes the range" + eolt +
                "    between the two <dates>.  Otherwise -num applies." + eolt +
                "-json: return result in json format." + eolt +
                eolt +
                getDateFormatsUsage();
        }

        /*
         * Show details of a single plan.  TODO: add flags for varying details:
         * -tasks -finished, etc.
         */
        private String showSinglePlan(Shell shell, String[] args,
                                      CommandServiceAPI cs, CommandShell cmd)
            throws ShellException {

            int planId = 0;
            final boolean verbose = shell.getVerbose();

            try {
                if ("-last".equals(args[1])) {
                    planId = PlanCommand.PlanSubCommand.getLastPlanId(cs);
                    if (planId == 0) {
                        if (shell.getJson()) {
                            CommandResult result =
                                new CommandFails(lastPlanNotFound,
                                                 ErrorMessage.NOSQL_5200,
                                                 CommandResult.NO_CLEANUP_JOBS);
                            return Shell.toJsonReport(operation, result);
                        }
                        return lastPlanNotFound;
                    }
                } else if ("-id".equals(args[1])) {
                    if (args.length != 3) {
                        shell.badArgCount(this);
                    }
                    planId = parseUnsignedInt(args[2]);
                } else {
                    shell.unknownArgument(args[1], this);
                }

                long options = StatusReport.SHOW_FINISHED_BIT;
                if (verbose) {
                    options |= StatusReport.VERBOSE_BIT;
                }
                final String planStatus =
                    cs.getPlanStatus(
                        planId, options, shell.getJson());
                if (shell.getJson()) {
                    CommandResult result = new CommandSucceeds(planStatus);
                    return Shell.toJsonReport(operation, result);
                }
                return planStatus;
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return cantGetHere;
        }
    }

    /*
     * ShowPools
     */
    static final class ShowPools extends SubCommand {

        ShowPools() {
            super("pools", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
             throws ShellException {

            return new ShowPoolExecutor<String>() {

                @Override
                public String allPoolsResult(List<String> poolNames,
                                             CommandServiceAPI cs,
                                             Topology topo)
                    throws ShellException, RemoteException {
                    String res = "";
                    for (String pn : poolNames) {
                       res += pn + ": ";
                       for (StorageNodeId snid :
                           cs.getStorageNodePoolIds(pn)) {
                           DatacenterId dcid = topo.get(snid)
                               .getDatacenterId();
                           String dcName = topo.getDatacenterMap()
                               .get(dcid).getName();
                           res += snid.toString() + " zn:[id="
                               + dcid + " name="
                               + dcName + "], ";
                        }
                        res = res.substring(0, res.length() - 2);
                        res += eol;
                    }
                    return res;
                }

                @Override
                public String singlePoolResult(String poolName,
                                               CommandServiceAPI cs,
                                               Topology topo)
                    throws ShellException, RemoteException {
                    String res = poolName + ": ";
                    for (StorageNodeId snid :
                        cs.getStorageNodePoolIds(poolName)) {
                        DatacenterId dcid = topo.get(snid)
                            .getDatacenterId();
                        String dcName = topo.getDatacenterMap()
                            .get(dcid).getName();
                        res += snid.toString() + " zn:[id="
                            + dcid + " name="
                            + dcName + "], ";
                    }
                    res = res.substring(0, res.length() - 2);
                    res += eol;
                    return res;
                }
            }.commonExecute(args, shell);
        }

        private abstract class ShowPoolExecutor<T> implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, ShowPools.this);
                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();

                String poolName = null;
                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];
                    if ("-name".equals(arg)) {
                        poolName = Shell.nextArg(args, i++, ShowPools.this);
                    } else {
                        throw new ShellUsageException(
                            "Invalid argument: " + arg, ShowPools.this);
                    }
                }

                try{
                    Topology topo = cs.getTopology();
                    /* show pools */
                    if (args.length == 1) {
                        final List<String> poolNames =
                            cs.getStorageNodePoolNames();
                        return allPoolsResult(poolNames, cs, topo);
                    }
                    if (cs.getStorageNodePoolNames().contains(poolName)) {
                        return singlePoolResult(poolName, cs, topo);
                    }
                    throw new ShellArgumentException(
                        "Not a valid pool name: " + poolName);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T
                allPoolsResult(List<String> poolNames,
                               CommandServiceAPI cs,
                               Topology topo)
                throws ShellException, RemoteException;

            public abstract T
                singlePoolResult(String poolName,
                                 CommandServiceAPI cs,
                                 Topology topo)
                throws ShellException, RemoteException;
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show pool");
            final ObjectNode top = JsonUtils.createObjectNode();
            final ArrayNode poolArray = top.putArray("pools");
            return new ShowPoolExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    allPoolsResult(List<String> poolNames,
                                   CommandServiceAPI cs,
                                   Topology topo)
                    throws ShellException, RemoteException {

                    for (String pn : poolNames) {
                        final ObjectNode poolNode =
                            JsonUtils.createObjectNode();
                        poolNode.put("poolName", pn);
                        final ArrayNode snArray = poolNode.putArray("sns");
                        for (StorageNodeId snid :
                            cs.getStorageNodePoolIds(pn)) {
                            final ObjectNode snNode =
                                JsonUtils.createObjectNode();
                            final DatacenterId dcid =
                                topo.get(snid).getDatacenterId();
                            final String dcName =
                                topo.getDatacenterMap().get(dcid).getName();
                            snNode.put("resourceId", snid.toString());
                            snNode.put("znId", dcid.toString());
                            snNode.put("zoneName", dcName);
                            snArray.add(snNode);
                        }
                        poolArray.add(poolNode);
                    }
                    scr.setReturnValue(top);
                    return scr;
                }

                @Override
                public ShellCommandResult
                    singlePoolResult(String poolName,
                                     CommandServiceAPI cs,
                                     Topology topo)
                    throws ShellException, RemoteException {
                    final ObjectNode poolNode =
                        JsonUtils.createObjectNode();
                    poolNode.put("poolName", poolName);
                    final ArrayNode snArray = poolNode.putArray("sns");
                    for (StorageNodeId snid :
                        cs.getStorageNodePoolIds(poolName)) {
                        final ObjectNode snNode =
                            JsonUtils.createObjectNode();
                        final DatacenterId dcid =
                            topo.get(snid).getDatacenterId();
                        final String dcName =
                            topo.getDatacenterMap().get(dcid).getName();
                        snNode.put("snId", snid.toString());
                        snNode.put("zoneId", dcid.toString());
                        snNode.put("zoneName", dcName);
                        snArray.add(snNode);
                    }
                    poolArray.add(poolNode);
                    scr.setReturnValue(top);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "show pools [-name <name>] " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Lists the storage node pools";
        }
    }

    /*
     * TODO: Add filter flags
     */
    private static final class ShowPerf extends SubCommand {

        private ShowPerf() {
            super("perf", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new ShowPerfExecutor<String>() {
                @Override
                public String
                    multiPerfResult(Map<ResourceId, PerfEvent> map) {
                    final ByteArrayOutputStream outStream =
                        new ByteArrayOutputStream();
                    final PrintStream out = new PrintStream(outStream);
                    out.println(PerfEvent.HEADER);
                    for (PerfEvent pe : map.values()) {
                        out.println(pe.getColumnFormatted());
                    }
                    return outStream.toString();
                }
            }.commonExecute(args, shell);
        }

        private abstract class ShowPerfExecutor<T> implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();
                try {
                    final Map<ResourceId, PerfEvent> perfMap = cs.getPerfMap();
                    return multiPerfResult(perfMap);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }
            public abstract T multiPerfResult(
                Map<ResourceId, PerfEvent> map);
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show perf");
            final ObjectNode top = JsonUtils.createObjectNode();
            final ArrayNode perfArray = top.putArray("perfs");
            return new ShowPerfExecutor<ShellCommandResult>() {
                @Override
                public ShellCommandResult
                multiPerfResult(Map<ResourceId, PerfEvent> map) {
                    for (PerfEvent pe : map.values()) {
                        perfArray.add(pe.getColumnFormatted());
                    }
                    scr.setReturnValue(top);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "show perf " +
                    CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Displays recent performance information for each " +
                "Replication Node.";
        }
    }

    private static final class ShowSnapshots extends SubCommand {

        private ShowSnapshots() {
            super("snapshots", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new ShowSnapshotExecutor<String>() {

                @Override
                public String multiLineResult(String[] lines) {
                    String ret = "";
                    for (String ss : lines) {
                        ret += ss + eol;
                    }
                    return ret;
                }
            }.commonExecute(args, shell);
        }

        private abstract class ShowSnapshotExecutor<T> implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, ShowSnapshots.this);
                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();
                StorageNodeId snid = null;
                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];
                    if ("-sn".equals(arg)) {
                        final String sn =
                            Shell.nextArg(args, i++, ShowSnapshots.this);
                        try {
                            snid = StorageNodeId.parse(sn);
                        } catch (IllegalArgumentException iae) {
                            invalidArgument(sn);
                        }
                    } else {
                        shell.unknownArgument(arg, ShowSnapshots.this);
                    }
                }
                try {
                    final Snapshot snapshot =
                        new Snapshot(cs, shell.getVerbose(),
                                     shell.getOutput());
                    String [] list = null;
                    if (snid != null) {
                        list = snapshot.listSnapshots(snid);
                    } else {
                        list = snapshot.listSnapshots();
                    }
                    return multiLineResult(list);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage());
                }
                return null;
            }

            public abstract T multiLineResult(String[] lines);

        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show snapshot");
            final ObjectNode top = JsonUtils.createObjectNode();
            final ArrayNode snapshotArray = top.putArray("snapshots");
            return new ShowSnapshotExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    multiLineResult(String[] lines) {
                    for (String ss : lines) {
                        snapshotArray.add(ss);
                    }
                    scr.setReturnValue(top);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "show snapshots [-sn <id>] " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Lists snapshots on the specified Storage Node. If no " +
                "Storage Node" + eolt + "is specified one is chosen from " +
                "the store.";
        }
    }

    private static final class ShowUpgradeOrder extends SubCommand {

        private ShowUpgradeOrder() {
            super("upgrade-order", 3);
        }

        @Override
        protected String getCommandSyntax() {
            return "show upgrade-order " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Lists the Storage Nodes which need to be upgraded in an " +
                "order that" + eolt + "prevents disruption to the store's " +
                "operation.";
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            return new ShowUpgradeOrderExecutor<String>() {

                @Override
                public String retrieveUpgradeOrder(CommandServiceAPI cs,
                                                   KVVersion current,
                                                   KVVersion prerequisite)
                    throws RemoteException {
                    return cs.getUpgradeOrder(current, prerequisite);
                }

            }.commonExecute(args, shell);
        }

        private abstract class ShowUpgradeOrderExecutor<T>
            implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();

                try {
                    /*
                     * Thus command gets the order for upgrading to the
                     * version of the CLI.
                     */
                    return
                        retrieveUpgradeOrder(cs,
                                             KVVersion.CURRENT_VERSION,
                                             KVVersion.PREREQUISITE_VERSION);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T retrieveUpgradeOrder(CommandServiceAPI cs,
                                                   KVVersion current,
                                                   KVVersion prerequisite)
                throws RemoteException;
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show upgrade-order");
            return new ShowUpgradeOrderExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    retrieveUpgradeOrder(CommandServiceAPI cs,
                                         KVVersion current,
                                         KVVersion prerequisite)
                    throws RemoteException {
                    final ObjectNode top = JsonUtils.createObjectNode();
                    List<Set<StorageNodeId>> result =
                        cs.getUpgradeOrderList(current, prerequisite);
                    final ArrayNode orderArray =
                        top.putArray("upgradeOrders");
                    for (Set<StorageNodeId> set : result) {
                        final ObjectNode on = JsonUtils.createObjectNode();
                        final ArrayNode upgradeNodes =
                            on.putArray("upgradeNodes");
                        for (StorageNodeId id : set) {
                            upgradeNodes.add(id.toString());
                        }
                        orderArray.add(on);
                    }
                    scr.setReturnValue(top);
                    return scr;
                }

            }.commonExecute(args, shell);
        }

    }

    /*
     * ShowDatacenters
     */
    static final class ShowDatacenters extends ShowZones {

        static final String dcCommandDeprecation =
            "The command:" + eol + eolt +
            "show datacenters" + eol + eol +
            "is deprecated and has been replaced by:" + eol + eolt +
            "show zones" + eol + eol;

        ShowDatacenters() {
            super("datacenters", 4);
        }

        /** This is a deprecated command. Return true. */
        @Override
        protected boolean isDeprecated() {
            return true;
        }

        /** Add deprecation message. */
        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return dcCommandDeprecation + super.execute(args, shell);
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            return super.executeJsonOutput(args, shell);
        }

        /** Add deprecation message. */
        @Override
        public String getCommandDescription() {
            return super.getCommandDescription() + eol + eolt +
                "This command is deprecated and has been replaced by:"
                + eol + eolt +
                "show zones";
        }
    }

    static class ShowZones extends SubCommand {
        static final String ID_FLAG = "-zn";
        static final String NAME_FLAG = "-znname";
        static final String DESC_STR = "zone";
        static final String dcFlagsDeprecation =
            "The -dc and -dcname flags, and the dc<ID> ID format, are" +
            " deprecated" + eol +
            "and have been replaced by -zn, -znname, and zn<ID>." +
            eol + eol;

        private ShowZones() {
            super("zones", 4);
        }

        ShowZones(final String name, final int prefixLength) {
            super(name, prefixLength);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            return new ShowZoneExecutor<String>() {

                @Override
                public String zoneResult(DatacenterId id,
                                         String nameFlag,
                                         Topology topo,
                                         Parameters params,
                                         String deprecatedDcFlagPrefix) {
                    final ByteArrayOutputStream outStream =
                        new ByteArrayOutputStream();
                    final PrintStream out = new PrintStream(outStream);
                    TopologyPrinter.printZoneInfo(
                        id, nameFlag, topo, out, params);
                    return deprecatedDcFlagPrefix + outStream;
                }
            }.commonExecute(args, shell);
        }

        private abstract class ShowZoneExecutor<T> implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                DatacenterId id = null;
                String nameFlag = null;
                boolean deprecatedDcFlag = false;

                Shell.checkHelp(args, ShowZones.this);
                if (args.length > 1) {
                    for (int i = 1; i < args.length; i++) {
                        if (CommandUtils.isDatacenterIdFlag(args[i])) {
                            try {
                                id = DatacenterId.parse(
                                    Shell.nextArg(args, i++, ShowZones.this));
                            } catch (IllegalArgumentException e) {
                                throw new ShellUsageException(
                                    "Invalid zone ID: " + args[i],
                                    ShowZones.this);
                            }
                            if (CommandUtils.isDeprecatedDatacenterId(
                                    args[i-1], args[i])) {
                                deprecatedDcFlag = true;
                            }
                        } else if (CommandUtils.
                                   isDatacenterNameFlag(args[i])) {
                            nameFlag =
                                Shell.nextArg(args, i++, ShowZones.this);
                            if (CommandUtils.isDeprecatedDatacenterName(
                                    args[i-1])) {
                                deprecatedDcFlag = true;
                            }
                        } else {
                            shell.unknownArgument(args[i], ShowZones.this);
                        }
                    }
                }

                final String deprecatedDcFlagPrefix =
                    !deprecatedDcFlag ? "" : dcFlagsDeprecation;

                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();
                try {
                    final Topology topo = cs.getTopology();
                    final Parameters params = cs.getParameters();
                    return zoneResult(id, nameFlag, topo, params,
                                      deprecatedDcFlagPrefix);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T
                zoneResult(DatacenterId id,
                           String nameFlag,
                           Topology topo,
                           Parameters params,
                           String deprecatedDcFlagPrefix)
                throws ShellException;
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show zones");
            final ObjectNode top = JsonUtils.createObjectNode();
            final ArrayNode zoneArray = top.putArray("zns");
            return new ShowZoneExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    zoneResult(DatacenterId id, String nameFlag,
                               Topology topo, Parameters params,
                               String deprecatedDcFlagPrefix)
                    throws ShellException{
                    final boolean showAll =
                        ((id == null) && (nameFlag == null) ? true : false);
                    Datacenter showZone = null;

                    /*
                     * Display zones, sorted by ID
                     */
                    final List<Datacenter> dcList =
                        topo.getSortedDatacenters();
                    for (final Datacenter zone : dcList) {
                        if (showAll) {
                            zoneArray.add(zone.toJson());
                        } else {
                            if ((id != null) &&
                                id.equals(zone.getResourceId())) {
                                showZone = zone;
                                break;
                            } else if ((nameFlag != null) &&
                                nameFlag.equals(zone.getName())) {
                                showZone = zone;
                                break;
                            }
                        }
                    }
                    if (showAll) {
                        scr.setReturnValue(top);
                        return scr;
                    }

                    /*
                     * If showZone is null, then the id or name input is
                     * unknown
                     */
                    if (showZone == null) {
                        throw new ShellArgumentException(
                            DatacenterId.DATACENTER_PREFIX +
                            ": unknown id or name");
                    }

                    zoneArray.add(showZone.toJson());
                    scr.setReturnValue(top);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "show " + name +
                   " [" + ID_FLAG + " <id> | " + NAME_FLAG + " <name>] " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Lists the names of all " + DESC_STR + "s, or display " +
                "information about a" + eolt +
                "specific " + DESC_STR + ". If no " + DESC_STR + " is " +
                "specified, list the names" + eolt +
                "of all " + DESC_STR + "s. If a specific " + DESC_STR +
                " is specified using" + eolt +
                "either the " + DESC_STR + "'s id (via the '" + ID_FLAG +
                "' flag), or the " + DESC_STR + "'s" + eolt +
                "name (via the '" + NAME_FLAG + "' flag), then list " +
                "information such as the" + eolt + "names of the storage " +
                "nodes deployed to that " + DESC_STR + ".";
        }
    }

    /*
     * ShowTables
     */
    private static final class ShowTables extends SubCommand {
        final static String CMD_TEXT = "tables";
        final static String TABLE_FLAG = "-name";
        final static String PARENT_FLAG = "-parent";
        final static String LEVEL_FLAG = "-level";
        final static String NAMESPACE_FLAG = "-namespace";

        private ShowTables() {
            super(CMD_TEXT, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new ShowTableExecutor<String>() {

                @Override
                public String emptyTableResult() {
                    return "No table found.";
                }

                @Override
                public String singleTableResult(TableImpl table)
                    throws ShellException {
                    return table.toJsonString(true);
                }

                @Override
                public String allTablesResult(Map<String, Table> tableMap,
                                              Integer maxLevel,
                                              boolean verbose)
                    throws ShellException {
                    return getAllTablesInfo(tableMap, maxLevel, verbose);
                }
            }.commonExecute(args, shell);
        }

        private abstract class ShowTableExecutor<T>
            implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                String namespace = null;
                String tableName = null;
                String parentName = null;
                Integer maxLevel = null;
                Shell.checkHelp(args, ShowTables.this);
                if (args.length > 1) {
                    for (int i = 1; i < args.length; i++) {
                        if (TABLE_FLAG.equals(args[i])) {
                            tableName =
                                Shell.nextArg(args, i++, ShowTables.this);
                        } else if (PARENT_FLAG.equals(args[i])) {
                            parentName =
                                Shell.nextArg(args, i++, ShowTables.this);
                        } else if (NAMESPACE_FLAG.equals(args[i])) {
                            namespace =
                                Shell.nextArg(args, i++, ShowTables.this);
                        } else if (LEVEL_FLAG.equals(args[i])) {
                            String sLevel =
                                Shell.nextArg(args, i++, ShowTables.this);
                            maxLevel = parseUnsignedInt(sLevel);
                        } else {
                            shell.unknownArgument(args[i], ShowTables.this);
                        }
                    }
                }

                final CommandShell cmd = (CommandShell) shell;
                if (namespace == null) {
                    namespace = cmd.getNamespace();
                }
                final CommandServiceAPI cs = cmd.getAdmin();
                TableMetadata meta = null;
                try {
                    meta = cs.getMetadata(TableMetadata.class,
                                          MetadataType.TABLE);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                if (meta == null) {
                    return emptyTableResult();
                }

                /* Show the specified table's meta data. */
                if (tableName != null) {
                    TableImpl table = meta.getTable(namespace, tableName);
                    if (table == null) {
                        throw new ShellArgumentException("Table " +
                            TableMetadata.makeNamespaceName(
                                namespace, tableName) +
                            " does not exist.");
                    }
                    return singleTableResult(table);
                }

                /* Show multiple tables's meta data. */
                Map<String, Table> tableMap = null;
                boolean verbose = shell.getVerbose();
                if (parentName != null) {
                    TableImpl tbParent = meta.getTable(namespace, parentName);
                    if (tbParent == null) {
                        throw new ShellArgumentException("Table " +
                            TableMetadata.makeNamespaceName(
                                namespace, parentName) +
                            " does not exist.");
                    }
                    tableMap = tbParent.getChildTables();
                } else {
                    tableMap = meta.getTables(namespace);
                }
                if (tableMap == null || tableMap.size() == 0) {
                    return emptyTableResult();
                }
                return allTablesResult(tableMap, maxLevel, verbose);
            }

            public abstract T emptyTableResult();

            public abstract T singleTableResult(TableImpl table)
                throws ShellException;

            public abstract T
                allTablesResult(Map<String, Table> tableMap,
                                Integer maxLevel, boolean verbose)
                throws ShellException;

        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show tables");

            return new ShowTableExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult emptyTableResult() {
                    scr.setDescription("No table found.");
                    return scr;
                }

                @Override
                public ShellCommandResult
                    singleTableResult(TableImpl table)
                    throws ShellException {
                    final ObjectNode on =
                        CommandJsonUtils.createObjectNode();
                    TableJsonUtils.toJsonString(table, on, false);
                    scr.setReturnValue(on);
                    return scr;
                }

                @Override
                public ShellCommandResult
                    allTablesResult(Map<String, Table> tableMap,
                                          Integer maxLevel,
                                          boolean verbose)
                    throws ShellException {
                    scr.setReturnValue(
                        getAllTablesInfoJson(tableMap, maxLevel, verbose));
                    return scr;
                }
                
            }.commonExecute(args, shell);
        }

        private String getAllTablesInfo(Map<String, Table> tableMap,
                                        Integer maxLevel, boolean verbose) {
            if (!verbose) {
                return "Tables: " + eolt +
                    getTableAndChildrenName(tableMap, 0, maxLevel);
            }
            return getTableAndChildrenMetaInfo(tableMap, 0, maxLevel);
        }

        private ObjectNode getAllTablesInfoJson(Map<String, Table> tableMap,
                                                Integer maxLevel,
                                                boolean verbose) {
            if (!verbose) {
                return getTableAndChildrenJson(tableMap, 0, maxLevel);
            }
            return getTableAndChildrenMetaInfoJson(tableMap, 0, maxLevel);
        }

        private String getTableAndChildrenName(Map<String, Table> tableMap,
                                               int curLevel,
                                               Integer maxLevel) {
            final String INDENT = "  ";
            String indent = "";
            StringBuilder sb = new StringBuilder();
            if (curLevel > 0) {
                for (int i = 0; i < curLevel; i++) {
                    indent += INDENT;
                }
            }
            for (Map.Entry<String, Table> entry: tableMap.entrySet()) {
                TableImpl table = (TableImpl)entry.getValue();
                sb.append(indent);
                sb.append(table.getNamespaceName());
                String desc = table.getDescription();
                if (desc != null && desc.length() > 0) {
                    sb.append(" -- ");
                    sb.append(desc);
                }
                sb.append(Shell.eolt);
                if (maxLevel != null && curLevel == maxLevel) {
                    continue;
                }
                Map<String, Table> childTabs = table.getChildTables();
                if (childTabs != null) {
                    sb.append(getTableAndChildrenName(
                                  childTabs, curLevel + 1, maxLevel));
                }
            }
            return sb.toString();
        }

        private ObjectNode getTableAndChildrenJson(Map<String, Table> tableMap,
                                                   int curLevel,
                                                   Integer maxLevel) {
            final ObjectNode top = JsonUtils.createObjectNode();
            final ArrayNode tableArray = top.putArray("tables");
            for (Map.Entry<String, Table> entry: tableMap.entrySet()) {
                final ObjectNode tableNode = JsonUtils.createObjectNode();
                final TableImpl table = (TableImpl) entry.getValue();
                tableNode.put("name", table.getNamespaceName());
                tableNode.put("description", table.getDescription());
                if (maxLevel != null && curLevel == maxLevel) {
                    tableArray.add(tableNode);
                    continue;
                }
                Map<String, Table> childTabs = table.getChildTables();
                if (childTabs != null) {
                    tableNode.put("childTables",
                                  getTableAndChildrenJson(childTabs,
                                      curLevel + 1, maxLevel));
                }
                tableArray.add(tableNode);
            }
            return top;
        }

        private String getTableAndChildrenMetaInfo(Map<String, Table> tableMap,
                                                   int curLevel,
                                                   Integer maxLevel) {
            StringBuffer sb = new StringBuffer();
            for (Map.Entry<String, Table> entry: tableMap.entrySet()) {
                TableImpl table = (TableImpl)entry.getValue();
                sb.append(table.getFullName());
                sb.append(":");
                sb.append(Shell.eol);
                sb.append(table.toJsonString(true));
                sb.append(Shell.eol);
                if (maxLevel != null && curLevel == maxLevel) {
                    continue;
                }
                if (table.getChildTables() != null) {
                    sb.append(
                        getTableAndChildrenMetaInfo(
                            table.getChildTables(), curLevel++, maxLevel));
                }
            }
            return sb.toString();
        }

        private ObjectNode getTableAndChildrenMetaInfoJson(
            Map<String, Table> tableMap, int curLevel,
            Integer maxLevel) {
            final ObjectNode top = JsonUtils.createObjectNode();
            final ArrayNode tableArray = top.putArray("tables");
            for (Map.Entry<String, Table> entry: tableMap.entrySet()) {
                TableImpl table = (TableImpl) entry.getValue();
                final ObjectNode tableNode = JsonUtils.createObjectNode();
                tableNode.put("name", table.getFullName());
                TableJsonUtils.toJsonString(table, tableNode, false);
                if (maxLevel != null && curLevel == maxLevel) {
                    tableArray.add(tableNode);
                    continue;
                }
                if (table.getChildTables() != null) {
                    tableNode.put("childTables",
                                  getTableAndChildrenMetaInfoJson(
                                      table.getChildTables(),
                                      curLevel++, maxLevel));
                }
                tableArray.add(tableNode);
            }
            return top;
        }

        @Override
        protected String getCommandSyntax() {
            return "show " + CMD_TEXT + " [" + TABLE_FLAG + " <name>] " +
                   "[" + PARENT_FLAG + " <name>] [" + LEVEL_FLAG +
                   " <level>] " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Display table metadata.  By default the names of all " +
                "top-tables and" + eolt + "their child tables are listed.  " +
                "Top-level tables are those without" + eolt + "parents.  " +
                "The level of child tables can be limited by specifying the" +
                eolt + LEVEL_FLAG + " flag.  If a specific table is named " +
                "its detailed metadata is" + eolt + "displayed.  The table " +
                "name is a dot-separated name with the format" + eolt +
                "tableName[.childTableName]*.  Flag " + PARENT_FLAG +
                " is used to show all child" + eolt + "tables for the " +
                "given parent table.";
        }
    }

    /*
     * ShowIndexes
     */
    private static final class ShowIndexes extends SubCommand {
        final static String CMD_TEXT = "indexes";
        final static String INDEX_FLAG = "-name";
        final static String TABLE_FLAG = "-table";
        final static String NAMESPACE_FLAG = "-namespace";

        private ShowIndexes() {
            super(CMD_TEXT, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new ShowIndexExecutor<String>() {

                @Override
                public String singleIndexResult(Index index,
                                                String indexName,
                                                String tableName) {
                    return getIndexInfo(index);
                }

                @Override
                public String
                    allTableAllIndexResult(Map<String, Table> tableMap) {
                    return getAllTablesIndexesInfo(tableMap);
                }

                @Override
                public String tableIndexResult(TableImpl table)
                    throws ShellException{
                    String ret = getTableIndexesInfo(table);
                    if (ret == null) {
                        return "No Index found.";
                    }
                    return ret;
                }
            }.commonExecute(args, shell);
        }

        private abstract class ShowIndexExecutor<T> implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                String namespace = null;
                String tableName = null;
                String indexName = null;
                Shell.checkHelp(args, ShowIndexes.this);
                if (args.length > 1) {
                    for (int i = 1; i < args.length; i++) {
                        if (TABLE_FLAG.equals(args[i])) {
                            tableName =
                                Shell.nextArg(args, i++, ShowIndexes.this);
                        } else if (NAMESPACE_FLAG.equals(args[i])) {
                            namespace =
                                Shell.nextArg(args, i++, ShowIndexes.this);
                        } else if (INDEX_FLAG.equals(args[i])) {
                            indexName =
                                Shell.nextArg(args, i++, ShowIndexes.this);
                        } else if (TABLE_FLAG.equals(args[i])) {
                            tableName =
                                Shell.nextArg(args, i++, ShowIndexes.this);
                        } else {
                            shell.unknownArgument(args[i], ShowIndexes.this);
                        }
                    }
                }

                if (indexName != null && tableName == null) {
                    shell.requiredArg(TABLE_FLAG, ShowIndexes.this);
                }

                final CommandShell cmd = (CommandShell) shell;
                if (namespace == null) {
                    namespace = cmd.getNamespace();
                }
                final CommandServiceAPI cs = cmd.getAdmin();
                TableMetadata meta = null;
                try {
                    meta = cs.getMetadata(TableMetadata.class,
                                          MetadataType.TABLE);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                if (meta == null) {
                    throw new ShellArgumentException("No table found.");
                }

                if (tableName != null) {
                    TableImpl table = meta.getTable(namespace, tableName);
                    if (table == null) {
                        throw new ShellArgumentException("Table " +
                            TableMetadata.makeNamespaceName(namespace,
                                                            tableName) +
                            " does not exist.");
                    }
                    if (indexName != null) {
                        Index index = table.getIndex(indexName);
                        if (index == null) {
                            throw new ShellArgumentException(
                                "Index " + indexName + " on table " +
                                tableName + " does not exist.");
                        }
                        return singleIndexResult(index,
                                                 indexName, tableName);
                    }
                    return tableIndexResult(table);
                }

                Map<String, Table> tableMap = null;
                tableMap = meta.getTables();
                if (tableMap == null || tableMap.isEmpty()) {
                    throw new ShellArgumentException("No table found.");
                }
                return allTableAllIndexResult(tableMap);
            }

            public abstract T singleIndexResult(Index index,
                                                String indexName,
                                                String tableName);

            public abstract T
                allTableAllIndexResult(Map<String, Table> tableMap);

            public abstract T
                tableIndexResult(TableImpl table) throws ShellException;
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show index");
            return new ShowIndexExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    singleIndexResult(Index index,
                                      String indexName,
                                      String tableName) {
                    scr.setReturnValue(getIndexInfoJson(index));
                    return scr;
                }

                @Override
                public ShellCommandResult
                    allTableAllIndexResult(Map<String, Table> tableMap) {
                    scr.setReturnValue(getAllTablesIndexesInfoJson(tableMap));
                    return scr;
                }

                @Override
                public ShellCommandResult
                    tableIndexResult(TableImpl table)  throws ShellException {
                    final ObjectNode on =
                        getTableIndexesInfoJson(table);
                    if (on == null) {
                        scr.setDescription("No Index found.");
                        return scr;
                    }
                    scr.setReturnValue(on);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        private String getAllTablesIndexesInfo(Map<String, Table> tableMap) {
            StringBuilder sb = new StringBuilder();
            for (Entry<String, Table> entry: tableMap.entrySet()) {
                Table table = entry.getValue();
                String ret = getTableIndexesInfo(table);
                if (ret == null) {
                    continue;
                }
                if (sb.length() > 0) {
                    sb.append(eol);
                }
                sb.append(ret);
                sb.append(eol);
                if (table.getChildTables() != null) {
                    ret = getAllTablesIndexesInfo(table.getChildTables());
                    if (ret.length() > 0) {
                        sb.append(eol);
                        sb.append(ret);
                    }
                }
            }
            return sb.toString();
        }

        private ObjectNode
            getAllTablesIndexesInfoJson(Map<String, Table> tableMap) {
            final ObjectNode top = JsonUtils.createObjectNode();
            final ArrayNode tableArray = top.putArray("tables");
            for (Entry<String, Table> entry: tableMap.entrySet()) {
                Table table = entry.getValue();
                final ObjectNode on = JsonUtils.createObjectNode();
                final ObjectNode ret = getTableIndexesInfoJson(table);
                if (ret == null) {
                    continue;
                }
                on.put("table", ret);
                final ArrayNode childArray = on.putArray("childTable");
                if (table.getChildTables() != null) {
                    childArray.add(
                        getAllTablesIndexesInfoJson(table.getChildTables()));
                }
                tableArray.add(on);
            }
            return top;
        }

        private String getTableIndexesInfo(Table table) {
            Map<String, Index> map = table.getIndexes();
            if (map == null || map.isEmpty()) {
                return null;
            }
            StringBuilder sb = new StringBuilder();
            sb.append("Indexes on table ");
            sb.append(table.getFullName());
            for (Entry<String, Index> entry: map.entrySet()) {
                sb.append(eol);
                sb.append(getIndexInfo(entry.getValue()));
            }
            return sb.toString();
        }

        private ObjectNode getTableIndexesInfoJson(Table table) {
            Map<String, Index> map = table.getIndexes();
            if (map == null || map.isEmpty()) {
                return null;
            }
            final ObjectNode on = JsonUtils.createObjectNode();
            on.put("tableName", table.getFullName());
            final ArrayNode indexArray = on.putArray("indexes");
            for (Entry<String, Index> entry: map.entrySet()) {
                indexArray.add(getIndexInfoJson(entry.getValue()));
            }
            return on;
        }

        private String getIndexInfo(Index index) {
            StringBuilder sb = new StringBuilder();
            sb.append(Shell.tab);
            sb.append(index.getName());
            sb.append(" (");
            boolean first = true;
            for (String s : index.getFields()) {
                if (!first) {
                    sb.append(", ");
                } else {
                    first = false;
                }
                sb.append(s);
            }
            sb.append(")");

            if (index.getType().equals(Index.IndexType.TEXT)) {
                sb.append(", type: " + Index.IndexType.TEXT);
            } else if (index.getType().equals(Index.IndexType.SECONDARY)) {
                sb.append(", type: " + Index.IndexType.SECONDARY);
            }

            if (index.getDescription() != null) {
                sb.append(" -- ");
                sb.append(index.getDescription());
            }
            return sb.toString();
        }

        private ObjectNode getIndexInfoJson(Index index) {
            final ObjectNode on = JsonUtils.createObjectNode();
            on.put("name", index.getName());
            final ArrayNode fieldArray = on.putArray("fields");
            for (String s : index.getFields()) {
                fieldArray.add(s);
            }

            on.put("type", index.getType().name());
            on.put("description", index.getDescription());
            return on;
        }

        @Override
        protected String getCommandSyntax() {
            return "show " + CMD_TEXT + " [" + TABLE_FLAG + " <name>] " +
                    "[" + INDEX_FLAG + " <name>] " +
                    CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Display index metadata. By default the indexes metadata " +
                "of all tables" + eolt +
                "are listed.  If a specific table is named its indexes " +
                "metadata are" + eolt +
                "displayed, if a specific index of the table is named its " +
                "metadata is " + eolt +
                "displayed.";
        }
    }

    /*
     * ShowUsers
     *
     * Print the user information stored in the security metadata copy. If
     * no user name is specified, a brief information of all users will be
     * printed. Otherwise, only the specified user will be printed.
     *
     * While showing the information of all users, the format will be:
     * <br>"user: id=xxx name=xxx"<br>
     * For showing the details of a specified user, it will be:
     * <br>"user: id=xxx name=xxx state=xxx type=xxx retained-passwd=xxxx"
     */
    private static final class ShowUsers extends SubCommand {

        static final String NAME_FLAG = "-name";
        static final String DESC_STR = "user";

        private ShowUsers() {
            super("users", 4);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            return new ShowUserExecutor<String>() {

                @Override
                public String noUserResult(String message) {
                    return message;
                }

                @Override
                public String
                    multiUserResult(
                        Collection<UserDescription> usersDesc) {
                    final ByteArrayOutputStream outStream =
                        new ByteArrayOutputStream();
                    final PrintStream out = new PrintStream(outStream);
                    for (final UserDescription desc : usersDesc) {
                        out.println("user: " + desc.brief());
                    }
                    return outStream.toString();
                }

            }.commonExecute(args, shell);
        }

        private abstract class ShowUserExecutor<T> implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                String nameFlag = null;

                Shell.checkHelp(args, ShowUsers.this);
                if (args.length > 1) {
                    for (int i = 1; i < args.length; i++) {
                        if ("-name".equals(args[i])) {
                            nameFlag =
                                Shell.nextArg(args, i++, ShowUsers.this);
                            if (nameFlag == null || nameFlag.isEmpty()) {
                                throw new ShellUsageException(
                                    "User name could not be empty.",
                                    ShowUsers.this);
                            }
                        } else {
                            shell.unknownArgument(args[i], ShowUsers.this);
                        }
                    }
                }

                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();
                try {
                    final Map<String, UserDescription> userDescMap =
                            cs.getUsersDescription();

                    if (userDescMap == null || userDescMap.isEmpty()) {
                        return noUserResult("No users.");
                    }
                    if (nameFlag != null) { /* Print details for a user */
                        final UserDescription desc = userDescMap.get(nameFlag);
                        final String message = desc == null ?
                            "User with name of " + nameFlag + " not found." :
                            "user: " + desc.details();
                        throw new ShellArgumentException(message);
                    }

                    /* Print summary for all users */
                    final Collection<UserDescription> usersDesc =
                           userDescMap.values();
                    return multiUserResult(usersDesc);

                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T noUserResult(String text);

            public abstract T
                multiUserResult(Collection<UserDescription> usersDesc);
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show users");
            final ObjectNode top = JsonUtils.createObjectNode();
            final ArrayNode userArray = top.putArray("users");
            return new ShowUserExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    noUserResult(String text) {
                    scr.setDescription(text);
                    return scr;
                }

                @Override
                public ShellCommandResult
                    multiUserResult(
                        Collection<UserDescription> usersDesc) {
                    final ObjectNode on = JsonUtils.createObjectNode();
                    for (final UserDescription desc : usersDesc) {
                        on.put("user", desc.brief());
                        userArray.add(on);
                    }
                    scr.setReturnValue(top);
                    return scr;
                }
                
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "show " + "users" + " [-name <name>] " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Lists the names of all " + DESC_STR + "s, or displays " +
                "information about a" + eolt +
                "specific " + DESC_STR + ". If no " + DESC_STR + " is " +
                "specified, lists the names" + eolt +
                "of all " + DESC_STR + "s. If a " + DESC_STR +
                " is specified using the " + NAME_FLAG + " flag," + eolt +
                "then lists detailed information about the " + DESC_STR + ".";
        }
    }

    /*
     * ShowVersions
     *
     * Print client and server version information.
     */
    private static final class ShowVersions extends SubCommand {

        private ShowVersions() {
            super("versions", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            return new ShowVersionExecutor<String>() {

                @Override
                public String versionResult(StorageNodeStatus status,
                                            String exceptionMessage,
                                            String adminHostName) {
                    final StringBuilder sb = new StringBuilder();
                    sb.append("Client version: ");
                    sb.append(
                        KVVersion.CURRENT_VERSION.getNumericVersionString());
                    sb.append(eol);
                    if (status == null) {
                        sb.append("Cannot reach server at host ");
                        sb.append(adminHostName);
                        sb.append(": " + eol);
                        sb.append(exceptionMessage);
                    } else {
                        sb.append("Server version: ");
                        sb.append(status.getKVVersion());
                    }
                    return sb.toString();
                }

            }.commonExecute(args, shell);
        }

        private abstract class ShowVersionExecutor<T> implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                CommandShell cmd = (CommandShell) shell;
                StorageNodeStatus status = null;
                String exceptionMessage = "";
                try{
                    CommandServiceAPI cs = cmd.getAdmin();
                    LoadParameters adminConfig = cs.getParams();
                    int snId = adminConfig.getMap(ParameterState.SNA_TYPE).
                        getOrZeroInt(ParameterState.COMMON_SN_ID);
                    String storeName =
                        adminConfig.getMap(ParameterState.GLOBAL_TYPE).
                        get(ParameterState.COMMON_STORENAME).asString();
                    /* ping service doesn't need to login,
                     * so set login manager as null here */
                    StorageNodeAgentAPI sna =
                        RegistryUtils.getStorageNodeAgent(
                        storeName, cmd.getAdminHostname(), cmd.getAdminPort(),
                        new StorageNodeId(snId), null/*LoginManager*/);
                    status = sna.ping();
                } catch (RemoteException re) {
                    exceptionMessage = re.toString();
                } catch (NotBoundException e) {
                    exceptionMessage = e.toString();
                }

                return versionResult(
                    status, exceptionMessage, cmd.getAdminHostname());
            }

            public abstract T versionResult(StorageNodeStatus status,
                                            String exceptionMessage,
                                            String adminHostName);

        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {

            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show versions");

            return new ShowVersionExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    versionResult(StorageNodeStatus status,
                                  String exceptionMessage,
                                  String adminHostName) {
                    final ObjectNode on = JsonUtils.createObjectNode();
                    on.put(
                        "clientVersion",
                        KVVersion.CURRENT_VERSION.getNumericVersionString());
                    if (status != null) {
                        final KVVersion version = status.getKVVersion();
                        on.put("serverVersion",
                               version.getNumericVersionString());
                        if (version.getReleaseEdition() != null) {
                            on.put("serverEdition",
                                   version.getReleaseEdition());
                        }
                    }
                    scr.setReturnValue(on);
                    return scr;
                }

            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "show versions " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Display client and connected server version information. "
                + eolt + "If you want to get all servers version, please use "
                + "ping instead.";
        }
    }

    /**
     * When specifying event timestamps, these formats are accepted.
     */
    private static String[] dateFormats = {
        "yyyy-MM-dd HH:mm:ss.SSS",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd HH:mm",
        "yyyy-MM-dd",
        "MM-dd-yyyy HH:mm:ss.SSS",
        "MM-dd-yyyy HH:mm:ss",
        "MM-dd-yyyy HH:mm",
        "MM-dd-yyyy",
        "HH:mm:ss.SSS",
        "HH:mm:ss",
        "HH:mm"
    };

    private static String getDateFormatsUsage() {
        String usage =
            "<date> can be given in the following formats," + eolt +
            "which are interpreted in the UTC time zone." + eolt;

        for (String fs : dateFormats) {
            usage += eolt + "    " + fs;
        }

        return usage;
    }

    /**
     * Apply the above formats in sequence until one of them matches.
     */
    private static Date parseTimestamp(String s, ShellCommand command)
        throws ShellUsageException {

        TimeZone tz = TimeZone.getTimeZone("UTC");

        Date r = null;
        for (String fs : dateFormats) {
            final DateFormat f = new SimpleDateFormat(fs);
            f.setTimeZone(tz);
            f.setLenient(false);
            try {
                r = f.parse(s);
                break;
            } catch (ParseException pe) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }

        if (r == null) {
            throw new ShellUsageException
                ("Invalid date format: " + s, command);
        }

        /*
         * If the date parsed is in the distant past (i.e., in January 1970)
         * then the string lacked a year/month/day.  We'll be friendly and
         * interpret the time as being in the recent past, that is, today.
         */

        final Calendar rcal = Calendar.getInstance(tz);
        rcal.setTime(r);

        if (rcal.get(Calendar.YEAR) == 1970) {
            final Calendar nowCal = Calendar.getInstance();
            nowCal.setTime(new Date());

            rcal.set(nowCal.get(Calendar.YEAR),
                     nowCal.get(Calendar.MONTH),
                     nowCal.get(Calendar.DAY_OF_MONTH));

            /* If the resulting time is in the future, subtract one day. */

            if (rcal.after(nowCal)) {
                rcal.add(Calendar.DAY_OF_MONTH, -1);
            }
            r = rcal.getTime();
        }
        return r;
    }
}
