/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.compilereport;

import java.io.IOException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.GroupRef;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.IndexType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.PlatformProperties;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class ReportMaker {

    static Date m_timestamp = new Date();

    /**
     * Make an html bootstrap tag with our custom css class.
     */
    static void tag(StringBuilder sb, String color, String text) {
        sb.append("<span class='label label");
        if (color != null) {
            sb.append("-").append(color);
        }
        sb.append(" l-").append(text).append("'>").append(text).append("</span>");
    }

    static String genrateIndexRow(Table table, Index index) {
        StringBuilder sb = new StringBuilder();
        sb.append("        <tr class='primaryrow2'>");

        // name column
        String anchor = (table.getTypeName() + "-" + index.getTypeName()).toLowerCase();
        sb.append("<td style='white-space: nowrap'><i id='s-" + anchor + "--icon' class='icon-chevron-right'></i> <a href='#' id='s-");
        sb.append(anchor).append("' class='togglex'>");
        sb.append(index.getTypeName());
        sb.append("</a></td>");

        // type column
        sb.append("<td>");
        sb.append(IndexType.get(index.getType()).toString());
        sb.append("</td>");

        // columns column
        sb.append("<td>");
        List<ColumnRef> cols = CatalogUtil.getSortedCatalogItems(index.getColumns(), "index");
        List<String> columnNames = new ArrayList<String>();
        for (ColumnRef colRef : cols) {
            columnNames.add(colRef.getColumn().getTypeName());
        }
        sb.append(StringUtils.join(columnNames, ", "));
        sb.append("</td>");

        // uniqueness column
        sb.append("<td>");
        if (index.getUnique()) {
            tag(sb, "important", "Unique");
        }
        else {
            tag(sb, "info", "Multikey");
        }
        sb.append("</td>");

        sb.append("</tr>\n");

        // BUILD THE DROPDOWN FOR THE PLAN/DETAIL TABLE
        sb.append("<tr class='dropdown2'><td colspan='5' id='s-"+ table.getTypeName().toLowerCase() +
                "-" + index.getTypeName().toLowerCase() + "--dropdown'>\n");

        IndexAnnotation annotation = (IndexAnnotation) index.getAnnotation();
        if (annotation != null) {
            if (annotation.proceduresThatUseThis.size() > 0) {
                sb.append("<p>Used by procedures: ");
                List<String> procs = new ArrayList<String>();
                for (Procedure proc : annotation.proceduresThatUseThis) {
                    procs.add("<a href='#p-" + proc.getTypeName() + "'>" + proc.getTypeName() + "</a>");
                }
                sb.append(StringUtils.join(procs, ", "));
                sb.append("</p>");
            }
        }

        sb.append("</td></tr>\n");

        return sb.toString();
    }

    static String generateIndexesTable(Table table) {
        StringBuilder sb = new StringBuilder();
        sb.append("    <table class='table tableL2 table-condensed'>\n    <thead><tr>" +
                  "<th>Index Name</th>" +
                  "<th>Type</th>" +
                  "<th>Columns</th>" +
                  "<th>Uniqueness</th>" +
                  "</tr></thead>\n    <tbody>\n");

        for (Index index : table.getIndexes()) {
            sb.append(genrateIndexRow(table, index));
        }

        sb.append("    </tbody>\n    </table>\n");
        return sb.toString();
    }

    static String generateSchemaRow(Table table) {
        StringBuilder sb = new StringBuilder();
        sb.append("<tr class='primaryrow'>");

        // column 1: table name
        String anchor = table.getTypeName().toLowerCase();
        sb.append("<td style='white-space: nowrap;'><i id='s-" + anchor + "--icon' class='icon-chevron-right'></i> <a href='#' id='s-");
        sb.append(anchor).append("' class='togglex'>");
        sb.append(table.getTypeName());
        sb.append("</a></td>");

        // column 2: type
        sb.append("<td>");
        if (table.getMaterializer() != null) {
            tag(sb, "info", "MaterialziedView");
        }
        else {
            tag(sb, null, "Table");
        }
        sb.append("</td>");

        // column 3: partitioning
        sb.append("<td>");
        if (table.getIsreplicated()) {
            tag(sb, "warning", "Replicated");
        }
        else {
            tag(sb, "success", "Partitioned");
        }
        sb.append("</td>");

        // column 4: column count
        sb.append("<td>");
        sb.append(table.getColumns().size());
        sb.append("</td>");

        // column 5: index count
        sb.append("<td>");
        sb.append(table.getIndexes().size());
        sb.append("</td>");

        // column 6: has pkey
        sb.append("<td>");
        boolean found = false;
        for (Constraint constraint : table.getConstraints()) {
            if (ConstraintType.get(constraint.getType()) == ConstraintType.PRIMARY_KEY) {
                found = true;
                break;
            }
        }
        if (found) {
            tag(sb, "info", "Has-Pkey");
        }
        else {
            tag(sb, null, "No-Pkey");
        }
        sb.append("</td>");

        sb.append("</tr>\n");

        // BUILD THE DROPDOWN FOR THE DDL / INDEXES DETAIL

        sb.append("<tr class='tablesorter-childRow'><td class='invert' colspan='6' id='s-"+ table.getTypeName().toLowerCase() + "--dropdown'>\n");

        TableAnnotation annotation = (TableAnnotation) table.getAnnotation();
        if (annotation != null) {
            // output the DDL
            if (annotation.ddl == null) {
                sb.append("<p>MISSING DDL</p>\n");
            }
            else {
                String ddl = annotation.ddl;
                sb.append("<p><pre>" + ddl + "</pre></p>\n");
            }

            // make sure procs appear in only one category
            annotation.proceduresThatReadThis.removeAll(annotation.proceduresThatUpdateThis);

            if (annotation.proceduresThatReadThis.size() > 0) {
                sb.append("<p>Read-only by procedures: ");
                List<String> procs = new ArrayList<String>();
                for (Procedure proc : annotation.proceduresThatReadThis) {
                    procs.add("<a href='#p-" + proc.getTypeName() + "'>" + proc.getTypeName() + "</a>");
                }
                sb.append(StringUtils.join(procs, ", "));
                sb.append("</p>");
            }
            if (annotation.proceduresThatUpdateThis.size() > 0) {
                sb.append("<p>Read/Write by procedures: ");
                List<String> procs = new ArrayList<String>();
                for (Procedure proc : annotation.proceduresThatUpdateThis) {
                    procs.add("<a href='#p-" + proc.getTypeName() + "'>" + proc.getTypeName() + "</a>");
                }
                sb.append(StringUtils.join(procs, ", "));
                sb.append("</p>");
            }
        }

        if (table.getIndexes().size() > 0) {
            sb.append(generateIndexesTable(table));
        }
        else {
            sb.append("<p>No indexes defined on table.</p>\n");
        }

        sb.append("</td></tr>\n");

        return sb.toString();
    }

    static String generateSchemaTable(CatalogMap<Table> tables) {
        StringBuilder sb = new StringBuilder();
        for (Table table : tables) {
            sb.append(generateSchemaRow(table));
        }
        return sb.toString();
    }

    static String genrateStatementRow(Procedure procedure, Statement statement) {
        StringBuilder sb = new StringBuilder();
        sb.append("        <tr class='primaryrow2'>");

        // name column
        String anchor = (procedure.getTypeName() + "-" + statement.getTypeName()).toLowerCase();
        sb.append("<td style='white-space: nowrap'><i id='p-" + anchor + "--icon' class='icon-chevron-right'></i> <a href='#' id='p-");
        sb.append(anchor).append("' class='togglex'>");
        sb.append(statement.getTypeName());
        sb.append("</a></td>");

        // sql column
        sb.append("<td><tt>");
        sb.append(statement.getSqltext());
        sb.append("</td></tt>");

        // params column
        sb.append("<td>");
        List<StmtParameter> params = CatalogUtil.getSortedCatalogItems(statement.getParameters(), "index");
        List<String> paramTypes = new ArrayList<String>();
        for (StmtParameter param : params) {
            paramTypes.add(VoltType.get((byte) param.getJavatype()).name());
        }
        if (paramTypes.size() == 0) {
            sb.append("<i>None</i>");
        }
        sb.append(StringUtils.join(paramTypes, ", "));
        sb.append("</td>");

        // r/w column
        sb.append("<td>");
        if (statement.getReadonly()) {
            tag(sb, "success", "Read");
        }
        else {
            tag(sb, "warning", "Write");
        }
        sb.append("</td>");

        // attributes
        sb.append("<td>");

        if (!statement.getIscontentdeterministic() || !statement.getIsorderdeterministic()) {
            tag(sb, "inverse", "Determinism");
        }

        if (statement.getSeqscancount() > 0) {
            tag(sb, "important", "Scans");
        }

        sb.append("</td>");

        sb.append("</tr>\n");

        // BUILD THE DROPDOWN FOR THE PLAN/DETAIL TABLE
        sb.append("<tr class='dropdown2'><td colspan='5' id='p-"+ procedure.getTypeName().toLowerCase() +
                "-" + statement.getTypeName().toLowerCase() + "--dropdown'>\n");

        sb.append("<div class='well well-small'><h4>Explain Plan:</h4>\n");
        StatementAnnotation annotation = (StatementAnnotation) statement.getAnnotation();
        if (annotation != null) {
            String plan = annotation.explainPlan;
            plan = plan.replace("\n", "<br/>");
            plan = plan.replace(" ", "&nbsp;");

            for (Table t : annotation.tablesRead) {
                String name = t.getTypeName().toUpperCase();
                String link = "\"<a href='#s-" + t.getTypeName() + "'>" + name + "</a>\"";
                plan = plan.replace("\"" + name + "\"", link);
            }
            for (Table t : annotation.tablesUpdated) {
                String name = t.getTypeName().toUpperCase();
                String link = "\"<a href='#s-" + t.getTypeName() + "'>" + name + "</a>\"";
                plan = plan.replace("\"" + name + "\"", link);
            }
            for (Index i : annotation.indexesUsed) {
                Table t = (Table) i.getParent();
                String name = i.getTypeName().toUpperCase();
                String link = "\"<a href='#s-" + t.getTypeName() + "-" + i.getTypeName() +"'>" + name + "</a>\"";
                plan = plan.replace("\"" + name + "\"", link);
            }

            sb.append("<tt>").append(plan).append("</tt>");
        }
        else {
            sb.append("<i>No SQL explain plan found.</i>\n");
        }
        sb.append("</div>\n");

        sb.append("</td></tr>\n");

        return sb.toString();
    }

    static String generateStatementsTable(Procedure procedure) {
        StringBuilder sb = new StringBuilder();
        sb.append("    <table class='table tableL2 table-condensed'>\n    <thead><tr>" +
                  "<th><span style='white-space: nowrap;'>Statement Name</span></th>" +
                  "<th>Statement SQL</th>" +
                  "<th>Params</th>" +
                  "<th>R/W</th>" +
                  "<th>Attributes</th>" +
                  "</tr></thead>\n    <tbody>\n");

        for (Statement statement : procedure.getStatements()) {
            sb.append(genrateStatementRow(procedure, statement));
        }

        sb.append("    </tbody>\n    </table>\n");
        return sb.toString();
    }

    static String generateProcedureRow(Procedure procedure) {
        StringBuilder sb = new StringBuilder();
        sb.append("<tr class='primaryrow'>");

        // column 1: procedure name
        String anchor = procedure.getTypeName().toLowerCase();
        sb.append("<td style='white-space: nowrap'><i id='p-" + anchor + "--icon' class='icon-chevron-right'></i> <a href='#p-");
        sb.append(anchor).append("' id='p-").append(anchor).append("' class='togglex'>");
        sb.append(procedure.getTypeName());
        sb.append("</a></td>");

        // column 2: parameter types
        sb.append("<td>");
        List<ProcParameter> params = CatalogUtil.getSortedCatalogItems(procedure.getParameters(), "index");
        List<String> paramTypes = new ArrayList<String>();
        for (ProcParameter param : params) {
            paramTypes.add(VoltType.get((byte) param.getType()).name());
        }
        if (paramTypes.size() == 0) {
            sb.append("<i>None</i>");
        }
        sb.append(StringUtils.join(paramTypes, ", "));
        sb.append("</td>");

        // column 3: partitioning
        sb.append("<td>");
        if (procedure.getSinglepartition()) {
            tag(sb, "success", "Single");
        }
        else {
            tag(sb, "warning", "Multi");
        }
        sb.append("</td>");

        // column 4: read/write
        sb.append("<td>");
        if (procedure.getReadonly()) {
            tag(sb, "success", "Read");
        }
        else {
            tag(sb, "warning", "Write");
        }
        sb.append("</td>");

        // column 5: access
        sb.append("<td>");
        List<String> groupNames = new ArrayList<String>();
        for (GroupRef groupRef : procedure.getAuthgroups()) {
            groupNames.add(groupRef.getGroup().getTypeName());
        }
        if (groupNames.size() == 0) {
            sb.append("<i>None</i>");
        }
        sb.append(StringUtils.join(groupNames, ", "));
        sb.append("</td>");

        // column 6: attributes
        sb.append("<td>");
        if (procedure.getHasjava()) {
            tag(sb, "info", "Java");
        }
        else {
            tag(sb, null, "Single-Stmt");
        }
        boolean isND = false;
        int scanCount = 0;
        for (Statement stmt : procedure.getStatements()) {
            scanCount += stmt.getSeqscancount();
            if (!stmt.getIscontentdeterministic() || !stmt.getIsorderdeterministic()) {
                isND = false;
            }
        }
        if (isND) {
            tag(sb, "inverse", "Determinism");
        }
        if (scanCount > 0) {
            tag(sb, "important", "Scans");
        }
        sb.append("</td>");

        sb.append("</tr>\n");

        // BUILD THE DROPDOWN FOR THE STATEMENT/DETAIL TABLE

        sb.append("<tr class='tablesorter-childRow'><td class='invert' colspan='6' id='p-"+ procedure.getTypeName().toLowerCase() + "--dropdown'>\n");

        // output partitioning parameter info
        if (procedure.getSinglepartition()) {
            String pTable = procedure.getPartitioncolumn().getParent().getTypeName();
            String pColumn = procedure.getPartitioncolumn().getTypeName();
            int pIndex = procedure.getPartitionparameter();

            sb.append(String.format("<p>Partitioned on parameter %d which maps to column %s" +
                                    " of table <a class='invert' href='#s-%s'>%s</a>.</p>",
                                    pIndex, pColumn, pTable, pTable));
        }

        // output what schema this interacts with
        ProcedureAnnotation annotation = (ProcedureAnnotation) procedure.getAnnotation();
        if (annotation != null) {
            // make sure tables appear in only one category
            annotation.tablesRead.removeAll(annotation.tablesUpdated);

            if (annotation.tablesRead.size() > 0) {
                sb.append("<p>Read-only access to tables: ");
                List<String> tables = new ArrayList<String>();
                for (Table table : annotation.tablesRead) {
                    tables.add("<a href='#s-" + table.getTypeName() + "'>" + table.getTypeName() + "</a>");
                }
                sb.append(StringUtils.join(tables, ", "));
                sb.append("</p>");
            }
            if (annotation.tablesUpdated.size() > 0) {
                sb.append("<p>Read/Write access to tables: ");
                List<String> tables = new ArrayList<String>();
                for (Table table : annotation.tablesUpdated) {
                    tables.add("<a href='#s-" + table.getTypeName() + "'>" + table.getTypeName() + "</a>");
                }
                sb.append(StringUtils.join(tables, ", "));
                sb.append("</p>");
            }
            if (annotation.indexesUsed.size() > 0) {
                sb.append("<p>Uses indexes: ");
                List<String> indexes = new ArrayList<String>();
                for (Index index : annotation.indexesUsed) {
                    Table table = (Table) index.getParent();
                    indexes.add("<a href='#s-" + table.getTypeName() + "-" + index.getTypeName() + "'>" + index.getTypeName() + "</a>");
                }
                sb.append(StringUtils.join(indexes, ", "));
                sb.append("</p>");
            }
        }

        sb.append(generateStatementsTable(procedure));

        sb.append("</td></tr>\n");

        return sb.toString();
    }

    static String generateProceduresTable(CatalogMap<Procedure> procedures) {
        StringBuilder sb = new StringBuilder();
        for (Procedure procedure : procedures) {
            if (procedure.getDefaultproc()) {
                continue;
            }
            sb.append(generateProcedureRow(procedure));
        }
        return sb.toString();
    }

    /**
     * Get some embeddable HTML of some generic catalog/application stats
     * that is drawn on the first page of the report.
     */
    static String getStatsHTML(Database db) {
        StringBuilder sb = new StringBuilder();
        sb.append("<table class='table table-condensed'>\n");

        // count things
        int indexes = 0, views = 0, statements = 0;
        int partitionedTables = 0, replicatedTables = 0;
        int partitionedProcs = 0, replicatedProcs = 0;
        int readProcs = 0, writeProcs = 0;
        for (Table t : db.getTables()) {
            if (t.getMaterializer() != null) {
                views++;
            }
            else {
                if (t.getIsreplicated()) replicatedTables++;
                else partitionedTables++;
            }
            indexes += t.getIndexes().size();
        }
        for (Procedure p : db.getProcedures()) {
            // skip auto-generated crud procs
            if (p.getDefaultproc()) continue;
            if (p.getSinglepartition()) partitionedProcs++;
            else replicatedProcs++;
            if (p.getReadonly()) readProcs++;
            else writeProcs++;
            statements += p.getStatements().size();
        }

        // version
        sb.append("<tr><td>Compiled by VoltDB Version</td><td>");
        sb.append(VoltDB.instance().getVersionString()).append("</td></tr>\n");

        // timestamp
        sb.append("<tr><td>Compile Timestamp</td><td>");
        sb.append(SimpleDateFormat.getInstance().format(m_timestamp)).append("</td></tr>\n");

        // tables
        sb.append("<tr><td>Table Count</td><td>");
        sb.append(String.format("%d (%d partitioned / %d replicated)",
                partitionedTables + replicatedTables, partitionedTables, replicatedTables));
        sb.append("</td></tr>\n");

        // views
        sb.append("<tr><td>Materialized View Count</td><td>").append(views).append("</td></tr>\n");

        // indexes
        sb.append("<tr><td>Index Count</td><td>").append(indexes).append("</td></tr>\n");

        // procedures
        sb.append("<tr><td>Procedure Count</td><td>");
        sb.append(String.format("%d (%d partitioned / %d replicated) (%d read-only / %d read-write)",
                partitionedProcs + replicatedProcs, partitionedProcs, replicatedProcs,
                readProcs, writeProcs));
        sb.append("</td></tr>\n");

        // statements
        sb.append("<tr><td>SQL Statement Count</td><td>").append(statements).append("</td></tr>\n");

        sb.append("</table>\n");
        return sb.toString();
    }

    /**
     * Generate the HTML catalog report from a newly compiled VoltDB catalog
     */
    public static String report(Catalog catalog) throws IOException {
        // asyncronously get platform properties
        new Thread() {
            @Override
            public void run() {
                PlatformProperties.getPlatformProperties();
            }
        }.start();


        URL url = Resources.getResource(ReportMaker.class, "template.html");
        String contents = Resources.toString(url, Charsets.UTF_8);

        Cluster cluster = catalog.getClusters().get("cluster");
        assert(cluster != null);
        Database db = cluster.getDatabases().get("database");
        assert(db != null);

        String statsData = getStatsHTML(db);
        contents = contents.replace("##STATS##", statsData);

        String schemaData = generateSchemaTable(db.getTables());
        contents = contents.replace("##SCHEMA##", schemaData);

        String procData = generateProceduresTable(db.getProcedures());
        contents = contents.replace("##PROCS##", procData);

        String platformData = PlatformProperties.getPlatformProperties().toHTML();
        contents = contents.replace("##PLATFORM##", platformData);


        contents = contents.replace("##VERSION##", VoltDB.instance().getVersionString());

        DateFormat df = SimpleDateFormat.getInstance();
        contents = contents.replace("##TIMESTAMP##", df.format(m_timestamp));

        String msg = Encoder.hexEncode(VoltDB.instance().getVersionString() + "," + System.currentTimeMillis());
        contents = contents.replace("get.py?a=KEY&", String.format("get.py?a=%s&", msg));

        return contents;
    }

    /**
     * Find the pre-compild catalog report in the jarfile, and modify it for use in the
     * the built-in web portal.
     */
    public static String liveReport() {
        byte[] reportbytes = VoltDB.instance().getCatalogContext().getFileInJar("catalog-report.html");
        String report = new String(reportbytes, Charsets.UTF_8);

        // remove commented out code
        report = report.replace("<!--##RESOURCES", "");
        report = report.replace("##RESOURCES-->", "");

        // inject the running system platform properties
        PlatformProperties pp = PlatformProperties.getPlatformProperties();
        String ppStr = "<h4>Cluster Platform</h4>\n<p>" + pp.toHTML() + "</p><br/>\n";
        report = report.replace("<!--##PLATFORM2##-->", ppStr);

        // change the live/static var to live
        if (VoltDB.instance().getConfig().m_isEnterprise) {
            report = report.replace("&b=r&", "&b=e&");
        }
        else {
            report = report.replace("&b=r&", "&b=c&");
        }

        return report;
    }

    /**
     * Some test code that needs to be moved.
     */
    public static void main(String args[]) throws IOException {
        CatalogBuilder builder = new CatalogBuilder();
        builder.addSchema("/Users/jhugg/Documents/workspace/voltdb2/examples/voter/ddl.sql");
        builder.compile("dummy.jar");

        /*TPCCProjectBuilder pb = new TPCCProjectBuilder();
        pb.addAllDefaults();
        pb.compile("dummy.jar");*/
    }
}
