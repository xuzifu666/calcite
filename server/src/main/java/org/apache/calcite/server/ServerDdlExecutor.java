/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.server;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.ContextSqlValidator;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.materialize.MaterializationKey;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.model.JsonSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.ddl.SqlAttributeDefinition;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateForeignSchema;
import org.apache.calcite.sql.ddl.SqlCreateFunction;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.apache.calcite.sql.ddl.SqlCreateSchema;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlCreateTableLike;
import org.apache.calcite.sql.ddl.SqlCreateType;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.ddl.SqlDropObject;
import org.apache.calcite.sql.ddl.SqlDropSchema;
import org.apache.calcite.sql.ddl.SqlTruncateTable;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

/** Executes DDL commands.
 *
 * <p>Given a DDL command that is a sub-class of {@link SqlNode}, dispatches
 * the command to an appropriate {@code execute} method. For example,
 * "CREATE TABLE" ({@link SqlCreateTable}) is dispatched to
 * {@link #execute(SqlCreateTable, CalcitePrepare.Context)}. */
public class ServerDdlExecutor extends DdlExecutorImpl {
  /** Singleton instance. */
  public static final ServerDdlExecutor INSTANCE = new ServerDdlExecutor();

  /** Parser factory. */
  @SuppressWarnings("unused") // used via reflection
  public static final SqlParserImplFactory PARSER_FACTORY =
      new SqlParserImplFactory() {
        @Override public SqlAbstractParserImpl getParser(Reader stream) {
          return SqlDdlParserImpl.FACTORY.getParser(stream);
        }

        @Override public DdlExecutor getDdlExecutor() {
          return ServerDdlExecutor.INSTANCE;
        }
      };

  /** Creates a ServerDdlExecutor.
   * Protected only to allow sub-classing;
   * use {@link #INSTANCE} where possible. */
  protected ServerDdlExecutor() {
  }

  /** Returns the schema in which to create an object;
   * the left part is null if the schema does not exist. */
  static Pair<@Nullable CalciteSchema, String> schema(
      CalcitePrepare.Context context, boolean mutable, SqlIdentifier id) {
    final String name;
    final List<String> path;
    if (id.isSimple()) {
      path = context.getDefaultSchemaPath();
      name = id.getSimple();
    } else {
      path = Util.skipLast(id.names);
      name = Util.last(id.names);
    }
    CalciteSchema schema =
        mutable ? context.getMutableRootSchema()
            : context.getRootSchema();
    for (String p : path) {
      @Nullable CalciteSchema subSchema = schema.getSubSchema(p, true);
      if (subSchema == null) {
        return Pair.of(null, name);
      }
      schema = subSchema;
    }
    return Pair.of(schema, name);
  }

  /**
   * Returns the SqlValidator with the given {@code context} schema
   * and type factory.
   */
  static SqlValidator validator(CalcitePrepare.Context context,
      boolean mutable) {
    return new ContextSqlValidator(context, mutable);
  }

  /** Wraps a query to rename its columns. Used by CREATE VIEW and CREATE
   * MATERIALIZED VIEW. */
  static SqlNode renameColumns(@Nullable SqlNodeList columnList,
      SqlNode query) {
    if (columnList == null) {
      return query;
    }
    final SqlParserPos p = query.getParserPosition();
    final SqlNodeList selectList = SqlNodeList.SINGLETON_STAR;
    final SqlCall from =
        SqlStdOperatorTable.AS.createCall(p,
            ImmutableList.<SqlNode>builder()
                .add(query)
                .add(new SqlIdentifier("_", p))
                .addAll(columnList)
                .build());
    return new SqlSelect(p, null, selectList, from, null, null, null, null,
        null, null, null, null, null);
  }

  /** Erase the table date that calcite-sever created. */
  static void erase(SqlIdentifier name, CalcitePrepare.Context context) {
    // Directly clearing data is more efficient than executing SQL
    final Pair<@Nullable CalciteSchema, String> pair =
        schema(context, true, name);
    final CalciteSchema calciteSchema = requireNonNull(pair.left);
    final String tblName = pair.right;
    final CalciteSchema.TableEntry tableEntry =
        calciteSchema.getTable(tblName, context.config().caseSensitive());
    final Table table = requireNonNull(tableEntry, "tableEntry").getTable();
    if (table instanceof MutableArrayTable) {
      MutableArrayTable mutableArrayTable = (MutableArrayTable) table;
      mutableArrayTable.rows.clear();
    } else {
      // Not calcite-server created, so not support truncate.
      throw new UnsupportedOperationException("Only MutableArrayTable support truncate");
    }
  }

  /** Populates the table called {@code name} by executing {@code query}. */
  static void populate(SqlIdentifier name, SqlNode query,
      CalcitePrepare.Context context) {
    // Generate, prepare and execute an "INSERT INTO table query" statement.
    // (It's a bit inefficient that we convert from SqlNode to SQL and back
    // again.)
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(context.getRootSchema().plus())
        .build();
    final Planner planner = Frameworks.getPlanner(config);
    try {
      final StringBuilder buf = new StringBuilder();
      final SqlWriterConfig writerConfig =
          SqlPrettyWriter.config().withAlwaysUseParentheses(false);
      final SqlPrettyWriter w = new SqlPrettyWriter(writerConfig, buf);
      buf.append("INSERT INTO ");
      name.unparse(w, 0, 0);
      buf.append(' ');
      query.unparse(w, 0, 0);
      final String sql = buf.toString();
      final SqlNode query1 = planner.parse(sql);
      final SqlNode query2 = planner.validate(query1);
      final RelRoot r = planner.rel(query2);
      final PreparedStatement prepare =
          context.getRelRunner().prepareStatement(r.rel);
      int rowCount = prepare.executeUpdate();
      Util.discard(rowCount);
      prepare.close();
    } catch (SqlParseException | ValidationException
        | RelConversionException | SQLException e) {
      throw Util.throwAsRuntime(e);
    }
  }

  /** Returns the value of a literal, converting
   * {@link NlsString} into String. */
  @SuppressWarnings("rawtypes")
  static @Nullable Comparable value(SqlNode node) {
    final Comparable v = SqlLiteral.value(node);
    return v instanceof NlsString ? ((NlsString) v).getValue() : v;
  }

  /** Executes a {@code CREATE FOREIGN SCHEMA} command. */
  public void execute(SqlCreateForeignSchema create,
      CalcitePrepare.Context context) {
    final Pair<@Nullable CalciteSchema, String> pair =
        schema(context, true, create.name);
    requireNonNull(pair.left); // TODO: should not assume parent schema exists
    if (pair.left.plus().subSchemas().get(pair.right) != null) {
      if (!create.getReplace() && !create.ifNotExists) {
        throw SqlUtil.newContextException(create.name.getParserPosition(),
            RESOURCE.schemaExists(pair.right));
      }
    }
    final Schema subSchema;
    final String libraryName;
    if (create.type != null) {
      checkArgument(create.library == null);
      final String typeName = (String) requireNonNull(value(create.type));
      final JsonSchema.Type type =
          Util.enumVal(JsonSchema.Type.class,
              typeName.toUpperCase(Locale.ROOT));
      if (type != null) {
        switch (type) {
        case JDBC:
          libraryName = JdbcSchema.Factory.class.getName();
          break;
        default:
          libraryName = null;
        }
      } else {
        libraryName = null;
      }
      if (libraryName == null) {
        throw SqlUtil.newContextException(create.type.getParserPosition(),
            RESOURCE.schemaInvalidType(typeName,
                Arrays.toString(JsonSchema.Type.values())));
      }
    } else {
      libraryName =
          requireNonNull((String) value(requireNonNull(create.library)));
    }
    final SchemaFactory schemaFactory =
        AvaticaUtils.instantiatePlugin(SchemaFactory.class, libraryName);
    final Map<String, Object> operandMap = new LinkedHashMap<>();
    for (Pair<SqlIdentifier, SqlNode> option : create.options()) {
      operandMap.put(option.left.getSimple(),
          requireNonNull(value(option.right)));
    }
    subSchema =
        schemaFactory.create(pair.left.plus(), pair.right, operandMap);
    pair.left.add(pair.right, subSchema);
  }

  /** Executes a {@code CREATE FUNCTION} command. */
  public void execute(SqlCreateFunction create,
      CalcitePrepare.Context context) {
    throw new UnsupportedOperationException("CREATE FUNCTION is not supported");
  }

  /** Executes {@code DROP FUNCTION}, {@code DROP TABLE},
   * {@code DROP MATERIALIZED VIEW}, {@code DROP TYPE},
   * {@code DROP VIEW} commands. */
  public void execute(SqlDropObject drop,
      CalcitePrepare.Context context) {
    final Pair<@Nullable CalciteSchema, String> pair =
        schema(context, false, drop.name);
    final @Nullable CalciteSchema schema =
        pair.left; // null if schema does not exist
    final String objectName = pair.right;

    boolean existed;
    switch (drop.getKind()) {
    case DROP_TABLE:
    case DROP_MATERIALIZED_VIEW:
      Table materializedView =
          schema != null
              && drop.getKind() == SqlKind.DROP_MATERIALIZED_VIEW
              ? schema.plus().tables().get(objectName)
              : null;

      existed = schema != null && schema.removeTable(objectName);
      if (existed) {
        if (materializedView instanceof Wrapper) {
          ((Wrapper) materializedView).maybeUnwrap(MaterializationKey.class)
              .ifPresent(materializationKey ->
                  MaterializationService.instance()
                      .removeMaterialization(materializationKey));
        }
      } else if (!drop.ifExists) {
        throw SqlUtil.newContextException(drop.name.getParserPosition(),
            RESOURCE.tableNotFound(objectName));
      }
      break;
    case DROP_VIEW:
      // Not quite right: removes any other functions with the same name
      existed = schema != null && schema.removeFunction(objectName);
      if (!existed && !drop.ifExists) {
        throw SqlUtil.newContextException(drop.name.getParserPosition(),
            RESOURCE.viewNotFound(objectName));
      }
      break;
    case DROP_TYPE:
      existed = schema != null && schema.removeType(objectName);
      if (!existed && !drop.ifExists) {
        throw SqlUtil.newContextException(drop.name.getParserPosition(),
            RESOURCE.typeNotFound(objectName));
      }
      break;
    case DROP_FUNCTION:
      existed = schema != null && schema.removeFunction(objectName);
      if (!existed && !drop.ifExists) {
        throw SqlUtil.newContextException(drop.name.getParserPosition(),
            RESOURCE.functionNotFound(objectName));
      }
      break;
    case OTHER_DDL:
    default:
      throw new AssertionError(drop.getKind());
    }
  }

  /**
   * Executes a {@code TRUNCATE TABLE} command.
   */
  public void execute(SqlTruncateTable truncate,
      CalcitePrepare.Context context) {
    final Pair<@Nullable CalciteSchema, String> pair =
        schema(context, true, truncate.name);
    if (pair.left == null
        || pair.left.plus().tables().get(pair.right) == null) {
      throw SqlUtil.newContextException(truncate.name.getParserPosition(),
          RESOURCE.tableNotFound(pair.right));
    }

    if (!truncate.continueIdentify) {
      // Calcite not support RESTART IDENTIFY
      throw new UnsupportedOperationException("RESTART IDENTIFY is not supported");
    }

    erase(truncate.name, context);
  }

  /** Executes a {@code CREATE MATERIALIZED VIEW} command. */
  public void execute(SqlCreateMaterializedView create,
      CalcitePrepare.Context context) {
    final Pair<@Nullable CalciteSchema, String> pair =
        schema(context, true, create.name);
    if (pair.left != null
        && pair.left.plus().tables().get(pair.right) != null) {
      // Materialized view exists.
      if (!create.ifNotExists) {
        // They did not specify IF NOT EXISTS, so give error.
        throw SqlUtil.newContextException(create.name.getParserPosition(),
            RESOURCE.tableExists(pair.right));
      }
      return;
    }
    final SqlNode q = renameColumns(create.columnList, create.query);
    final String sql = q.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
    requireNonNull(pair.left); // TODO: should not assume parent schema exists
    final List<String> schemaPath = pair.left.path(null);
    final ViewTableMacro viewTableMacro =
        ViewTable.viewMacro(pair.left.plus(), sql, schemaPath,
            context.getObjectPath(), false);
    final TranslatableTable x = viewTableMacro.apply(ImmutableList.of());
    final RelDataType rowType = x.getRowType(context.getTypeFactory());

    // Table does not exist. Create it.
    final MaterializedViewTable table =
        new MaterializedViewTable(pair.right, RelDataTypeImpl.proto(rowType));
    pair.left.add(pair.right, table);
    populate(create.name, create.query, context);
    table.key =
        MaterializationService.instance().defineMaterialization(pair.left, null,
            sql, schemaPath, pair.right, true, true);
  }

  /** Executes a {@code CREATE SCHEMA} command. */
  public void execute(SqlCreateSchema create,
      CalcitePrepare.Context context) {
    final Pair<@Nullable CalciteSchema, String> pair =
        schema(context, true, create.name);
    requireNonNull(pair.left); // TODO: should not assume parent schema exists
    if (pair.left.plus().subSchemas().get(pair.right) != null) {
      if (create.ifNotExists) {
        return;
      }
      if (!create.getReplace()) {
        throw SqlUtil.newContextException(create.name.getParserPosition(),
            RESOURCE.schemaExists(pair.right));
      }
    }
    final Schema subSchema = new AbstractSchema();
    pair.left.add(pair.right, subSchema);
  }

  /** Executes a {@code DROP SCHEMA} command. */
  public void execute(SqlDropSchema drop,
      CalcitePrepare.Context context) {
    final Pair<@Nullable CalciteSchema, String> pair =
        schema(context, false, drop.name);
    final String name = pair.right;
    final boolean existed = pair.left != null
        && pair.left.removeSubSchema(name);
    if (!existed && !drop.ifExists) {
      throw SqlUtil.newContextException(drop.name.getParserPosition(),
          RESOURCE.schemaNotFound(name));
    }
  }

  /** Executes a {@code CREATE TABLE} command. */
  public void execute(SqlCreateTable create,
      CalcitePrepare.Context context) {
    final Pair<@Nullable CalciteSchema, String> pair =
        schema(context, true, create.name);
    requireNonNull(pair.left); // TODO: should not assume parent schema exists
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    final RelDataType queryRowType;
    if (create.query != null) {
      // A bit of a hack: pretend it's a view, to get its row type
      final String sql =
          create.query.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
      final ViewTableMacro viewTableMacro =
          ViewTable.viewMacro(pair.left.plus(), sql, pair.left.path(null),
              context.getObjectPath(), false);
      final TranslatableTable x = viewTableMacro.apply(ImmutableList.of());
      queryRowType = x.getRowType(typeFactory);

      if (create.columnList != null
          && queryRowType.getFieldCount() != create.columnList.size()) {
        throw SqlUtil.newContextException(
            create.columnList.getParserPosition(),
            RESOURCE.columnCountMismatch());
      }
    } else {
      queryRowType = null;
    }
    final List<SqlNode> columnList;
    if (create.columnList != null) {
      columnList = create.columnList;
    } else {
      if (queryRowType == null) {
        // "CREATE TABLE t" is invalid; because there is no "AS query" we need
        // a list of column names and types, "CREATE TABLE t (INT c)".
        throw SqlUtil.newContextException(create.name.getParserPosition(),
            RESOURCE.createTableRequiresColumnList());
      }
      columnList = new ArrayList<>();
      for (String name : queryRowType.getFieldNames()) {
        columnList.add(new SqlIdentifier(name, SqlParserPos.ZERO));
      }
    }
    final ImmutableList.Builder<ColumnDef> b = ImmutableList.builder();
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    final RelDataTypeFactory.Builder storedBuilder = typeFactory.builder();
    // REVIEW 2019-08-19 Danny Chan: Should we implement the
    // #validate(SqlValidator) to get the SqlValidator instance?
    final SqlValidator validator = validator(context, true);
    for (Ord<SqlNode> c : Ord.zip(columnList)) {
      if (c.e instanceof SqlColumnDeclaration) {
        final SqlColumnDeclaration d = (SqlColumnDeclaration) c.e;
        final RelDataType type = d.dataType.deriveType(validator, true);
        builder.add(d.name.getSimple(), type);
        if (d.strategy != ColumnStrategy.VIRTUAL) {
          storedBuilder.add(d.name.getSimple(), type);
        }
        b.add(ColumnDef.of(d.expression, type, d.strategy));
      } else if (c.e instanceof SqlIdentifier) {
        final SqlIdentifier id = (SqlIdentifier) c.e;
        if (queryRowType == null) {
          throw SqlUtil.newContextException(id.getParserPosition(),
              RESOURCE.createTableRequiresColumnTypes(id.getSimple()));
        }
        final RelDataTypeField f = queryRowType.getFieldList().get(c.i);
        final ColumnStrategy strategy = f.getType().isNullable()
            ? ColumnStrategy.NULLABLE
            : ColumnStrategy.NOT_NULLABLE;
        b.add(ColumnDef.of(c.e, f.getType(), strategy));
        builder.add(id.getSimple(), f.getType());
        storedBuilder.add(id.getSimple(), f.getType());
      } else {
        throw new AssertionError(c.e.getClass());
      }
    }
    final RelDataType rowType = builder.build();
    final RelDataType storedRowType = storedBuilder.build();
    final List<ColumnDef> columns = b.build();
    final InitializerExpressionFactory ief =
        new NullInitializerExpressionFactory() {
          @Override public ColumnStrategy generationStrategy(RelOptTable table,
              int iColumn) {
            return columns.get(iColumn).strategy;
          }

          @Override public RexNode newColumnDefaultValue(RelOptTable table,
              int iColumn, InitializerContext context) {
            final ColumnDef c = columns.get(iColumn);
            if (c.expr != null) {
              // REVIEW Danny 2019-10-09: Should we support validation for DDL nodes?
              final SqlNode validated = context.validateExpression(storedRowType, c.expr);
              // The explicit specified type should have the same nullability
              // with the column expression inferred type,
              // actually they should be exactly the same.
              return context.convertExpression(validated);
            }
            return super.newColumnDefaultValue(table, iColumn, context);
          }
        };
    if (pair.left.plus().tables().get(pair.right) != null) {
      // Table exists.
      if (create.ifNotExists) {
        return;
      }
      if (!create.getReplace()) {
        // They did not specify IF NOT EXISTS, so give error.
        throw SqlUtil.newContextException(create.name.getParserPosition(),
            RESOURCE.tableExists(pair.right));
      }
    }
    // Table does not exist. Create it.
    pair.left.add(pair.right,
        new MutableArrayTable(pair.right,
            RelDataTypeImpl.proto(storedRowType),
            RelDataTypeImpl.proto(rowType), ief));
    if (create.query != null) {
      populate(create.name, create.query, context);
    }
  }

  /** Executes a {@code CREATE TABLE LIKE} command. */
  public void execute(SqlCreateTableLike create,
      CalcitePrepare.Context context) {
    final Pair<@Nullable CalciteSchema, String> pair =
        schema(context, true, create.name);
    requireNonNull(pair.left); // TODO: should not assume parent schema exists
    if (pair.left.plus().tables().get(pair.right) != null) {
      // Table exists.
      if (create.ifNotExists) {
        return;
      }
      if (!create.getReplace()) {
        // They did not specify IF NOT EXISTS, so give error.
        throw SqlUtil.newContextException(create.name.getParserPosition(),
            RESOURCE.tableExists(pair.right));
      }
    }

    final Pair<@Nullable CalciteSchema, String> sourceTablePair =
        schema(context, true, create.sourceTable);
    final CalciteSchema schema =
        // TODO: should not assume parent schema exists
        requireNonNull(sourceTablePair.left);
    final String tableName = sourceTablePair.right;
    final CalciteSchema.TableEntry tableEntry =
        schema.getTable(tableName, context.config().caseSensitive());
    final Table table = requireNonNull(tableEntry, "tableEntry").getTable();

    InitializerExpressionFactory ief = NullInitializerExpressionFactory.INSTANCE;
    if (table instanceof Wrapper) {
      final InitializerExpressionFactory sourceIef =
          ((Wrapper) table).unwrap(InitializerExpressionFactory.class);
      if (sourceIef != null) {
        final Set<SqlCreateTableLike.LikeOption> optionSet = create.options();
        final boolean includingGenerated =
            optionSet.contains(SqlCreateTableLike.LikeOption.GENERATED)
                || optionSet.contains(SqlCreateTableLike.LikeOption.ALL);
        final boolean includingDefaults =
            optionSet.contains(SqlCreateTableLike.LikeOption.DEFAULTS)
                || optionSet.contains(SqlCreateTableLike.LikeOption.ALL);

        // initializes columns based on the source table InitializerExpressionFactory
        // and like options.
        ief =
            new CopiedTableInitializerExpressionFactory(
                includingGenerated, includingDefaults, sourceIef);
      }
    }

    final JavaTypeFactory typeFactory = context.getTypeFactory();
    final RelDataType rowType = table.getRowType(typeFactory);
    // Table does not exist. Create it.
    pair.left.add(pair.right,
        new MutableArrayTable(pair.right,
            RelDataTypeImpl.proto(rowType),
            RelDataTypeImpl.proto(rowType), ief));
  }

  /** Executes a {@code CREATE TYPE} command. */
  public void execute(SqlCreateType create,
      CalcitePrepare.Context context) {
    final Pair<@Nullable CalciteSchema, String> pair =
        schema(context, true, create.name);
    requireNonNull(pair.left); // TODO: should not assume parent schema exists
    final SqlValidator validator = validator(context, false);
    pair.left.add(pair.right, typeFactory -> {
      if (create.dataType != null) {
        return create.dataType.deriveType(validator);
      } else {
        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        if (create.attributeDefs != null) {
          for (SqlNode def : create.attributeDefs) {
            final SqlAttributeDefinition attributeDef =
                (SqlAttributeDefinition) def;
            final SqlDataTypeSpec typeSpec = attributeDef.dataType;
            final RelDataType type = typeSpec.deriveType(validator);
            builder.add(attributeDef.name.getSimple(), type);
          }
        }
        return builder.build();
      }
    });
  }

  /** Executes a {@code CREATE VIEW} command. */
  public void execute(SqlCreateView create,
      CalcitePrepare.Context context) {
    final Pair<@Nullable CalciteSchema, String> pair =
        schema(context, true, create.name);
    requireNonNull(pair.left); // TODO: should not assume parent schema exists
    final SchemaPlus schemaPlus = pair.left.plus();
    for (Function function : schemaPlus.getFunctions(pair.right)) {
      if (function.getParameters().isEmpty()) {
        if (!create.getReplace()) {
          throw SqlUtil.newContextException(create.name.getParserPosition(),
              RESOURCE.viewExists(pair.right));
        }
        pair.left.removeFunction(pair.right);
      }
    }
    final SqlNode q = renameColumns(create.columnList, create.query);
    final String sql = q.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
    final ViewTableMacro viewTableMacro =
        ViewTable.viewMacro(schemaPlus, sql, pair.left.path(null),
            context.getObjectPath(), false);
    final TranslatableTable x = viewTableMacro.apply(ImmutableList.of());
    Util.discard(x);
    schemaPlus.add(pair.right, viewTableMacro);
  }

  /**
   * Initializes columns based on the source {@link InitializerExpressionFactory}
   * and like options.
   */
  private static class CopiedTableInitializerExpressionFactory
      extends NullInitializerExpressionFactory {

    private final boolean includingGenerated;
    private final boolean includingDefaults;
    private final InitializerExpressionFactory sourceIef;

    CopiedTableInitializerExpressionFactory(
        boolean includingGenerated,
        boolean includingDefaults,
        InitializerExpressionFactory sourceIef) {
      this.includingGenerated = includingGenerated;
      this.includingDefaults = includingDefaults;
      this.sourceIef = sourceIef;
    }

    @Override public ColumnStrategy generationStrategy(
        RelOptTable table, int iColumn) {
      final ColumnStrategy sourceStrategy = sourceIef.generationStrategy(table, iColumn);
      if (includingGenerated
          && (sourceStrategy == ColumnStrategy.STORED
          || sourceStrategy == ColumnStrategy.VIRTUAL)) {
        return sourceStrategy;
      }
      if (includingDefaults && sourceStrategy == ColumnStrategy.DEFAULT) {
        return ColumnStrategy.DEFAULT;
      }

      return super.generationStrategy(table, iColumn);
    }

    @Override public RexNode newColumnDefaultValue(
        RelOptTable table, int iColumn, InitializerContext context) {
      if (includingDefaults || includingGenerated) {
        return sourceIef.newColumnDefaultValue(table, iColumn, context);
      } else {
        return super.newColumnDefaultValue(table, iColumn, context);
      }
    }
  }

  /** Column definition. */
  private static class ColumnDef {
    final @Nullable SqlNode expr;
    final RelDataType type;
    final ColumnStrategy strategy;

    private ColumnDef(@Nullable SqlNode expr, RelDataType type,
        ColumnStrategy strategy) {
      this.expr = expr;
      this.type = type;
      this.strategy = requireNonNull(strategy, "strategy");
      checkArgument(
          strategy == ColumnStrategy.NULLABLE
              || strategy == ColumnStrategy.NOT_NULLABLE
              || expr != null);
    }

    static ColumnDef of(@Nullable SqlNode expr, RelDataType type,
        ColumnStrategy strategy) {
      return new ColumnDef(expr, type, strategy);
    }
  }
}
