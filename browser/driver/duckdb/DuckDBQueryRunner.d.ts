import { BaseQueryRunner } from "../../query-runner/BaseQueryRunner";
import { QueryRunner } from "../../query-runner/QueryRunner";
import { Table } from "../../schema-builder/table/Table";
import { TableCheck } from "../../schema-builder/table/TableCheck";
import { TableColumn } from "../../schema-builder/table/TableColumn";
import { TableExclusion } from "../../schema-builder/table/TableExclusion";
import { TableForeignKey } from "../../schema-builder/table/TableForeignKey";
import { TableIndex } from "../../schema-builder/table/TableIndex";
import { TableUnique } from "../../schema-builder/table/TableUnique";
import { View } from "../../schema-builder/view/View";
import { IsolationLevel } from "../types/IsolationLevel";
import { ReplicationMode } from "../types/ReplicationMode";
import { DuckDBDriver } from "./DuckDBDriver";
import { Query } from "../Query";
/**
 * Runs queries on a single DuckDB database connection.
 */
export declare class DuckDBQueryRunner extends BaseQueryRunner implements QueryRunner {
    /**
     * Database driver used by connection.
     */
    driver: DuckDBDriver;
    /**
     * Database connection used by query runner.
     */
    databaseConnection: any;
    constructor(driver: DuckDBDriver, mode?: ReplicationMode);
    /**
     * Creates/uses database connection from the connection pool to perform further operations.
     * Returns obtained database connection.
     */
    connect(): Promise<any>;
    /**
     * Releases used database connection.
     */
    release(): Promise<void>;
    /**
     * Starts transaction.
     */
    startTransaction(isolationLevel?: IsolationLevel): Promise<void>;
    /**
     * Commits transaction.
     */
    commitTransaction(): Promise<void>;
    /**
     * Rollbacks transaction.
     */
    rollbackTransaction(): Promise<void>;
    /**
     * Executes a given SQL query.
     */
    query(query: string, parameters?: any[], useStructuredResult?: boolean): Promise<any>;
    /**
     * Returns raw data stream.
     */
    stream(query: string, parameters?: any[], onEnd?: Function, onError?: Function): Promise<any>;
    /**
     * Returns all available database names including system databases.
     */
    getDatabases(): Promise<string[]>;
    /**
     * Returns all available schema names including system schemas.
     */
    getSchemas(database?: string): Promise<string[]>;
    /**
     * Checks if database with the given name exist.
     */
    hasDatabase(database: string): Promise<boolean>;
    /**
     * Loads currently using database
     */
    getCurrentDatabase(): Promise<string>;
    /**
     * Checks if schema with the given name exist.
     */
    hasSchema(schema: string): Promise<boolean>;
    /**
     * Loads currently using schema
     */
    getCurrentSchema(): Promise<string>;
    /**
     * Checks if table with the given name exist in the given database.
     */
    hasTable(tableOrName: Table | string): Promise<boolean>;
    /**
     * Checks if column with the given name exist in the given table.
     */
    hasColumn(tableOrName: Table | string, column: string): Promise<boolean>;
    /**
     * Creates a new database.
     */
    createDatabase(database: string, ifNotExist?: boolean): Promise<void>;
    /**
     * Drops database.
     */
    dropDatabase(database: string, ifExist?: boolean): Promise<void>;
    /**
     * Creates a new table schema.
     */
    createSchema(schemaPath: string, ifNotExist?: boolean): Promise<void>;
    /**
     * Drops table schema.
     */
    dropSchema(schemaPath: string, ifExist?: boolean, cascade?: boolean): Promise<void>;
    /**
     * Creates a new table.
     */
    createTable(table: Table, ifNotExist?: boolean, createForeignKeys?: boolean, createIndices?: boolean): Promise<void>;
    /**
     * Drops the table.
     */
    dropTable(target: Table | string, ifExist?: boolean, dropForeignKeys?: boolean, dropIndices?: boolean): Promise<void>;
    /**
     * Creates a new view.
     */
    createView(view: View, syncWithMetadata?: boolean, oldView?: View): Promise<void>;
    /**
     * Drops the view.
     */
    dropView(target: View | string): Promise<void>;
    /**
     * Renames the given table.
     */
    renameTable(oldTableOrName: Table | string, newTableName: string): Promise<void>;
    /**
     * Creates a new column from the column in the table.
     */
    addColumn(tableOrName: Table | string, column: TableColumn): Promise<void>;
    /**
     * Creates a new columns from the column in the table.
     */
    addColumns(tableOrName: Table | string, columns: TableColumn[]): Promise<void>;
    /**
     * Renames column in the given table.
     */
    renameColumn(tableOrName: Table | string, oldTableColumnOrName: TableColumn | string, newTableColumnOrName: TableColumn | string): Promise<void>;
    /**
     * Changes a column in the table.
     */
    changeColumn(tableOrName: Table | string, oldTableColumnOrName: TableColumn | string, newColumn: TableColumn): Promise<void>;
    /**
     * Changes a column in the table.
     */
    changeColumns(tableOrName: Table | string, changedColumns: {
        newColumn: TableColumn;
        oldColumn: TableColumn;
    }[]): Promise<void>;
    /**
     * Drops column in the table.
     */
    dropColumn(tableOrName: Table | string, columnOrName: TableColumn | string): Promise<void>;
    /**
     * Drops the columns in the table.
     */
    dropColumns(tableOrName: Table | string, columns: TableColumn[]): Promise<void>;
    /**
     * Creates a new primary key.
     */
    createPrimaryKey(tableOrName: Table | string, columnNames: string[]): Promise<void>;
    /**
     * Updates composite primary keys.
     */
    updatePrimaryKeys(tableOrName: Table | string, columns: TableColumn[]): Promise<void>;
    /**
     * Drops a primary key.
     */
    dropPrimaryKey(tableOrName: Table | string): Promise<void>;
    /**
     * Creates a new unique constraint.
     */
    createUniqueConstraint(tableOrName: Table | string, uniqueConstraint: TableUnique): Promise<void>;
    /**
     * Creates new unique constraints.
     */
    createUniqueConstraints(tableOrName: Table | string, uniqueConstraints: TableUnique[]): Promise<void>;
    /**
     * Drops unique constraint.
     */
    dropUniqueConstraint(tableOrName: Table | string, uniqueOrName: TableUnique | string): Promise<void>;
    /**
     * Drops unique constraints.
     */
    dropUniqueConstraints(tableOrName: Table | string, uniqueConstraints: TableUnique[]): Promise<void>;
    /**
     * Creates new check constraint.
     */
    createCheckConstraint(tableOrName: Table | string, checkConstraint: TableCheck): Promise<void>;
    /**
     * Creates new check constraints.
     */
    createCheckConstraints(tableOrName: Table | string, checkConstraints: TableCheck[]): Promise<void>;
    /**
     * Drops check constraint.
     */
    dropCheckConstraint(tableOrName: Table | string, checkOrName: TableCheck | string): Promise<void>;
    /**
     * Drops check constraints.
     */
    dropCheckConstraints(tableOrName: Table | string, checkConstraints: TableCheck[]): Promise<void>;
    /**
     * Creates a new foreign key.
     */
    createForeignKey(tableOrName: Table | string, foreignKey: TableForeignKey): Promise<void>;
    /**
     * Creates new foreign keys.
     */
    createForeignKeys(tableOrName: Table | string, foreignKeys: TableForeignKey[]): Promise<void>;
    /**
     * Drops a foreign key from the table.
     */
    dropForeignKey(tableOrName: Table | string, foreignKeyOrName: TableForeignKey | string): Promise<void>;
    /**
     * Drops foreign keys from the table.
     */
    dropForeignKeys(tableOrName: Table | string, foreignKeys: TableForeignKey[]): Promise<void>;
    /**
     * Creates a new index.
     */
    createIndex(tableOrName: Table | string, index: TableIndex): Promise<void>;
    /**
     * Creates new indices
     */
    createIndices(tableOrName: Table | string, indices: TableIndex[]): Promise<void>;
    /**
     * Drops an index from the table.
     */
    dropIndex(tableOrName: Table | string, indexOrName: TableIndex | string): Promise<void>;
    /**
     * Drops indices from the table.
     */
    dropIndices(tableOrName: Table | string, indices: TableIndex[]): Promise<void>;
    /**
     * Clears all table contents.
     */
    clearTable(tableName: string): Promise<void>;
    /**
     * Called before migrations are run.
     */
    beforeMigration(): Promise<void>;
    /**
     * Called after migrations are run.
     */
    afterMigration(): Promise<void>;
    /**
     * Removes all tables from the currently connected database.
     */
    clearDatabase(): Promise<void>;
    /**
     * Changes table comment.
     */
    changeTableComment(tableOrName: Table | string, newComment?: string): Promise<void>;
    /**
     * Creates a new exclusion constraint.
     */
    createExclusionConstraint(tableOrName: Table | string, exclusionConstraint: TableExclusion): Promise<void>;
    /**
     * Creates new exclusion constraints.
     */
    createExclusionConstraints(tableOrName: Table | string, exclusionConstraints: TableExclusion[]): Promise<void>;
    /**
     * Drops exclusion constraint.
     */
    dropExclusionConstraint(tableOrName: Table | string, exclusionOrName: TableExclusion | string): Promise<void>;
    /**
     * Drops exclusion constraints.
     */
    dropExclusionConstraints(tableOrName: Table | string, exclusionConstraints: TableExclusion[]): Promise<void>;
    protected loadViews(viewNames?: string[]): Promise<View[]>;
    protected loadTables(tableNames?: string[]): Promise<Table[]>;
    /**
     * Builds create table sql.
     */
    protected createTableSql(table: Table, createForeignKeys?: boolean): Query;
    /**
     * Builds drop table sql.
     */
    protected dropTableSql(tableOrName: Table | string, ifExist?: boolean): Query;
    protected createViewSql(view: View): Query;
    protected insertViewDefinitionSql(view: View): Query;
    protected dropViewSql(viewOrPath: View | string): Query;
    protected deleteViewDefinitionSql(viewOrPath: View | string): Query;
    protected createIndexSql(table: Table, index: TableIndex): Query;
    protected dropIndexSql(table: Table, indexOrName: TableIndex | string): Query;
    /**
     * Builds create column sql.
     */
    protected buildCreateColumnSql(column: TableColumn): string;
    /**
     * Executes up sql queries.
     */
    protected executeQueries(upQueries: Query | Query[], downQueries: Query | Query[]): Promise<void>;
}
