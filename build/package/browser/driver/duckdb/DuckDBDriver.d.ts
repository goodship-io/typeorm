import { ObjectLiteral } from "../../common/ObjectLiteral";
import { DataSource } from "../../data-source/DataSource";
import { ColumnMetadata } from "../../metadata/ColumnMetadata";
import { EntityMetadata } from "../../metadata/EntityMetadata";
import { QueryRunner } from "../../query-runner/QueryRunner";
import { RdbmsSchemaBuilder } from "../../schema-builder/RdbmsSchemaBuilder";
import { TableColumn } from "../../schema-builder/table/TableColumn";
import { Driver } from "../Driver";
import { ColumnType } from "../types/ColumnTypes";
import { CteCapabilities } from "../types/CteCapabilities";
import { DataTypeDefaults } from "../types/DataTypeDefaults";
import { MappedColumnTypes } from "../types/MappedColumnTypes";
import { ReplicationMode } from "../types/ReplicationMode";
import { UpsertType } from "../types/UpsertType";
import { DuckDBConnectionOptions } from "./DuckDBConnectionOptions";
import { DuckDBQueryRunner } from "./DuckDBQueryRunner";
import { Table } from "../../schema-builder/table/Table";
import { View } from "../../schema-builder/view/View";
import { TableForeignKey } from "../../schema-builder/table/TableForeignKey";
/**
 * Organizes communication with DuckDB DBMS.
 */
export declare class DuckDBDriver implements Driver {
    /**
     * Connection used by driver.
     */
    connection: DataSource;
    /**
     * DuckDB underlying library.
     */
    duckdb: any;
    /**
     * DuckDB database connection.
     */
    databaseConnection: any;
    /**
     * We store all created query runners because we need to release them.
     */
    connectedQueryRunners: QueryRunner[];
    /**
     * Connection options.
     */
    options: DuckDBConnectionOptions;
    /**
     * Version of DuckDB.
     */
    version?: string;
    /**
     * Database name used to perform all write queries.
     */
    database?: string;
    /**
     * Schema name used to perform all write queries.
     */
    schema?: string;
    /**
     * Indicates if replication is enabled.
     */
    isReplicated: boolean;
    /**
     * Indicates if tree tables are supported by this driver.
     */
    treeSupport: boolean;
    /**
     * Represent transaction support by this driver
     */
    transactionSupport: "nested";
    /**
     * Gets list of supported column data types by a driver.
     */
    supportedDataTypes: ColumnType[];
    /**
     * Gets list of supported upsert types by a driver.
     */
    supportedUpsertTypes: UpsertType[];
    /**
     * Gets list of spatial column data types.
     */
    spatialTypes: ColumnType[];
    /**
     * Gets list of column data types that support length by a driver.
     */
    withLengthColumnTypes: ColumnType[];
    /**
     * Gets list of column data types that support precision by a driver.
     */
    withPrecisionColumnTypes: ColumnType[];
    /**
     * Gets list of column data types that support scale by a driver.
     */
    withScaleColumnTypes: ColumnType[];
    /**
     * Orm has special columns and we need to know what database column types should be for those types.
     */
    mappedDataTypes: MappedColumnTypes;
    /**
     * Default values of length, precision and scale depends on column data type.
     */
    dataTypeDefaults: DataTypeDefaults;
    /**
     * Max length allowed by DuckDB for aliases.
     */
    maxAliasLength: number;
    cteCapabilities: CteCapabilities;
    constructor(connection: DataSource);
    /**
     * Performs connection to the database.
     */
    connect(): Promise<void>;
    /**
     * Makes any action after connection (e.g. create extensions, install functions, etc.).
     */
    afterConnect(): Promise<void>;
    /**
     * Closes connection with database.
     */
    disconnect(): Promise<void>;
    /**
     * Creates a schema builder used to build and sync a schema.
     */
    createSchemaBuilder(): RdbmsSchemaBuilder;
    /**
     * Creates a query runner used to execute database queries.
     */
    createQueryRunner(mode?: ReplicationMode): DuckDBQueryRunner;
    /**
     * Replaces parameters in the given sql with special escaping character
     * and an array of parameter names to be passed to a query.
     */
    escapeQueryWithParameters(sql: string, parameters: ObjectLiteral, nativeParameters: ObjectLiteral): [string, any[]];
    /**
     * Escape a table name.
     */
    escape(name: string): string;
    /**
     * Build full table name with database name, schema name and table name.
     */
    buildTableName(tableName: string, schema?: string, database?: string): string;
    /**
     * Parse a target table name or other types and return a normalized table definition.
     */
    parseTableName(target: EntityMetadata | Table | View | TableForeignKey | string): {
        database?: string;
        schema?: string;
        tableName: string;
    };
    /**
     * Prepares given value to a value to be persisted, based on its column type and metadata.
     */
    preparePersistentValue(value: any, columnMetadata: ColumnMetadata): any;
    /**
     * Prepares given value to a value to be persisted, based on its column type or metadata.
     */
    prepareHydratedValue(value: any, columnMetadata: ColumnMetadata): any;
    /**
     * Creates a database type from a given column metadata.
     */
    normalizeType(column: {
        type?: ColumnType;
        length?: number | string;
        precision?: number | null;
        scale?: number;
        array?: boolean;
    }): string;
    /**
     * Normalizes "default" value of the column.
     */
    normalizeDefault(columnMetadata: ColumnMetadata): string | undefined;
    /**
     * Normalizes "isUnique" value of the column.
     */
    normalizeIsUnique(column: ColumnMetadata): boolean;
    /**
     * Returns default column lengths, which is required on column creation.
     */
    getColumnLength(column: ColumnMetadata | TableColumn): string;
    /**
     * Creates column type definition including length, precision and scale
     */
    createFullType(column: TableColumn): string;
    /**
     * Obtains a new database connection to a master server.
     * Used for replication.
     * If replication is not setup then returns default connection's database connection.
     */
    obtainMasterConnection(): Promise<any>;
    /**
     * Obtains a new database connection to a slave server.
     * Used for replication.
     * If replication is not setup then returns master (default) connection's database connection.
     */
    obtainSlaveConnection(): Promise<any>;
    /**
     * Creates generated map of values generated or returned by database after INSERT query.
     */
    createGeneratedMap(metadata: EntityMetadata, insertResult: ObjectLiteral, entityIndex: number): {};
    /**
     * Differentiate columns of this table and columns from the given column metadatas columns
     * and returns only changed.
     */
    findChangedColumns(tableColumns: TableColumn[], columnMetadatas: ColumnMetadata[]): ColumnMetadata[];
    /**
     * Returns true if driver supports RETURNING / OUTPUT statement.
     */
    isReturningSqlSupported(): boolean;
    /**
     * Returns true if driver supports uuid values generation on its own.
     */
    isUUIDGenerationSupported(): boolean;
    /**
     * Returns true if driver supports fulltext indices.
     */
    isFullTextColumnTypeSupported(): boolean;
    /**
     * Creates an escaped parameter.
     */
    createParameter(parameterName: string, index: number): string;
    /**
     * Loads all driver dependencies.
     */
    protected loadDependencies(): void;
    /**
     * Creates a new connection pool for a given database credentials.
     */
    protected createDatabaseConnection(): Promise<any>;
    /**
     * If driver dependency is not given explicitly, then try to load it via "require".
     */
    protected query(sql: string, parameters?: any[]): Promise<any>;
}
