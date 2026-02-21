"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DuckDBDriver = void 0;
const ConnectionIsNotSetError_1 = require("../../error/ConnectionIsNotSetError");
const DriverPackageNotInstalledError_1 = require("../../error/DriverPackageNotInstalledError");
const PlatformTools_1 = require("../../platform/PlatformTools");
const RdbmsSchemaBuilder_1 = require("../../schema-builder/RdbmsSchemaBuilder");
const ApplyValueTransformers_1 = require("../../util/ApplyValueTransformers");
const DateUtils_1 = require("../../util/DateUtils");
const DuckDBQueryRunner_1 = require("./DuckDBQueryRunner");
const InstanceChecker_1 = require("../../util/InstanceChecker");
/**
 * Organizes communication with DuckDB DBMS.
 */
class DuckDBDriver {
    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------
    constructor(connection) {
        /**
         * We store all created query runners because we need to release them.
         */
        this.connectedQueryRunners = [];
        /**
         * Indicates if replication is enabled.
         */
        this.isReplicated = false;
        /**
         * Indicates if tree tables are supported by this driver.
         */
        this.treeSupport = true;
        /**
         * Represent transaction support by this driver
         */
        this.transactionSupport = "nested";
        /**
         * Gets list of supported column data types by a driver.
         */
        this.supportedDataTypes = [
            "boolean",
            "tinyint",
            "smallint",
            "integer",
            "bigint",
            "real",
            "double",
            "decimal",
            "varchar",
            "text",
            "blob",
            "date",
            "time",
            "timestamp",
            "timestamptz",
            "interval",
            "uuid",
            "json",
            "array",
        ];
        /**
         * Gets list of supported upsert types by a driver.
         */
        this.supportedUpsertTypes = ["on-conflict-do-update"];
        /**
         * Gets list of spatial column data types.
         */
        this.spatialTypes = [
            "geometry",
            "geography",
            "point",
            "linestring",
            "polygon",
            "multipoint",
            "multilinestring",
            "multipolygon",
            "geometrycollection",
        ];
        /**
         * Gets list of column data types that support length by a driver.
         */
        this.withLengthColumnTypes = ["varchar", "decimal"];
        /**
         * Gets list of column data types that support precision by a driver.
         */
        this.withPrecisionColumnTypes = [
            "decimal",
            "time",
            "timestamp",
            "timestamptz",
        ];
        /**
         * Gets list of column data types that support scale by a driver.
         */
        this.withScaleColumnTypes = ["decimal"];
        /**
         * Orm has special columns and we need to know what database column types should be for those types.
         */
        this.mappedDataTypes = {
            createDate: "timestamp",
            createDateDefault: "CURRENT_TIMESTAMP",
            updateDate: "timestamp",
            updateDateDefault: "CURRENT_TIMESTAMP",
            deleteDate: "timestamp",
            deleteDateNullable: true,
            version: "integer",
            treeLevel: "integer",
            migrationId: "integer",
            migrationName: "varchar",
            migrationTimestamp: "bigint",
            cacheId: "varchar",
            cacheIdentifier: "varchar",
            cacheTime: "bigint",
            cacheDuration: "integer",
            cacheQuery: "text",
            cacheResult: "text",
            metadataType: "varchar",
            metadataDatabase: "varchar",
            metadataSchema: "varchar",
            metadataTable: "varchar",
            metadataName: "varchar",
            metadataValue: "text",
        };
        /**
         * Default values of length, precision and scale depends on column data type.
         */
        this.dataTypeDefaults = {
            varchar: { length: 255 },
            decimal: { precision: 10, scale: 0 },
        };
        /**
         * Max length allowed by DuckDB for aliases.
         */
        this.maxAliasLength = 63;
        this.cteCapabilities = {
            enabled: true,
            requiresRecursiveHint: true,
            materializedHint: true,
            writable: false,
        };
        this.connection = connection;
        this.options = connection.options;
        this.database = this.options.database;
        this.loadDependencies();
    }
    // -------------------------------------------------------------------------
    // Public Methods
    // -------------------------------------------------------------------------
    /**
     * Performs connection to the database.
     */
    async connect() {
        this.databaseConnection = await this.createDatabaseConnection();
    }
    /**
     * Makes any action after connection (e.g. create extensions, install functions, etc.).
     */
    async afterConnect() {
        // DuckDB might need extension loading or configuration setup
        if (this.options.config) {
            for (const [key, value] of Object.entries(this.options.config)) {
                if (key !== "max_memory" &&
                    key !== "threads" &&
                    typeof value !== "object") {
                    await this.query(`SET ${key} = ${this.escape(value)}`);
                }
            }
        }
    }
    /**
     * Closes connection with database.
     */
    async disconnect() {
        if (!this.databaseConnection)
            return;
        this.connectedQueryRunners.forEach((queryRunner) => {
            const duckdbQueryRunner = queryRunner;
            if (duckdbQueryRunner.databaseConnection) {
                duckdbQueryRunner.databaseConnection.close();
            }
        });
        return new Promise((ok, fail) => {
            this.databaseConnection.close((err) => {
                if (err)
                    fail(err);
                else
                    ok();
            });
        });
    }
    /**
     * Creates a schema builder used to build and sync a schema.
     */
    createSchemaBuilder() {
        return new RdbmsSchemaBuilder_1.RdbmsSchemaBuilder(this.connection);
    }
    /**
     * Creates a query runner used to execute database queries.
     */
    createQueryRunner(mode) {
        return new DuckDBQueryRunner_1.DuckDBQueryRunner(this, mode);
    }
    /**
     * Replaces parameters in the given sql with special escaping character
     * and an array of parameter names to be passed to a query.
     */
    escapeQueryWithParameters(sql, parameters, nativeParameters) {
        const escapedParameters = Object.keys(nativeParameters).map((key) => nativeParameters[key]);
        if (!parameters || !Object.keys(parameters).length)
            return [sql, escapedParameters];
        sql = sql.replace(/:(\.\.\.)?([A-Za-z0-9_.]+)/g, (full, isArray, key) => {
            if (!parameters.hasOwnProperty(key)) {
                return full;
            }
            let value = parameters[key];
            if (isArray) {
                return value
                    .map((v) => {
                    escapedParameters.push(v);
                    return `$${escapedParameters.length}`;
                })
                    .join(", ");
            }
            if (typeof value === "function") {
                return value();
            }
            escapedParameters.push(value);
            return `$${escapedParameters.length}`;
        });
        return [sql, escapedParameters];
    }
    /**
     * Escape a table name.
     */
    escape(name) {
        return `"${name}"`;
    }
    /**
     * Build full table name with database name, schema name and table name.
     */
    buildTableName(tableName, schema, database) {
        let result = tableName;
        if (schema) {
            result = `${schema}.${result}`;
        }
        if (database) {
            result = `${database}.${result}`;
        }
        return result;
    }
    /**
     * Parse a target table name or other types and return a normalized table definition.
     */
    parseTableName(target) {
        const driverDatabase = this.database;
        const driverSchema = this.schema;
        if (InstanceChecker_1.InstanceChecker.isTable(target) || InstanceChecker_1.InstanceChecker.isView(target)) {
            const parsed = this.parseTableName(target.name);
            return {
                database: target.database || parsed.database || driverDatabase,
                schema: target.schema || parsed.schema || driverSchema,
                tableName: parsed.tableName,
            };
        }
        if (InstanceChecker_1.InstanceChecker.isTableForeignKey(target)) {
            const parsed = this.parseTableName(target.referencedTableName);
            return {
                database: target.referencedDatabase ||
                    parsed.database ||
                    driverDatabase,
                schema: target.referencedSchema || parsed.schema || driverSchema,
                tableName: parsed.tableName,
            };
        }
        if (InstanceChecker_1.InstanceChecker.isEntityMetadata(target)) {
            return {
                database: target.database || driverDatabase,
                schema: target.schema || driverSchema,
                tableName: target.tableName,
            };
        }
        const parts = target.split(".");
        return {
            database: (parts.length > 2 ? parts[0] : undefined) || driverDatabase,
            schema: (parts.length > 2
                ? parts[1]
                : parts.length > 1
                    ? parts[0]
                    : undefined) || driverSchema,
            tableName: parts.length > 2
                ? parts[2]
                : parts.length > 1
                    ? parts[1]
                    : parts[0],
        };
    }
    /**
     * Prepares given value to a value to be persisted, based on its column type and metadata.
     */
    preparePersistentValue(value, columnMetadata) {
        if (columnMetadata.transformer)
            value = ApplyValueTransformers_1.ApplyValueTransformers.transformTo(columnMetadata.transformer, value);
        if (value === null || value === undefined)
            return value;
        if (columnMetadata.type === Boolean) {
            return value === true ? 1 : 0;
        }
        else if (columnMetadata.type === "date") {
            return DateUtils_1.DateUtils.mixedDateToDateString(value);
        }
        else if (columnMetadata.type === "time") {
            return DateUtils_1.DateUtils.mixedDateToTimeString(value);
        }
        else if (columnMetadata.type === "timestamp" ||
            columnMetadata.type === "timestamptz" ||
            columnMetadata.type === Date) {
            return DateUtils_1.DateUtils.mixedDateToDate(value);
        }
        else if (["json", "jsonb", "array", "struct", "map"].indexOf(columnMetadata.type) >= 0) {
            return JSON.stringify(value);
        }
        return value;
    }
    /**
     * Prepares given value to a value to be persisted, based on its column type or metadata.
     */
    prepareHydratedValue(value, columnMetadata) {
        if (value === null || value === undefined)
            return columnMetadata.transformer
                ? ApplyValueTransformers_1.ApplyValueTransformers.transformFrom(columnMetadata.transformer, value)
                : value;
        if (columnMetadata.type === Boolean) {
            value = value ? true : false;
        }
        else if (columnMetadata.type === "timestamp" ||
            columnMetadata.type === "timestamptz" ||
            columnMetadata.type === Date) {
            value = DateUtils_1.DateUtils.normalizeHydratedDate(value);
        }
        else if (columnMetadata.type === "date") {
            value = DateUtils_1.DateUtils.mixedDateToDateString(value);
        }
        else if (columnMetadata.type === "time") {
            value = DateUtils_1.DateUtils.mixedDateToTimeString(value);
        }
        else if (["json", "jsonb", "array", "struct", "map"].indexOf(columnMetadata.type) >= 0) {
            if (typeof value === "string") {
                try {
                    value = JSON.parse(value);
                }
                catch (error) {
                    // Handle parsing errors gracefully
                }
            }
        }
        if (columnMetadata.transformer)
            value = ApplyValueTransformers_1.ApplyValueTransformers.transformFrom(columnMetadata.transformer, value);
        return value;
    }
    /**
     * Creates a database type from a given column metadata.
     */
    normalizeType(column) {
        if (column.type === Number || column.type === "integer") {
            return "integer";
        }
        else if (column.type === String || column.type === "varchar") {
            return "varchar" + (column.length ? `(${column.length})` : "");
        }
        else if (column.type === Date || column.type === "timestamp") {
            return ("timestamp" + (column.precision ? `(${column.precision})` : ""));
        }
        else if (column.type === "timestamptz") {
            return ("timestamptz" +
                (column.precision ? `(${column.precision})` : ""));
        }
        else if (column.type === Boolean || column.type === "boolean") {
            return "boolean";
        }
        else if (column.type === "decimal") {
            if (column.precision && column.scale) {
                return `decimal(${column.precision},${column.scale})`;
            }
            else if (column.precision) {
                return `decimal(${column.precision})`;
            }
            return "decimal";
        }
        else if (column.type === Buffer) {
            return "blob";
        }
        else if (column.type === "uuid") {
            return "uuid";
        }
        else if (column.type === "json") {
            return "json";
        }
        return column.type || "";
    }
    /**
     * Normalizes "default" value of the column.
     */
    normalizeDefault(columnMetadata) {
        const defaultValue = columnMetadata.default;
        if (typeof defaultValue === "number") {
            return `${defaultValue}`;
        }
        else if (typeof defaultValue === "boolean") {
            return defaultValue ? "true" : "false";
        }
        else if (typeof defaultValue === "function") {
            const value = defaultValue();
            return this.normalizeDefault({
                ...columnMetadata,
                default: value,
            });
        }
        else if (typeof defaultValue === "string") {
            return `'${defaultValue}'`;
        }
        else if (defaultValue === null || defaultValue === undefined) {
            return undefined;
        }
        else if (typeof defaultValue === "object" && defaultValue !== null) {
            return `${defaultValue}`;
        }
        else {
            return `${defaultValue}`;
        }
    }
    /**
     * Normalizes "isUnique" value of the column.
     */
    normalizeIsUnique(column) {
        return column.entityMetadata.uniques.some((uq) => uq.columns.length === 1 && uq.columns[0] === column);
    }
    /**
     * Returns default column lengths, which is required on column creation.
     */
    getColumnLength(column) {
        if (column.length)
            return column.length.toString();
        const columnType = column.type;
        switch (columnType) {
            case String:
            case "varchar":
                return "255";
            case "uuid":
                return "36";
            default:
                return "";
        }
    }
    /**
     * Creates column type definition including length, precision and scale
     */
    createFullType(column) {
        let type = column.type;
        if (this.getColumnLength(column)) {
            type += `(${this.getColumnLength(column)})`;
        }
        else if (column.width ||
            (column.precision !== null &&
                column.precision !== undefined &&
                column.scale !== null &&
                column.scale !== undefined)) {
            type += `(${column.precision || column.width},${column.scale || 0})`;
        }
        else if (column.precision !== null &&
            column.precision !== undefined) {
            type += `(${column.precision})`;
        }
        if (column.isArray)
            type += " array";
        return type;
    }
    /**
     * Obtains a new database connection to a master server.
     * Used for replication.
     * If replication is not setup then returns default connection's database connection.
     */
    obtainMasterConnection() {
        return new Promise((ok, fail) => {
            if (this.databaseConnection) {
                return ok(this.databaseConnection);
            }
            fail(new ConnectionIsNotSetError_1.ConnectionIsNotSetError("duckdb"));
        });
    }
    /**
     * Obtains a new database connection to a slave server.
     * Used for replication.
     * If replication is not setup then returns master (default) connection's database connection.
     */
    obtainSlaveConnection() {
        return this.obtainMasterConnection();
    }
    /**
     * Creates generated map of values generated or returned by database after INSERT query.
     */
    createGeneratedMap(metadata, insertResult, entityIndex) {
        return {};
    }
    /**
     * Differentiate columns of this table and columns from the given column metadatas columns
     * and returns only changed.
     */
    findChangedColumns(tableColumns, columnMetadatas) {
        return columnMetadatas.filter((columnMetadata) => {
            const tableColumn = tableColumns.find((c) => c.name === columnMetadata.databaseName);
            if (!tableColumn)
                return false;
            const isColumnChanged = tableColumn.name !== columnMetadata.databaseName ||
                tableColumn.type !== this.normalizeType(columnMetadata) ||
                (tableColumn.length || "").toString() !==
                    (columnMetadata.length || "").toString() ||
                tableColumn.precision !== columnMetadata.precision ||
                tableColumn.scale !== columnMetadata.scale ||
                tableColumn.default !== columnMetadata.default ||
                tableColumn.isPrimary !== columnMetadata.isPrimary ||
                tableColumn.isNullable !== columnMetadata.isNullable ||
                tableColumn.isUnique !==
                    this.normalizeIsUnique(columnMetadata) ||
                tableColumn.isGenerated !== columnMetadata.isGenerated;
            return isColumnChanged;
        });
    }
    /**
     * Returns true if driver supports RETURNING / OUTPUT statement.
     */
    isReturningSqlSupported() {
        return true;
    }
    /**
     * Returns true if driver supports uuid values generation on its own.
     */
    isUUIDGenerationSupported() {
        return true;
    }
    /**
     * Returns true if driver supports fulltext indices.
     */
    isFullTextColumnTypeSupported() {
        return false;
    }
    /**
     * Creates an escaped parameter.
     */
    createParameter(parameterName, index) {
        return `$${index + 1}`;
    }
    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------
    /**
     * Loads all driver dependencies.
     */
    loadDependencies() {
        try {
            const duckdbPackage = this.options.driver || PlatformTools_1.PlatformTools.load("@duckdb/node-api");
            this.duckdb = duckdbPackage;
        }
        catch (e) {
            throw new DriverPackageNotInstalledError_1.DriverPackageNotInstalledError("DuckDB", "@duckdb/node-api");
        }
    }
    /**
     * Creates a new connection pool for a given database credentials.
     */
    async createDatabaseConnection() {
        const { database, config, readOnly, accessMode } = this.options;
        return new Promise((ok, fail) => {
            const options = {};
            if (config) {
                Object.assign(options, config);
            }
            if (readOnly !== undefined) {
                options.access_mode = readOnly ? "read_only" : "read_write";
            }
            else if (accessMode) {
                options.access_mode = accessMode;
            }
            const connection = new this.duckdb.Database(database, options, (err) => {
                if (err)
                    return fail(err);
                ok(connection);
            });
        });
    }
    /**
     * If driver dependency is not given explicitly, then try to load it via "require".
     */
    async query(sql, parameters) {
        return new Promise((ok, fail) => {
            this.databaseConnection.all(sql, parameters || [], (err, result) => {
                if (err)
                    return fail(err);
                ok(result);
            });
        });
    }
}
exports.DuckDBDriver = DuckDBDriver;

//# sourceMappingURL=DuckDBDriver.js.map
