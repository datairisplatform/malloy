/*
 * Copyright 2023 Google LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files
 * (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

// LTNOTE: we need this extension to be installed to correctly index
//  postgres data...  We should probably do this on connection creation...
//
//     create extension if not exists tsm_system_rows
//

import * as crypto from 'crypto';
import {
  Connection,
  ConnectionConfig,
  MalloyQueryData,
  PersistSQLResults,
  PooledConnection,
  RedshiftDialect,
  QueryData,
  QueryDataRow,
  QueryOptionsReader,
  QueryRunStats,
  RunSQLOptions,
  SQLSourceDef,
  TableSourceDef,
  StreamingConnection,
  StructDef,
  FieldDef,
} from '@malloydata/malloy';
import {BaseConnection} from '@malloydata/malloy/connection';

import {Pool, types} from 'pg';
// Override parser for 64-bit integers (OID 20) and standard integers (OID 23)
types.setTypeParser(20, val => parseInt(val, 10));
types.setTypeParser(23, val => parseInt(val, 10));
import {randomUUID} from 'crypto';
interface RedshiftConnectionConfiguration {
  host?: string;
  port?: number;
  username?: string;
  password?: string;
  databaseName?: string;
  schema?: string;
}

type RedshiftConnectionConfigurationReader = RedshiftConnectionConfiguration;

export interface RedshiftConnectionOptions
  extends ConnectionConfig,
    RedshiftConnectionConfiguration {}

export class RedshiftConnection
  extends BaseConnection
  implements Connection, StreamingConnection, PersistSQLResults
{
  public readonly name: string;
  private config: RedshiftConnectionConfigurationReader = {};
  private readonly dialect = new RedshiftDialect();
  private pool: Pool;
  constructor(
    name: string,
    configReader?: RedshiftConnectionConfigurationReader,
    queryOptionsReader?: QueryOptionsReader
  );
  constructor(
    name: string,
    configReader?: RedshiftConnectionConfigurationReader,
    queryOptionsReader?: QueryOptionsReader
  ) {
    super();
    this.name = name;
    if (configReader) {
      this.config = configReader;
    }

    // Synchronously get config
    let config;
    if (this.config instanceof Function) {
      config = this.config();
    } else {
      config = this.config;
    }

    const {
      username: user,
      password,
      databaseName: database,
      port,
      host,
    } = config;

    this.pool = new Pool({
      user,
      password,
      database,
      port,
      host,
      ssl: {
        rejectUnauthorized: false,
      },
    });
  }

  protected async readConfig(): Promise<RedshiftConnectionConfiguration> {
    if (this.config instanceof Function) {
      return this.config();
    } else {
      return this.config;
    }
  }

  get dialectName(): string {
    return 'redshift';
  }

  public isPool(): this is PooledConnection {
    return false;
  }

  public canPersist(): this is PersistSQLResults {
    return true;
  }

  public canStream(): this is StreamingConnection {
    return true;
  }

  public get supportsNesting(): boolean {
    return true;
  }

  async fetchSelectSchema(
    sqlRef: SQLSourceDef
  ): Promise<SQLSourceDef | string> {
    const structDef: SQLSourceDef = {...sqlRef, fields: []};
    const tempTableName = `tmp${randomUUID()}`.replace(/-/g, '');
    const basicSchemaQuery = `DROP TABLE IF EXISTS ${tempTableName};
      CREATE TEMP TABLE ${tempTableName} AS ${sqlRef.selectStr};
      SELECT "column" as "column_name", type as "data_type", null as "comment"
      FROM pg_table_def
      WHERE tablename = '${tempTableName}';
      `;
    try {
      await this.runQueriesForSchema(
        basicSchemaQuery,
        structDef,
        tempTableName
      );
    } catch (error) {
      const queries = basicSchemaQuery;
      return `Error fetching SELECT schema for \n ${queries}: \n ${error}`;
    }
    return structDef;
  }

  private determineRedshiftSQLType(v: unknown): string {
    if (typeof v === 'number') {
      return 'decimal';
    } else if (typeof v === 'boolean') {
      return 'boolean';
    } else if (typeof v === 'string') {
      const timestamp = Date.parse(v);
      return !isNaN(timestamp) && isNaN(Number(v)) ? 'timestamptz' : 'varchar';
    } else {
      return 'varchar'; // fallback for null or other non-primitive/non-object/non-array
    }
  }

  // Helper function to update the path-to-type map
  private updatePathType(
    path: string,
    value: unknown,
    pathToType: Map<string, string>,
    pathsWithAmbiguousTypes: Set<string>,
    explicitType?: 'object' | 'array'
  ) {
    // If path is already known to be mixed, do nothing
    if (pathsWithAmbiguousTypes.has(path)) {
      return;
    }

    // use explicit type if provided, else figure out the type
    const newType = explicitType
      ? explicitType
      : this.determineRedshiftSQLType(value);

    // prevents a path from being locked into the 'string' type
    // if the first value seen is null
    const isNullFallback =
      !explicitType && newType === 'varchar' && value === null;

    // Path seen before. Check if the type is different.
    if (pathToType.has(path)) {
      const existingType = pathToType.get(path);
      // Conflict if types differ, and the new type isn't just a null fallback
      if (existingType !== newType && !isNullFallback) {
        // Mixed types detected - remove the path and mark as mixed.
        pathToType.delete(path);
        pathsWithAmbiguousTypes.add(path);
      }
    } else {
      // Path not seen before. Set the type, unless it's a null fallback.
      if (!isNullFallback) {
        pathToType.set(path, newType);
      }
    }
  }

  private async runQueriesForSchema(
    basicSchemaQuery: string,
    structDef: StructDef,
    tablePath: string
  ): Promise<void> {
    // Populate basic fields and identify super columns by calling the updated function
    const superCols = await this.runBasicSchemaQuery(
      basicSchemaQuery,
      structDef,
      tablePath
    );

    if (superCols.length > 0) {
      await this.runSuperColsSchemaQuery(superCols, structDef, tablePath);
    }
  }

  private async runBasicSchemaQuery(
    basicSchemaQuery: string,
    structDef: StructDef,
    tablePath: string
  ): Promise<string[]> {
    const {rows, totalRows} = await this.runSQL(basicSchemaQuery);
    if (!totalRows) {
      throw new Error(`Unable to read schema for table ${tablePath}.`);
    }

    const superCols: string[] = [];
    for (const row of rows) {
      const redshiftDataType = row['data_type'] as string;
      const name = row['column_name'] as string;

      if (redshiftDataType === 'super') {
        // we'll process these later
        superCols.push(name);
      } else {
        // add information directly to structDef
        const malloyType = this.dialect.sqlTypeToMalloyType(redshiftDataType);
        structDef.fields.push({...malloyType, name});
      }
    }
    return superCols;
  }

  async fetchTableSchema(
    tableKey: string,
    tablePath: string
  ): Promise<TableSourceDef | string> {
    const structDef: StructDef = {
      type: 'table',
      name: tableKey,
      dialect: 'redshift',
      tablePath,
      connection: this.name,
      fields: [],
    };
    const [schema, table] = tablePath.split('.');
    if (table === undefined) {
      return 'Default schema not yet supported in Redshift';
    }
    const basicSchemaQuery = `SELECT "column_name", "data_type", "remarks" as "comment"
       FROM svv_columns
       WHERE table_schema = '${schema}'
       AND table_name = '${table}';`;

    try {
      await this.runQueriesForSchema(basicSchemaQuery, structDef, tablePath);
    } catch (error) {
      return `Error fetching TABLE schema for ${tablePath}: ${error.message}`;
    }
    return structDef;
  }

  public async test(): Promise<void> {
    await this.runSQL('SELECT 1');
  }

  protected async runRedshiftQuery(
    sql: string | string[],
    _pageSize: number,
    _rowIndex: number,
    deJSON: boolean
  ): Promise<MalloyQueryData> {
    const sqlArray = this.config.schema
      ? [`SET search_path TO ${this.config.schema};`]
      : [];
    // redshift turns camelcase into all lowercase
    // this doesn't actually matter for prod,
    // but index and temp table tests do expect the original case
    sqlArray.push('SET enable_case_sensitive_identifier TO true;');
    if (Array.isArray(sql)) {
      sqlArray.push(...sql);
    } else {
      sqlArray.push(sql);
    }

    let client;
    try {
      // get client from pool
      client = await this.pool.connect();

      let result;
      for (const sqlStatement of sqlArray) {
        result = await client.query(sqlStatement);
      }
      if (Array.isArray(result)) {
        result = result.pop();
      }
      if (result?.rows) {
        result.rows = result.rows.map(row => {
          const newRow = {...row};
          Object.keys(newRow).forEach((key, index) => {
            if (key === '?column?') {
              newRow[index + 1] = newRow[key];
              delete newRow[key];
            }
          });
          return newRow;
        });
      }

      return {
        rows: result.rows as QueryData,
        totalRows: result.rows.length,
      };
    } catch (error) {
      throw new Error(`Error executing query: ${error.message}`);
    } finally {
      if (client) client.release();
    }
  }

  public async runSQL(
    sql: string | string[],
    {rowLimit}: RunSQLOptions = {},
    _rowIndex = 0
  ): Promise<MalloyQueryData> {
    const result = await this.runRedshiftQuery(sql, 100000, 0, true);
    return result;
  }

  public async *runSQLStream(
    sqlCommand: string,
    {rowLimit, abortSignal}: RunSQLOptions = {}
  ): AsyncIterableIterator<QueryDataRow> {
    const result = await this.runSQL(sqlCommand, {rowLimit});
    for (const row of result.rows) {
      if (abortSignal?.aborted) break;
      yield row;
    }
  }

  public async estimateQueryCost(_: string): Promise<QueryRunStats> {
    return {};
  }

  public async manifestTemporaryTable(sqlCommand: string): Promise<string> {
    const hash = crypto.createHash('md5').update(sqlCommand).digest('hex');
    const tableName = `tt${hash}`;

    const cmd = [
      `DROP TABLE IF EXISTS ${tableName};`,
      `CREATE TEMP TABLE ${tableName} AS ${sqlCommand};`,
    ];
    await this.runSQL(cmd);
    return tableName;
  }

  async close(): Promise<void> {
    if (this.pool) {
      await this.pool.end();
    }
    return;
  }

  // traverse the current superJson and fill out pathToType map
  private processNestedJson(
    superJson: Record<string, unknown>,
    columnName: string,
    pathToType: Map<string, string>,
    pathsWithAmbiguousTypes: Set<string>
  ): void {
    for (const [key, value] of Object.entries(superJson)) {
      const path = columnName ? `${columnName}.${key}` : key;

      // Check if path is already marked as mixed type before proceeding deeper
      if (pathsWithAmbiguousTypes.has(path)) {
        continue;
      }

      if (Array.isArray(value)) {
        // Track current path as an array
        this.updatePathType(
          path,
          value,
          pathToType,
          pathsWithAmbiguousTypes,
          'array'
        );

        // If the array path itself is ambiguous type
        if (pathsWithAmbiguousTypes.has(path)) {
          continue;
        }

        // process each element of the array
        const arrayElementPath = `${path}[*]`;
        for (const element of value) {
          if (pathsWithAmbiguousTypes.has(arrayElementPath)) {
            break;
          }

          if (
            element !== null &&
            typeof element === 'object' &&
            !Array.isArray(element)
          ) {
            // Element is an object: Recurse using element path.
            // Call recursively using 'this' as it's now a class method
            this.processNestedJson(
              element as Record<string, unknown>,
              arrayElementPath,
              pathToType,
              pathsWithAmbiguousTypes
            );
          } else {
            // Element is primitive or null
            this.updatePathType(
              arrayElementPath,
              element,
              pathToType,
              pathsWithAmbiguousTypes
            );
          }
        }
      } else if (value !== null && typeof value === 'object') {
        // track current path as an object
        this.updatePathType(
          path,
          value,
          pathToType,
          pathsWithAmbiguousTypes,
          'object'
        );

        if (pathsWithAmbiguousTypes.has(path)) {
          continue;
        }

        // 2. Recurse into object
        // Call recursively using 'this'
        this.processNestedJson(
          value as Record<string, unknown>,
          path, // Pass object path (e.g., 'user')
          pathToType,
          pathsWithAmbiguousTypes
        );
      } else {
        // Leaf node
        this.updatePathType(path, value, pathToType, pathsWithAmbiguousTypes);
      }
    }
  }

  // Recursively add fields to the structDef
  private addFieldToStructDef(
    parentFields: FieldDef[],
    path: string[],
    fieldType: string
  ): void {
    if (path.length === 0) return;

    const currentPathSegment = path[0];
    const remainingPath = path.slice(1);

    let fieldName = currentPathSegment;
    // case we're processing the children of an array
    if (currentPathSegment.endsWith('[*]')) {
      fieldName = currentPathSegment.slice(0, -3);
    }

    // Determine if current FieldDef[] contains a definition for the current path
    let field = parentFields.find(f => f.name === fieldName);

    // Create new FieldDef if it doesn't exist
    if (!field) {
      if (fieldType === 'array') {
        field = {
          type: 'array',
          join: 'many',
          name: fieldName,
          // technically this can also be atomic type,
          // but bracket notation should work the same in redshift
          elementTypeDef: {type: 'record_element'},
          fields: [],
        };
        // leaf node - use the actual type
      } else if (path.length === 1) {
        field = {
          ...this.dialect.sqlTypeToMalloyType(fieldType),
          name: fieldName,
        };
      }
      // object type
      else {
        field = {
          type: 'record',
          name: fieldName,
          fields: [],
          join: 'one',
        };
      }
      parentFields.push(field);
    }
    // recursively update the remaining path segments into the structDef
    if (path.length > 1 && 'fields' in field) {
      this.addFieldToStructDef(field.fields, remainingPath, fieldType);
    }
  }

  private createPathToTypeMap(
    rows: Record<string, unknown>[],
    superCols: string[]
  ): Map<string, string> {
    const pathToType = new Map<string, string>();
    const pathsWithAmbiguousTypes = new Set<string>();

    // process all super columns and update pathToType map
    for (const row of rows) {
      for (const [key, value] of Object.entries(row)) {
        if (superCols.includes(key) && typeof value === 'string') {
          try {
            const jsonValue = JSON.parse(value);
            if (
              jsonValue !== null &&
              typeof jsonValue === 'object' &&
              !Array.isArray(jsonValue)
            ) {
              this.processNestedJson(
                jsonValue as Record<string, unknown>,
                key,
                pathToType,
                pathsWithAmbiguousTypes
              );
            }
          } catch (e) {
            continue; // Ignore JSON parsing errors
          }
        }
      }
    }

    return pathToType;
  }

  private async runSuperColsSchemaQuery(
    superCols: string[],
    structDef: StructDef,
    tablePath: string
  ): Promise<void> {
    // query to sample the jsons in the super columns
    const sampleQuery = `
        SELECT ${superCols
          .map(s => `JSON_SERIALIZE(${s}) as ${s}`)
          .join(',')} FROM ${tablePath} LIMIT 100;
      `;

    const {rows, totalRows} = await this.runSQL(sampleQuery); // Apply formatting
    const pathToType = this.createPathToTypeMap(rows, superCols);

    /*
      example resulting pathToType map:
      Map(6) {
        'reviews_json.timestamp' => 'decimal',
        'reviews_json.reviews' => 'array',
        'reviews_json.reviews[*].question_id' => 'decimal',
        'reviews_json.reviews[*].label' => 'varchar',
        'reviews_json.reviews[*].other_option' => 'varchar',
        'reviews_json.foo.bar' => 'varchar'
      }
    */

    // Add each path to type pair into the structDef
    for (const [path, type] of pathToType.entries()) {
      const pathSegments = path.split('.');
      this.addFieldToStructDef(structDef.fields, pathSegments, type);
    }
  }
}
