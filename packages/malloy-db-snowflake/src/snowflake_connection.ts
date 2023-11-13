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

import * as crypto from 'crypto';
import {
  RunSQLOptions,
  MalloyQueryData,
  QueryRunStats,
  Connection,
  PersistSQLResults,
  StreamingConnection,
  PooledConnection,
  SQLBlock,
  StructDef,
  QueryDataRow,
  SnowflakeDialect,
  NamedStructDefs,
} from '@malloydata/malloy';
import {SnowflakeExecutor, SnowflakeQueryOptions} from './snowflake_executor';
import {FetchSchemaOptions} from '@malloydata/malloy/dist/runtime_types';

export class SnowflakeConnection
  implements Connection, PersistSQLResults, StreamingConnection
{
  static DEFAULT_QUERY_OPTIONS: SnowflakeQueryOptions = {
    rowLimit: 10,
  };

  private readonly dialect = new SnowflakeDialect();
  private executor: SnowflakeExecutor;
  private schemaCache = new Map<
    string,
    | {schema: StructDef; error?: undefined; timestamp: number}
    | {error: string; schema?: undefined; timestamp: number}
  >();
  private sqlSchemaCache = new Map<
    string,
    | {
        structDef: StructDef;
        error?: undefined;
        timestamp: number;
      }
    | {error: string; structDef?: undefined; timestamp: number}
  >();

  constructor(public readonly name: string) {
    this.executor = new SnowflakeExecutor();
    // set some default session parameters
    // this is quite imporant for snowflake because malloy tends to add quotes to all database identifiers
    // and snowflake is case sensitive by with quotes but matches against all caps identifiers without quotes
    // NOTE: before pooling can be enabled we should have a way to set this on all executors
    this.executor.batch(
      'ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = true;'
    );
  }

  get dialectName(): string {
    return 'snowflake';
  }

  // TODO: make it support nesting soon
  public get supportsNesting(): boolean {
    return false;
  }

  public isPool(): this is PooledConnection {
    return true;
  }

  public canPersist(): this is PersistSQLResults {
    return true;
  }

  public canStream(): this is StreamingConnection {
    return true;
  }

  public async estimateQueryCost(_sqlCommand: string): Promise<QueryRunStats> {
    return {};
  }

  async close(): Promise<void> {
    await this.executor.done();
  }

  private convert_column_to_lowercase(row: QueryDataRow): QueryDataRow {
    const result: QueryDataRow = {};
    for (const key in row) {
      result[key.toLowerCase()] = row[key];
    }
    return result;
  }

  public async runSQL(
    sql: string,
    _options?: RunSQLOptions
  ): Promise<MalloyQueryData> {
    let rows = await this.executor.batch(sql);
    rows = rows.map(row => this.convert_column_to_lowercase(row));
    return {rows, totalRows: rows.length};
  }

  public async *runSQLStream(
    sqlCommand: string,
    options?: SnowflakeQueryOptions
  ): AsyncIterableIterator<QueryDataRow> {
    for await (const row of await this.executor.stream(sqlCommand, options)) {
      yield this.convert_column_to_lowercase(row);
    }
  }

  private async schemaFromQuery(
    infoQuery: string,
    structDef: StructDef
  ): Promise<void> {
    const rows = await this.executor.batch(infoQuery);
    for (const row of rows) {
      const snowflakeDataType = row['DATA_TYPE'] as string;
      const s = structDef;
      const malloyType = this.dialect.sqlTypeToMalloyType(snowflakeDataType);
      // all column names are in uppercase, convert to lower case for consistency
      const name = (row['COLUMN_NAME'] as string).toLowerCase();
      name.toLowerCase();
      if (malloyType) {
        s.fields.push({...malloyType, name});
      } else {
        s.fields.push({
          type: 'unsupported',
          rawType: snowflakeDataType.toLowerCase(),
          name,
        });
      }
    }
  }

  private async getTableSchema(
    tableKey: string,
    tablePath: string
  ): Promise<StructDef> {
    // looks like snowflake:schemaName.tableName
    tableKey = tableKey.toLowerCase();

    let [schemaPrefix, tableName] = ['', tablePath];
    const schema_and_table = tablePath.split('.');
    if (schema_and_table.length === 2) {
      [schemaPrefix, tableName] = schema_and_table;
      schemaPrefix = schemaPrefix + '.';
    }

    const structDef: StructDef = {
      type: 'struct',
      dialect: 'snowflake',
      name: tableKey,
      structSource: {type: 'table', tablePath},
      structRelationship: {
        type: 'basetable',
        connectionName: this.name,
      },
      fields: [],
    };
    // FIXME: only variant is probably shown, cannot infer element types, so how do we deal with variants?

    const infoQuery = `
    SELECT COLUMN_NAME, DATA_TYPE FROM ${schemaPrefix}INFORMATION_SCHEMA.COLUMNS where table_name = UPPER('${tableName}');
    `;

    await this.schemaFromQuery(infoQuery, structDef);
    return structDef;
  }

  public async fetchSchemaForTables(
    missing: Record<string, string>,
    {refreshTimestamp}: FetchSchemaOptions
  ): Promise<{
    schemas: Record<string, StructDef>;
    errors: Record<string, string>;
  }> {
    const schemas: NamedStructDefs = {};
    const errors: {[name: string]: string} = {};

    for (const tableKey in missing) {
      let inCache = this.schemaCache.get(tableKey);
      if (
        !inCache ||
        (refreshTimestamp && refreshTimestamp > inCache.timestamp)
      ) {
        const tablePath = missing[tableKey];
        const timestamp = refreshTimestamp || Date.now();
        try {
          inCache = {
            schema: await this.getTableSchema(tableKey, tablePath),
            timestamp,
          };
          this.schemaCache.set(tableKey, inCache);
        } catch (error) {
          inCache = {error: error.message, timestamp};
        }
      }
      if (inCache.schema !== undefined) {
        schemas[tableKey] = inCache.schema;
      } else {
        errors[tableKey] = inCache.error || 'Unknown schema fetch error';
      }
    }
    return {schemas, errors};
  }

  private async getSQLBlockSchema(sqlRef: SQLBlock): Promise<StructDef> {
    const structDef: StructDef = {
      type: 'struct',
      dialect: 'snowflake',
      name: sqlRef.name,
      structSource: {
        type: 'sql',
        method: 'subquery',
        sqlBlock: sqlRef,
      },
      structRelationship: {
        type: 'basetable',
        connectionName: this.name,
      },
      fields: [],
    };

    const hash = crypto
      .createHash('md5')
      .update(sqlRef.selectStr)
      .digest('hex');
    const tempTableName = `tt_${hash}`;

    // create temp table with same schema as the query
    this.runSQL(
      `
      CREATE OR REPLACE TEMP TABLE ${tempTableName} as SELECT * FROM (
        ${sqlRef.selectStr}
      ) as x WHERE false;
      `
    );

    const infoQuery = `SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where table_name = UPPER('${tempTableName}');`;
    await this.schemaFromQuery(infoQuery, structDef);
    return structDef;
  }

  public async fetchSchemaForSQLBlock(
    sqlRef: SQLBlock,
    {refreshTimestamp}: FetchSchemaOptions
  ): Promise<
    | {structDef: StructDef; error?: undefined}
    | {error: string; structDef?: undefined}
  > {
    const key = sqlRef.name;
    let inCache = this.sqlSchemaCache.get(key);
    if (
      !inCache ||
      (refreshTimestamp && refreshTimestamp > inCache.timestamp)
    ) {
      const timestamp = refreshTimestamp ?? Date.now();
      try {
        inCache = {
          structDef: await this.getSQLBlockSchema(sqlRef),
          timestamp,
        };
      } catch (error) {
        inCache = {error: error.message, timestamp};
      }
      this.sqlSchemaCache.set(key, inCache);
    }
    return inCache;
  }

  public async manifestTemporaryTable(sqlCommand: string): Promise<string> {
    const hash = crypto.createHash('md5').update(sqlCommand).digest('hex');
    const tableName = `tt${hash}`;

    const cmd = `CREATE TEMP TABLE IF NOT EXISTS ${tableName} AS (${sqlCommand});`;
    await this.runSQL(cmd);
    return tableName;
  }
}
