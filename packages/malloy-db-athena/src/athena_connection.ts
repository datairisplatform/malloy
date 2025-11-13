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

import {
  AthenaClient,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
  StartQueryExecutionCommand,
  ColumnInfo,
} from '@aws-sdk/client-athena';
import {
  ConnectionConfig,
  MalloyQueryData,
  QueryDataRow,
  RunSQLOptions,
  StructDef,
  TableSourceDef,
  SQLSourceDef,
  AtomicTypeDef,
  mkFieldDef,
  TestableConnection,
  SQLSourceRequest,
  sqlKey,
} from '@malloydata/malloy';

import {BaseConnection} from '@malloydata/malloy/connection';

export interface AthenaConnectionConfiguration {
  region: string;
  accessKeyId: string;
  secretAccessKey: string;
  database?: string;
  outputLocation: string;
  workGroup?: string;
}

export type AthenaConnectionOptions = ConnectionConfig &
  AthenaConnectionConfiguration;

const athenaToMalloyTypes: {[key: string]: AtomicTypeDef} = {
  'varchar': {type: 'string'},
  'char': {type: 'string'},
  'string': {type: 'string'},
  'integer': {type: 'number', numberType: 'integer'},
  'bigint': {type: 'number', numberType: 'integer'},
  'smallint': {type: 'number', numberType: 'integer'},
  'tinyint': {type: 'number', numberType: 'integer'},
  'double': {type: 'number', numberType: 'float'},
  'float': {type: 'number', numberType: 'float'},
  'decimal': {type: 'number', numberType: 'float'},
  'date': {type: 'date'},
  'timestamp': {type: 'timestamp'},
  'boolean': {type: 'boolean'},
  'bool': {type: 'boolean'},
};

export class AthenaConnection
  extends BaseConnection
  implements TestableConnection
{
  private readonly client: AthenaClient;
  private readonly config: AthenaConnectionConfiguration;
  private readonly connectionName: string;

  constructor(options: AthenaConnectionOptions) {
    super();
    this.connectionName = options.name;
    this.config = {
      region: options.region,
      accessKeyId: options.accessKeyId,
      secretAccessKey: options.secretAccessKey,
      database: options.database,
      outputLocation: options.outputLocation,
      workGroup: options.workGroup,
    };

    this.client = new AthenaClient({
      region: this.config.region,
      credentials: {
        accessKeyId: this.config.accessKeyId,
        secretAccessKey: this.config.secretAccessKey,
      },
    });
  }

  get name(): string {
    return this.connectionName;
  }

  get dialectName(): string {
    return 'presto';
  }

  // main function to run SQL queries
  public async runSQL(
    sqlCommand: string,
    options: RunSQLOptions = {}
  ): Promise<MalloyQueryData> {
    // Wrap query with LIMIT if rowLimit is specified
    let sql = sqlCommand;
    if (options.rowLimit) {
      sql = `SELECT * FROM (${sqlCommand}) LIMIT ${options.rowLimit}`;
    }

    const queryExecutionId = await this.executeAthenaQuery(sql);
    const {rows, columns} = await this.fetchQueryResults(queryExecutionId);

    const malloyRows = this.parseAthenaResults(rows, columns);

    return {
      rows: malloyRows,
      totalRows: malloyRows.length,
    };
  }

  // main function to fetch table schemas
  async fetchTableSchema(
    tableKey: string,
    tablePath: string
  ): Promise<TableSourceDef> {
    const structDef: TableSourceDef = {
      type: 'table',
      name: tableKey,
      dialect: this.dialectName,
      tablePath,
      connection: this.name,
      fields: [],
    };
    const rows = await this.runFetchSchemaSQL(tablePath);
    this.structDefFromSchema(rows, structDef);
    return structDef;
  }

  private async waitForQuery(queryExecutionId: string): Promise<void> {
    while (true) {
      const {QueryExecution} = await this.client.send(
        new GetQueryExecutionCommand({QueryExecutionId: queryExecutionId})
      );
      const state = QueryExecution?.Status?.State;

      if (state === 'SUCCEEDED') return;
      if (state === 'FAILED' || state === 'CANCELLED') {
        throw new Error(
          `Query ${state}: ${QueryExecution?.Status?.StateChangeReason}`
        );
      }

      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }

  private async executeAthenaQuery(sql: string): Promise<string> {
    const params: any = {
      QueryString: sql,
      ResultConfiguration: {
        OutputLocation: this.config.outputLocation,
      },
    };

    if (this.config.database) {
      params.QueryExecutionContext = {Database: this.config.database};
    }

    if (this.config.workGroup) {
      params.WorkGroup = this.config.workGroup;
    }

    const {QueryExecutionId} = await this.client.send(
      new StartQueryExecutionCommand(params)
    );

    if (!QueryExecutionId) {
      throw new Error('No query execution ID returned from Athena');
    }

    await this.waitForQuery(QueryExecutionId);
    return QueryExecutionId;
  }

  private async fetchQueryResults(
    queryExecutionId: string
  ): Promise<{rows: unknown[][]; columns: ColumnInfo[]}> {
    const allRows: unknown[][] = [];
    const columns: ColumnInfo[] = [];
    let nextToken: string | undefined = undefined;

    while (true) {
      const {ResultSet, NextToken} = await this.client.send(
        new GetQueryResultsCommand({
          QueryExecutionId: queryExecutionId,
          NextToken: nextToken,
        })
      );

      const rows = ResultSet?.Rows ?? [];

      // First iteration: extract column info from metadata
      if (allRows.length === 0 && ResultSet?.ResultSetMetadata?.ColumnInfo) {
        columns.push(...ResultSet.ResultSetMetadata.ColumnInfo);
      }

      // Convert rows to array format
      for (const row of rows) {
        const values = row.Data?.map(col => col.VarCharValue ?? null) ?? [];
        allRows.push(values);
      }

      nextToken = NextToken;
      if (!nextToken) break;
    }

    // skip the first row (header row)
    const dataRows = allRows.slice(1);

    return {rows: dataRows, columns};
  }

  private convertAthenaValue(
    value: string | null,
    athenaType: string
  ): string | number | boolean | Date | null {
    if (value === null || value === undefined || value === '') {
      return null;
    }

    const baseType =
      athenaType.toLowerCase().match(/^(\w+)/)?.[0] ?? athenaType;

    switch (baseType) {
      case 'bigint':
      case 'integer':
      case 'smallint':
      case 'tinyint':
        return parseInt(value, 10);
      case 'double':
      case 'float':
      case 'decimal':
        return parseFloat(value);
      case 'boolean':
      case 'bool':
        return value.toLowerCase() === 'true';
      case 'date':
      case 'timestamp':
        return new Date(value);
      default:
        return value;
    }
  }

  private parseAthenaResults(
    rows: unknown[][],
    columns: ColumnInfo[]
  ): QueryDataRow[] {
    const malloyRows: QueryDataRow[] = [];

    for (const row of rows) {
      const malloyRow: QueryDataRow = {};
      for (let i = 0; i < columns.length; i++) {
        const column = columns[i];
        const columnName = column.Name ?? `column_${i}`;
        const columnType = column.Type ?? 'varchar';
        malloyRow[columnName] = this.convertAthenaValue(
          row[i] as string | null,
          columnType
        );
      }
      malloyRows.push(malloyRow);
    }

    return malloyRows;
  }

  private athenaTypeToMalloyType(athenaType: string | null): AtomicTypeDef {
    if (!athenaType) {
      return {type: 'sql native', rawType: 'unknown'};
    }
    const baseSqlType = athenaType.match(/^(\w+)/)?.[0] ?? athenaType;
    const lowerType = baseSqlType.toLowerCase();
    return (
      athenaToMalloyTypes[lowerType] ?? {
        type: 'sql native',
        rawType: athenaType,
      }
    );
  }

  // run SQL for fetching table schema (uses information_schema)
  private async runFetchSchemaSQL(
    tablePath: string
  ): Promise<Array<{name: string; type: string; comment: string | null}>> {
    // Parse tablePath to extract schema and table names
    // Handles formats like "schema.table" or "catalog.schema.table"
    const parts = tablePath.split('.');
    const tableName = parts[parts.length - 1];
    const schemaName = parts[parts.length - 2];

    // Query information_schema for column information
    const infoQuery = `
      SELECT column_name, data_type, '' as comment
      FROM information_schema.columns
      WHERE table_schema = '${schemaName}'
        AND table_name = '${tableName}'
      ORDER BY ordinal_position
    `;

    const queryExecutionId = await this.executeAthenaQuery(infoQuery);
    const {rows} = await this.fetchQueryResults(queryExecutionId);

    return rows.map(row => ({
      name: row[0] as string,
      type: row[1] as string,
      comment: row[2] as string | null,
    }));
  }

  private structDefFromSchema(
    rows: Array<{name: string; type: string; comment: string | null}>,
    structDef: StructDef
  ): void {
    for (const row of rows) {
      const malloyType = this.athenaTypeToMalloyType(row.type);
      structDef.fields.push(mkFieldDef(malloyType, row.name));
    }
  }

  async fetchSelectSchema(sqlRef: SQLSourceRequest): Promise<SQLSourceDef> {
    const structDef: SQLSourceDef = {
      type: 'sql_select',
      ...sqlRef,
      dialect: this.dialectName,
      fields: [],
      name: sqlKey(sqlRef.connection, sqlRef.selectStr),
    };

    // Use EXPLAIN to get the schema
    const explainResult = await this.runSQL(`EXPLAIN ${sqlRef.selectStr}`, {});

    if (explainResult.rows.length === 0) {
      throw new Error(
        'Received empty explain result when trying to fetch schema.'
      );
    }

    // Parse the query plan from EXPLAIN output
    // The first row should contain the query plan
    const firstRow = explainResult.rows[0];
    const planKey = Object.keys(firstRow)[0];
    const expResult = firstRow[planKey] as string;

    if (!expResult) {
      throw new Error('Explain result has rows but query plan is not present.');
    }

    const lines = expResult.split('\n');
    if (lines?.length === 0) {
      throw new Error(
        'Received invalid explain result when trying to fetch schema.'
      );
    }

    // Get column info from the query execution
    // Since we can't access the raw column metadata here, we'll fetch it differently
    // by actually running the LIMIT 0 query and inspecting the structure

    // Alternative: Run a modified query to get schema
    const schemaQuery = `SELECT * FROM (${sqlRef.selectStr}) LIMIT 1`;
    const schemaResult = await this.runSQL(schemaQuery, {rowLimit: 1});

    // Extract field definitions from the result
    if (schemaResult.rows.length > 0) {
      const firstDataRow = schemaResult.rows[0];
      for (const [fieldName, value] of Object.entries(firstDataRow)) {
        let fieldType: AtomicTypeDef;
        if (typeof value === 'number') {
          fieldType = Number.isInteger(value)
            ? {type: 'number', numberType: 'integer'}
            : {type: 'number', numberType: 'float'};
        } else if (typeof value === 'boolean') {
          fieldType = {type: 'boolean'};
        } else if (value instanceof Date) {
          fieldType = {type: 'timestamp'};
        } else {
          fieldType = {type: 'string'};
        }
        structDef.fields.push(mkFieldDef(fieldType, fieldName));
      }
    }

    return structDef;
  }

  async test(): Promise<void> {
    await this.runSQL('SELECT 1', {});
  }
}
