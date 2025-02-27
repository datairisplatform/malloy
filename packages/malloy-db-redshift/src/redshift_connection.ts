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
  PostgresDialect,
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
  mkArrayDef,
} from '@malloydata/malloy';
import {BaseConnection} from '@malloydata/malloy/connection';

import {Client, Pool} from 'pg';
import QueryStream from 'pg-query-stream';
import {randomUUID} from 'crypto';
import AWS from 'aws-sdk';
import {
  RedshiftDataClient,
  ExecuteStatementCommand,
  DescribeStatementCommand,
  GetStatementResultCommand,
  BatchExecuteStatementCommand,
} from '@aws-sdk/client-redshift-data';

interface RedshiftConnectionConfiguration {
  host?: string;
  port?: number;
  username?: string;
  password?: string;
  databaseName?: string;
  connectionString?: string;
}

type RedshiftConnectionConfigurationReader =
  | RedshiftConnectionConfiguration
  | (() => Promise<RedshiftConnectionConfiguration>);

const DEFAULT_PAGE_SIZE = 1000;
const SCHEMA_PAGE_SIZE = 1000;

export interface RedshiftConnectionOptions
  extends ConnectionConfig,
    RedshiftConnectionConfiguration {}

export class RedshiftConnection
  extends BaseConnection
  implements Connection, StreamingConnection, PersistSQLResults
{
  public readonly name: string;
  private queryOptionsReader: QueryOptionsReader = {};
  private configReader: RedshiftConnectionConfigurationReader = {};

  private readonly dialect = new RedshiftDialect();

  constructor(
    options: RedshiftConnectionOptions,
    queryOptionsReader?: QueryOptionsReader
  );
  constructor(
    name: string,
    queryOptionsReader?: QueryOptionsReader,
    configReader?: RedshiftConnectionConfigurationReader
  );
  constructor(
    arg: string | RedshiftConnectionOptions,
    queryOptionsReader?: QueryOptionsReader,
    configReader?: RedshiftConnectionConfigurationReader
  ) {
    super();
    if (typeof arg === 'string') {
      this.name = arg;
      if (configReader) {
        this.configReader = configReader;
      }
    } else {
      const {name, ...configReader} = arg;
      this.name = name;
      this.configReader = configReader;
    }
    if (queryOptionsReader) {
      this.queryOptionsReader = queryOptionsReader;
    }
  }

  private async readQueryConfig(): Promise<RunSQLOptions> {
    if (this.queryOptionsReader instanceof Function) {
      return this.queryOptionsReader();
    } else {
      return this.queryOptionsReader;
    }
  }

  protected async readConfig(): Promise<RedshiftConnectionConfiguration> {
    if (this.configReader instanceof Function) {
      return this.configReader();
    } else {
      return this.configReader;
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

  protected async getClient(): Promise<Client> {
    const {
      username: user,
      password,
      databaseName: database,
      port,
      host,
      connectionString,
    } = await this.readConfig();
    return new Client({
      user,
      password,
      database,
      port,
      host,
      connectionString,
    });
  }

  protected async runPostgresQuery(
    sqlCommand: string,
    _pageSize: number,
    _rowIndex: number,
    deJSON: boolean
  ): Promise<MalloyQueryData> {
    const client = await this.getClient();
    await client.connect();
    await this.connectionSetup(client);

    let result = await client.query(sqlCommand);
    if (Array.isArray(result)) {
      result = result.pop();
    }
    if (deJSON) {
      for (let i = 0; i < result.rows.length; i++) {
        result.rows[i] = result.rows[i].row;
      }
    }
    await client.end();
    return {
      rows: result.rows as QueryData,
      totalRows: result.rows.length,
    };
  }

  async fetchSelectSchema(
    sqlRef: SQLSourceDef
  ): Promise<SQLSourceDef | string> {
    console.log('BRIAN fetching SELECT schema');
    const structDef: SQLSourceDef = {...sqlRef, fields: []};
    const tempTableName = `tmp${randomUUID()}`.replace(/-/g, '');
    const infoQuery = [
      `DROP TABLE IF EXISTS ${tempTableName};`,
      `CREATE TEMP TABLE ${tempTableName} AS ${sqlRef.selectStr};`,
      `SELECT type as "data_type", "column" as "col_name"
      FROM pg_table_def
      WHERE tablename = '${tempTableName}';
      `,
    ];
    //   drop table if exists ${tempTableName};
    //   create temp table ${tempTableName} as SELECT * FROM (
    //     ${sqlRef.selectStr}
    //   ) as x where false;
    //   SELECT column_name, c.data_type, e.data_type as element_type
    //   FROM information_schema.columns c LEFT JOIN information_schema.element_types e
    //     ON ((c.table_catalog, c.table_schema, c.table_name, 'TABLE', c.dtd_identifier)
    //       = (e.object_catalog, e.object_schema, e.object_name, e.object_type, e.collection_type_identifier))
    //   where table_name='${tempTableName}';
    // `;
    try {
      await this.schemaFromQuery(infoQuery, structDef);
    } catch (error) {
      const queries = infoQuery.join('\n');
      return `Error fetching SELECT schema for \n ${queries}: \n ${error}`;
    }
    return structDef;
  }

  private async schemaFromQuery(
    infoQuery: string | string[],
    structDef: StructDef
  ): Promise<void> {
    const {rows, totalRows} = await this.runSQL(infoQuery);
    console.log('BRIAN schema rows:', rows);
    if (!totalRows) {
      throw new Error('Unable to read schema.');
    }
    for (const row of rows) {
      const postgresDataType = row['data_type'] as string;
      const name = row['column_name'] as string;
      if (postgresDataType === 'ARRAY') {
        const elementType = this.dialect.sqlTypeToMalloyType(
          row['element_type'] as string
        );
        structDef.fields.push(mkArrayDef(elementType, name));
      } else {
        const malloyType = this.dialect.sqlTypeToMalloyType(postgresDataType);
        structDef.fields.push({...malloyType, name});
      }
    }
  }

  async fetchTableSchema(
    tableKey: string,
    tablePath: string
  ): Promise<TableSourceDef | string> {
    console.log('BRIAN fetching TABLE schema');
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
      return 'Default schema not yet supported in Postgres';
    }
    const infoQuery = `SELECT type as "data_type", "column" as "col_name"
      FROM pg_table_def
      WHERE tablename = '${table}';`;

    try {
      await this.schemaFromQuery(infoQuery, structDef);
    } catch (error) {
      return `Error fetching TABLE schema for ${tablePath}: ${error.message}`;
    }
    return structDef;
  }

  public async test(): Promise<void> {
    await this.runSQL('SELECT 1');
  }

  public async connectionSetup(client: Client): Promise<void> {
    await client.query("SET TIME ZONE 'UTC'");
  }

  public async runSQL(
    sql: string | string[],
    {rowLimit}: RunSQLOptions = {},
    _rowIndex = 0
  ): Promise<MalloyQueryData> {
    // add statement in beginning of query to set the default schema/db
    const sqlArray = ['SET search_path TO malloytest;'];
    if (Array.isArray(sql)) {
      sqlArray.push(...sql);
    } else {
      sqlArray.push(sql);
    }
    // Initiate RedshiftData client
    const client = new RedshiftDataClient({region: 'us-west-1'});

    try {
      // Execute all SQL statements in batch
      // so they share the same context
      // if we don't do this, the "SET search_path" won't affect the following queries
      const batchExecuteCommand = new BatchExecuteStatementCommand({
        WorkgroupName: 'default-workgroup', //todo make configurable
        Database: 'dev', // todo make configurable
        SecretArn:
          'arn:aws:secretsmanager:us-west-1:977099028464:secret:redshift-access-secret-4MyGTg',
        Sqls: sqlArray,
      });

      const batchResponse = await client.send(batchExecuteCommand);
      const batchId = batchResponse.Id;

      // Wait for all queries to complete
      let status: string | undefined;
      let result;
      let statusResponse;
      do {
        // Pause for 1 second between status checks
        await new Promise(resolve => setTimeout(resolve, 1000));

        const describeCommand = new DescribeStatementCommand({
          Id: batchId,
        });
        statusResponse = await client.send(describeCommand);

        status = statusResponse.Status;
      } while (
        status !== 'FINISHED' &&
        status !== 'FAILED' &&
        status !== 'ABORTED'
      );

      // If the batch finished successfully, fetch the results of the last statement
      if (status === 'FINISHED') {
        // console.log('BRIAN final status check:', statusResponse);
        // Get the last statement's ID from the batch
        const lastStatementId =
          statusResponse.SubStatements?.[
            statusResponse.SubStatements.length - 1
          ].Id;

        if (lastStatementId) {
          const resultCommand = new GetStatementResultCommand({
            Id: lastStatementId,
          });
          result = await client.send(resultCommand);
        }
      } else {
        throw new Error(
          `Batch query did not finish successfully. Status: ${status}`
        );
      }

      return {
        rows:
          result?.Records?.map(record => {
            const row: QueryDataRow = {};
            record.forEach((field, index) => {
              // Extract the first non-null value (longValue, stringValue etc)
              const value =
                field.longValue ??
                field.stringValue ??
                field.doubleValue ??
                null;
              let key = result?.ColumnMetadata?.[index]?.name ?? '';
              if (key === '?column?') {
                // this can happen on SELECT 1 when there's no obvious column
                key = (index + 1).toString();
              }
              row[key] = value;
            });
            return row;
          }) ?? [],
        totalRows: result?.TotalNumRows ?? 0,
      };
    } catch (error) {
      throw new Error(`Error executing sql: ${sqlArray} batch query: ${error}`);
    }

    return {
      rows: [{'-1': -1}],
      totalRows: 0,
    };
  }

  public async *runSQLStream(
    sqlCommand: string,
    {rowLimit, abortSignal}: RunSQLOptions = {}
  ): AsyncIterableIterator<QueryDataRow> {
    const query = new QueryStream(sqlCommand);
    const client = await this.getClient();
    await client.connect();
    const rowStream = client.query(query);
    let index = 0;
    for await (const row of rowStream) {
      yield row.row as QueryDataRow;
      index += 1;
      if (
        (rowLimit !== undefined && index >= rowLimit) ||
        abortSignal?.aborted
      ) {
        query.destroy();
        break;
      }
    }
    await client.end();
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
    // const cmd = `CREATE TEMPORARY TABLE IF NOT EXISTS ${tableName} AS (${sqlCommand});`;
    console.log(cmd);
    await this.runSQL(cmd);
    return tableName;
  }

  async close(): Promise<void> {
    return;
  }
}

export class PooledPostgresConnection
  extends RedshiftConnection
  implements PooledConnection
{
  private _pool: Pool | undefined;

  constructor(
    options: RedshiftConnectionOptions,
    queryOptionsReader?: QueryOptionsReader
  );
  constructor(
    name: string,
    queryOptionsReader?: QueryOptionsReader,
    configReader?: RedshiftConnectionConfigurationReader
  );
  constructor(
    arg: string | RedshiftConnectionOptions,
    queryOptionsReader?: QueryOptionsReader,
    configReader?: RedshiftConnectionConfigurationReader
  ) {
    if (typeof arg === 'string') {
      super(arg, queryOptionsReader, configReader);
    } else {
      super(arg, queryOptionsReader);
    }
  }

  public isPool(): this is PooledConnection {
    return true;
  }

  public async drain(): Promise<void> {
    await this._pool?.end();
  }

  async getPool(): Promise<Pool> {
    if (!this._pool) {
      const {
        username: user,
        password,
        databaseName: database,
        port,
        host,
        connectionString,
      } = await this.readConfig();
      this._pool = new Pool({
        user,
        password,
        database,
        port,
        host,
        connectionString,
      });
      this._pool.on('acquire', client => client.query("SET TIME ZONE 'UTC'"));
    }
    return this._pool;
  }

  protected async runPostgresQuery(
    sqlCommand: string,
    _pageSize: number,
    _rowIndex: number,
    deJSON: boolean
  ): Promise<MalloyQueryData> {
    const pool = await this.getPool();
    let result = await pool.query(sqlCommand);

    if (Array.isArray(result)) {
      result = result.pop();
    }
    if (deJSON) {
      for (let i = 0; i < result.rows.length; i++) {
        result.rows[i] = result.rows[i].row;
      }
    }
    return {
      rows: result.rows as QueryData,
      totalRows: result.rows.length,
    };
  }

  public async *runSQLStream(
    sqlCommand: string,
    {rowLimit, abortSignal}: RunSQLOptions = {}
  ): AsyncIterableIterator<QueryDataRow> {
    const query = new QueryStream(sqlCommand);
    let index = 0;
    // This is a strange hack... `this.pool.query(query)` seems to return the wrong
    // type. Because `query` is a `QueryStream`, the result is supposed to be a
    // `QueryStream` as well, but it's not. So instead, we get a client and call
    // `client.query(query)`, which does what it's supposed to.
    const pool = await this.getPool();
    const client = await pool.connect();
    const resultStream: QueryStream = client.query(query);
    for await (const row of resultStream) {
      yield row.row as QueryDataRow;
      index += 1;
      if (
        (rowLimit !== undefined && index >= rowLimit) ||
        abortSignal?.aborted
      ) {
        query.destroy();
        break;
      }
    }
    client.release();
  }

  async close(): Promise<void> {
    await this.drain();
  }
}
