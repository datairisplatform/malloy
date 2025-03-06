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
  mkArrayDef,
} from '@malloydata/malloy';
import {BaseConnection} from '@malloydata/malloy/connection';

import {Client, Pool} from 'pg';
import {types} from 'pg';
// Override parser for 64-bit integers (OID 20) and standard integers (OID 23)
types.setTypeParser(20, val => parseInt(val, 10));
types.setTypeParser(23, val => parseInt(val, 10));
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
  private queryOptionsReader: QueryOptionsReader = {};
  private config: RedshiftConnectionConfigurationReader = {};

  private readonly dialect = new RedshiftDialect();

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
    //console.log('BRIAN fetching SELECT schema');
    const structDef: SQLSourceDef = {...sqlRef, fields: []};
    const tempTableName = `tmp${randomUUID()}`.replace(/-/g, '');
    const infoQuery = `DROP TABLE IF EXISTS ${tempTableName};
      CREATE TEMP TABLE ${tempTableName} AS ${sqlRef.selectStr};
      SELECT "column" as "column_name", type as "data_type", null as "comment"
      FROM pg_table_def
      WHERE tablename = '${tempTableName}';
      `;
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
      const queries = infoQuery;
      return `Error fetching SELECT schema for \n ${queries}: \n ${error}`;
    }
    return structDef;
  }

  private async schemaFromQuery(
    infoQuery: string | string[],
    structDef: StructDef
  ): Promise<void> {
    console.log(
      'BRIAN query: ',
      Array.isArray(infoQuery) ? infoQuery.join('\n') : infoQuery
    );
    const {rows, totalRows} = await this.runSQL(infoQuery);
    console.log('BRIAN rows: ', rows);
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
        //console.log('BRIAN structDef.fields', structDef.fields);
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
    const infoQuery = `SELECT "column_name", "data_type", "remarks" as "comment"
      FROM svv_columns
      WHERE table_schema = '${schema}'
      AND table_name = '${table}';`;

    try {
      await this.schemaFromQuery(infoQuery, structDef);
    } catch (error) {
      return `Error fetching TABLE schema for ${tablePath}: ${error.message}`;
    }
    console.log('BRIAN schema from table:', structDef);
    return structDef;
  }

  public async test(): Promise<void> {
    await this.runSQL('SELECT 1');
  }

  public async connectionSetup(client: Client): Promise<void> {
    await client.query("SET TIME ZONE 'UTC'");
  }
  protected async getClient(): Promise<Client> {
    const {
      username: user,
      password,
      databaseName: database,
      port,
      host,
    } = await this.readConfig();

    return new Client({
      user,
      password,
      database,
      port,
      host,
      ssl: {
        rejectUnauthorized: false, // For production, consider proper SSL validation
      },
    });
  }
  protected async runPostgresQuery(
    sql: string | string[],
    _pageSize: number,
    _rowIndex: number,
    deJSON: boolean
  ): Promise<MalloyQueryData> {
    const sqlArray = this.config.schema
      ? [`SET search_path TO ${this.config.schema};`]
      : [];
    if (Array.isArray(sql)) {
      sqlArray.push(...sql);
    } else {
      sqlArray.push(sql);
    }

    let client;
    try {
      console.log('BRIAN runPostgresQuery():');
      client = await this.getClient();
      console.log('BRIAN got client, attempting to connect');
      await client.connect();
      //await this.connectionSetup(client);

      // BEGIN and COMMIT are for executing multiple statements in a single transaction
      // ex: so the SET search_path is applied to all statements
      //await client.query('BEGIN');
      let result;
      for (const sqlStatement of sqlArray) {
        result = await client.query(sqlStatement);
      }
      console.log('BRIAN result: ', result);
      //await client.query('COMMIT');
      if (Array.isArray(result)) {
        result = result.pop();
      }
      // if (deJSON) {
      //   for (let i = 0; i < result.rows.length; i++) {
      //     result.rows[i] = result.rows[i].row;
      //   }
      // }
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
      console.log('BRIAN result:', result);

      await client.end();
      return {
        rows: result.rows as QueryData,
        totalRows: result.rows.length,
      };
    } catch (error) {
      throw new Error(`Error executing query: ${error.message}`);
    } finally {
      await client.end();
    }
  }

  public async runSQL(
    sql: string | string[],
    {rowLimit}: RunSQLOptions = {},
    _rowIndex = 0
  ): Promise<MalloyQueryData> {
    const sqlToRun = Array.isArray(sql) ? sql[0] : sql;
    return this.runPostgresQuery(sqlToRun, 100000, 0, true);
    // add statement in beginning of query to set the default schema/db
    // const sqlArray = [`SET search_path TO ${this.config.schema};`];
    // if (Array.isArray(sql)) {
    //   sqlArray.push(...sql);
    // } else {
    //   sqlArray.push(sql);
    // }
    // // Initiate RedshiftData client
    // const client = new RedshiftDataClient({region: this.config.region});

    // // Execute all SQL statements in batch
    // // so they share the same context
    // // if we don't do this, the "SET search_path" won't affect the following queries
    // const batchExecuteCommand = new BatchExecuteStatementCommand({
    //   WorkgroupName: this.config.workgroupName, //todo make configurable
    //   Database: this.config.database, // todo make configurable
    //   SecretArn: this.config.secretArn,
    //   Sqls: sqlArray,
    // });

    // const batchResponse = await client.send(batchExecuteCommand);
    // const batchId = batchResponse.Id;

    // // Wait for all queries to complete
    // let status: string | undefined;
    // let result;
    // let statusResponse;
    // do {
    //   // Pause for 1 second between status checks
    //   await new Promise(resolve => setTimeout(resolve, 1000));

    //   const describeCommand = new DescribeStatementCommand({
    //     Id: batchId,
    //   });
    //   statusResponse = await client.send(describeCommand);

    //   status = statusResponse.Status;
    // } while (
    //   status !== 'FINISHED' &&
    //   status !== 'FAILED' &&
    //   status !== 'ABORTED'
    // );

    // // If the batch finished successfully, fetch the results of the last statement
    // if (status === 'FINISHED') {
    //   // console.log('BRIAN final status check:', statusResponse);
    //   // Get the last statement's ID from the batch
    //   const lastStatementId =
    //     statusResponse.SubStatements?.[statusResponse.SubStatements.length - 1]
    //       .Id;

    //   if (lastStatementId) {
    //     const resultCommand = new GetStatementResultCommand({
    //       Id: lastStatementId,
    //     });
    //     result = await client.send(resultCommand);
    //   }
    // } else {
    //   throw new Error(
    //     `Batch error: \n sql: ${sqlArray.join('\n')} \n ${JSON.stringify(
    //       statusResponse,
    //       null,
    //       2
    //     )}`
    //   );
    // }

    // return {
    //   rows:
    //     result?.Records?.map(record => {
    //       const row: QueryDataRow = {};
    //       record.forEach((field, index) => {
    //         // Extract the first non-null value (longValue, stringValue etc)
    //         const value =
    //           field.longValue ?? field.stringValue ?? field.doubleValue ?? null;
    //         let key = result?.ColumnMetadata?.[index]?.name ?? '';
    //         if (key === '?column?') {
    //           // this can happen on SELECT 1 when there's no obvious column
    //           key = (index + 1).toString();
    //         }
    //         row[key] = value;
    //       });
    //       return row;
    //     }) ?? [],
    //   totalRows: result?.TotalNumRows ?? 0,
    // };
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
    // const cmd = `CREATE TEMPORARY TABLE IF NOT EXISTS ${tableName} AS (${sqlCommand});`;
    await this.runSQL(cmd);
    return tableName;
  }

  async close(): Promise<void> {
    return;
  }
}
