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
  Connection,
  ConnectionConfig,
  MalloyQueryData,
  PersistSQLResults,
  PooledConnection,
  DatabricksDialect,
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

import {Client} from 'pg';
import {DBSQLClient} from '@databricks/sql';
import crypto from 'crypto';

interface DatabricksConnectionConfiguration {
  host?: string;
  port?: number;
  username?: string;
  password?: string;
  databaseName?: string;
  connectionString?: string;
}

type DatabricksConnectionConfigurationReader =
  | DatabricksConnectionConfiguration
  | (() => Promise<DatabricksConnectionConfiguration>);

const DEFAULT_PAGE_SIZE = 1000;
// const SCHEMA_PAGE_SIZE = 1000;

const databricks_token = 'todo';
const server_hostname = 'todo';
const http_path = 'todo';
export interface DatabricksConnectionOptions extends ConnectionConfig {
  host?: string;
  port?: number;
  username?: string;
  password?: string;
  databaseName?: string;
  connectionString?: string;
}

export class DatabricksConnection
  extends BaseConnection
  implements Connection, StreamingConnection, PersistSQLResults
{
  public readonly name: string;
  private queryOptionsReader: QueryOptionsReader = {};
  private configReader: DatabricksConnectionConfigurationReader = {};

  private readonly dialect = new DatabricksDialect();

  private client: DBSQLClient | null = null;
  private session: any | null = null;
  private connecting: Promise<void>;

  constructor(
    arg: string | DatabricksConnectionOptions,
    queryOptionsReader?: QueryOptionsReader,
    configReader?: DatabricksConnectionConfigurationReader
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

    // Initialize connection
    this.connecting = this.connect();
  }

  private async connect(): Promise<void> {
    this.client = new DBSQLClient();
    await this.client.connect({
      token: databricks_token,
      host: server_hostname,
      path: http_path,
    });
    this.session = await this.client.openSession();
  }

  private async readQueryConfig(): Promise<RunSQLOptions> {
    if (this.queryOptionsReader instanceof Function) {
      return this.queryOptionsReader();
    } else {
      return this.queryOptionsReader;
    }
  }

  // protected async readConfig(): Promise<DatabricksConnectionConfiguration> {
  //   if (this.configReader instanceof Function) {
  //     return this.configReader();
  //   } else {
  //     return this.configReader;
  //   }
  // }

  get dialectName(): string {
    return 'databricks';
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

  // protected async getClient(): Promise<Client> {
  //   const {
  //     username: user,
  //     password,
  //     databaseName: database,
  //     port,
  //     host,
  //     connectionString,
  //   } = await this.readConfig();
  //   return new Client({
  //     user,
  //     password,
  //     database,
  //     port,
  //     host,
  //     connectionString,
  //   });
  // }

  // protected async runPostgresQuery(
  //   sqlCommand: string,
  //   _pageSize: number,
  //   _rowIndex: number,
  //   deJSON: boolean
  // ): Promise<MalloyQueryData> {
  //   const client = await this.getClient();
  //   await client.connect();
  //   await this.connectionSetup(client);

  //   let result = await client.query(sqlCommand);
  //   if (Array.isArray(result)) {
  //     result = result.pop();
  //   }
  //   if (deJSON) {
  //     for (let i = 0; i < result.rows.length; i++) {
  //       result.rows[i] = result.rows[i].row;
  //     }
  //   }
  //   await client.end();
  //   return {
  //     rows: result.rows as QueryData,
  //     totalRows: result.rows.length,
  //   };
  // }

  async fetchSelectSchema(
    sqlRef: SQLSourceDef
  ): Promise<SQLSourceDef | string> {
    const structDef: SQLSourceDef = {...sqlRef, fields: []};
    // const tempTableName = `tmp${randomUUID()}`.replace(/-/g, '');
    // const infoQuery = `
    //   CREATE OR REPLACE TEMP VIEW temp_schema_view AS
    //   ${sqlRef.selectStr};
    //   DESCRIBE TABLE temp_schema_view;
    // `;

    const infoQuery = [
      `CREATE OR REPLACE TEMP VIEW temp_schema_view AS
      ${sqlRef.selectStr};`,
      'DESCRIBE TABLE temp_schema_view;',
    ];
    // const infoQuery = `
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
      return `SELECT Error fetching schema for ${sqlRef.selectStr}: ${error}`;
    }
    return structDef;
  }

  private async schemaFromQuery(
    infoQuery: string[],
    structDef: StructDef
  ): Promise<void> {
    const {rows, totalRows} = await this.runRawSQL(infoQuery);
    if (!totalRows) {
      throw new Error('Unable to read schema.');
    }
    for (const row of rows) {
      const databricksDataType = row['data_type'] as string;
      const name = row['col_name'] as string;
      if (databricksDataType === 'ARRAY') {
        const elementType = this.dialect.sqlTypeToMalloyType(
          row['element_type'] as string
        );
        structDef.fields.push(mkArrayDef(elementType, name));
      } else {
        const malloyType = this.dialect.sqlTypeToMalloyType(databricksDataType);
        structDef.fields.push({...malloyType, name});
      }
    }
  }

  async fetchTableSchema(
    tableKey: string,
    tablePath: string
  ): Promise<TableSourceDef | string> {
    const structDef: StructDef = {
      type: 'table',
      name: tableKey,
      dialect: 'databricks',
      tablePath,
      connection: this.name,
      fields: [],
    };
    const [_, table] = tablePath.split('.');
    if (table === undefined) {
      return 'Table is undefined';
    }

    const infoQuery = `
      DESCRIBE TABLE ${tablePath}
    `;

    try {
      await this.schemaFromQuery([infoQuery], structDef);
    } catch (error) {
      return `Table Error fetching schema for ${tablePath}: ${error.message}`;
    }
    return structDef;
  }

  public async test(): Promise<void> {
    await this.runSQL('SELECT 1');
  }

  public async connectionSetup(client: Client): Promise<void> {
    await client.query("SET TIME ZONE 'UTC'");
  }

  public async runRawSQL(
    sql: string[],
    {rowLimit}: RunSQLOptions = {},
    _rowIndex = 0
  ): Promise<MalloyQueryData> {
    const config = await this.readQueryConfig();

    await this.connecting; // Wait for connection to be established
    if (!this.client || !this.session) {
      throw new Error('Databricks connection not established');
    }

    let result: QueryDataRow[] = [];
    for (let i = 0; i < sql.length; i++) {
      const queryOperation = await this.session.executeStatement(sql[i], {
        runAsync: true,
      });
      result = (await queryOperation.fetchAll()) as QueryDataRow[];
      await queryOperation.close();
    }

    // restrict num rows if necessary
    const databricksRowLimit = rowLimit ?? config.rowLimit ?? DEFAULT_PAGE_SIZE;
    if (result.length > databricksRowLimit) {
      result = result.slice(0, databricksRowLimit);
    }

    return {rows: result, totalRows: result.length};
  }

  public async runSQL(
    sql: string,
    {rowLimit}: RunSQLOptions = {},
    _rowIndex = 0
  ): Promise<MalloyQueryData> {
    const config = await this.readQueryConfig();

    await this.connecting; // Wait for connection to be established
    if (!this.client || !this.session) {
      throw new Error('Databricks connection not established');
    }

    const queryOperation = await this.session.executeStatement(sql, {
      runAsync: true,
    });

    let result = (await queryOperation.fetchAll()) as QueryDataRow[];

    // Extract actual result from Databricks response
    const actualResult = result.map(row =>
      row['row'] ? JSON.parse(String(row['row'])) : row
    );

    await queryOperation.close();

    // restrict num rows if necessary
    const databricksRowLimit = rowLimit ?? config.rowLimit ?? DEFAULT_PAGE_SIZE;
    if (result.length > databricksRowLimit) {
      result = result.slice(0, databricksRowLimit);
    }

    return {rows: actualResult, totalRows: result.length};
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
    const cmd = `CREATE or replace TEMPORARY VIEW ${tableName} AS (${sqlCommand})`;
    await this.runSQL(cmd);
    return tableName;
  }

  async close(): Promise<void> {
    if (this.session) {
      await this.session.close();
      this.session = null;
    }
    if (this.client) {
      await this.client.close();
      this.client = null;
    }
  }
}
