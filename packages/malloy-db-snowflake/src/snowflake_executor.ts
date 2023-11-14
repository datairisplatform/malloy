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

import snowflake, {
  SnowflakeError,
  Statement,
  Connection,
  ConnectionOptions,
} from 'snowflake-sdk';
import {Pool, Options as PoolOptions} from 'generic-pool';
import * as toml from 'toml';
import * as fs from 'fs';
import * as path from 'path';
import {Readable} from 'stream';
import {toAsyncGenerator, QueryData, QueryDataRow} from '@malloydata/malloy';

export interface SnowflakeQueryOptions {
  rowLimit: number;
}

export interface ConnectionConfigFile {
  // if not supplied "default" connection is used
  connection_name?: string;
  connection_file_path: string;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function isConnectionConfig(obj: any): obj is ConnectionOptions {
  return obj && obj.account && (obj.user || obj.username);
}

function columnNameToLowerCase(row: QueryDataRow): QueryDataRow {
  const ret: QueryDataRow = {};
  for (const key in row) {
    ret[key.toLowerCase()] = row[key];
  }
  return ret;
}

export class SnowflakeExecutor {
  private pool_: Pool<Connection>;
  private defaultPoolOptions_: PoolOptions = {
    min: 0,
    max: 1,
    evictionRunIntervalMillis: 60000, // default = 0, off
    idleTimeoutMillis: 60000, // default = 30000
  };

  constructor(
    connOptions?: ConnectionOptions | ConnectionConfigFile,
    poolOptions?: PoolOptions
  ) {
    this.pool_ = snowflake.createPool(
      SnowflakeExecutor.getConnectionOptions(connOptions),
      {
        ...this.defaultPoolOptions_,
        ...(poolOptions ?? {}),
      }
    );
  }

  public static getConnectionOptions(
    options?: ConnectionOptions | ConnectionConfigFile
  ): ConnectionOptions {
    const defaultOptions = {
      clientSessionKeepAlive: true, // default = false
      clientSessionKeepAliveHeartbeatFrequency: 900, // default = 3600
    };

    if (isConnectionConfig(options)) {
      return {
        ...defaultOptions,
        ...options,
        // python api expects user, while node expects username, let's hand both
        username: options['user'] ?? options['username'],
      };
    }

    let location: string | undefined = options?.connection_file_path;
    if (location === undefined) {
      const homeDir = process.env['HOME'] || process.env['USERPROFILE'];
      if (homeDir === undefined) {
        throw new Error(
          'must provide either snowflake ConnectionOptions via constructor or path to a connections.toml'
        );
      }
      location = path.join(homeDir, '.snowflake', 'connections.toml');
    }

    if (!fs.existsSync(location)) {
      throw new Error(
        `provided snowflake connection config file: ${options} does not exist`
      );
    }

    const tomlData = fs.readFileSync(location, 'utf-8');
    const connections = toml.parse(tomlData);

    const connection = connections[options?.connection_name ?? 'default'];
    // sometimes the connection file uses "user" instead of "username"
    if (
      connection['username'] === undefined &&
      connection['user'] !== undefined
    ) {
      connection['username'] = connection['user'];
    }
    if (!connection || !connection.account || !connection.username) {
      throw new Error(
        `provided snowflake connection config file: ${options} is not valid`
      );
    }

    return {
      // some basic options we configure by default but can be overriden
      ...defaultOptions,
      account: connection.account,
      username: connection.user,
      password: connection.password,
      warehouse: connection.warehouse,
      database: connection.database,
      schema: connection.schema,
      ...connection,
    };
  }

  public async done() {
    await this.pool_.drain().then(() => {
      this.pool_.clear();
    });
  }

  public async batch(sqlText: string): Promise<QueryData> {
    return await this.pool_.use(async (conn: Connection) => {
      return new Promise((resolve, reject) => {
        const _statment = conn.execute({
          sqlText,
          complete: (
            err: SnowflakeError | undefined,
            _stmt: Statement,
            rows?: QueryData
          ) => {
            if (err) {
              reject(err);
            } else if (rows) {
              resolve(rows.map(columnNameToLowerCase));
            }
          },
        });
      });
    });
  }

  public async stream(
    sqlText: string,
    options?: SnowflakeQueryOptions
  ): Promise<AsyncIterableIterator<QueryDataRow>> {
    const pool: Pool<Connection> = this.pool_;
    return await pool.acquire().then(async (conn: Connection) => {
      const stmt: Statement = conn.execute({
        sqlText,
        streamResult: true,
      });
      const stream: Readable = stmt.streamRows();
      function streamSnowflake(
        onError: (error: Error) => void,
        onData: (data: QueryDataRow) => void,
        onEnd: () => void
      ) {
        function handleEnd() {
          onEnd();
          pool.release(conn);
        }

        let index = 0;
        function handleData(this: Readable, row: QueryDataRow) {
          onData(columnNameToLowerCase(row));
          index += 1;
          if (options?.rowLimit !== undefined && index >= options.rowLimit) {
            onEnd();
          }
        }
        stream.on('error', onError);
        stream.on('data', handleData);
        stream.on('end', handleEnd);
      }
      return Promise.resolve(toAsyncGenerator<QueryDataRow>(streamSnowflake));
    });
  }
}
