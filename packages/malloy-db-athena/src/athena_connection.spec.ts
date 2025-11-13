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

import {AthenaConnection} from './athena_connection';
import {describeIfDatabaseAvailable} from '@malloydata/malloy/test';

const [describe] = describeIfDatabaseAvailable(['athena']);

describe('AthenaConnection', () => {
  let connection: AthenaConnection;

  beforeAll(() => {
    // NOTE: Credentials are hardcoded for testing - remove before commit!
    connection = new AthenaConnection({
      name: 'athena',
      region: '',
      accessKeyId: '',
      secretAccessKey: '',
      database: 'malloytest',
      outputLocation: '',
    });
  });

  afterAll(async () => {
    await connection.close();
  });

  describe('Connection', () => {
    it('should connect successfully with test()', async () => {
      await expect(connection.test()).resolves.not.toThrow();
    }, 30000);

    it('should have correct name and dialect', () => {
      expect(connection.name).toBe('athena');
      expect(connection.dialectName).toBe('presto');
    });
  });

  describe('Query Execution', () => {
    it('should execute a simple SELECT 1 query', async () => {
      const result = await connection.runSQL('SELECT 1 as test_col', {});
      expect(result.rows).toBeDefined();
      expect(result.rows.length).toBeGreaterThan(0);
      expect(result.rows[0]['test_col']).toBe(1);
    }, 30000);

    it('should execute a query on the aircraft_models table', async () => {
      const result = await connection.runSQL(
        'SELECT * FROM aircraft_models LIMIT 10',
        {}
      );
      expect(result.rows).toBeDefined();
      expect(result.rows.length).toBeGreaterThanOrEqual(1);
      expect(result.rows.length).toBeLessThanOrEqual(10);
    }, 30000);

    it('should respect rowLimit option', async () => {
      const result = await connection.runSQL('SELECT * FROM aircraft_models', {
        rowLimit: 5,
      });
      expect(result.rows.length).toBeLessThanOrEqual(5);
    }, 30000);

    it('should handle queries with different data types', async () => {
      const result = await connection.runSQL(
        `SELECT
          'test_string' as string_col,
          123 as int_col,
          45.67 as float_col,
          true as bool_col,
          DATE '2023-01-01' as date_col
        `,
        {}
      );
      expect(result.rows.length).toBe(1);
      const row = result.rows[0];
      expect(row['string_col']).toBe('test_string');
      expect(row['int_col']).toBe(123);
      expect(row['float_col']).toBeCloseTo(45.67, 2);
      expect(row['bool_col']).toBe(true);
      expect(row['date_col']).toBeInstanceOf(Date);
    }, 30000);
  });

  describe('Type Conversion', () => {
    it('should correctly convert null values', async () => {
      const result = await connection.runSQL('SELECT null as null_col', {});
      expect(result.rows[0]['null_col']).toBeNull();
    }, 30000);

    it('should correctly convert numeric types', async () => {
      const result = await connection.runSQL(
        'SELECT CAST(42 as INTEGER) as int_val, CAST(3.14 as DOUBLE) as float_val',
        {}
      );
      const row = result.rows[0];
      expect(row['int_val']).toBe(42);
      expect(typeof row['int_val']).toBe('number');
      expect(row['float_val']).toBeCloseTo(3.14, 2);
    }, 30000);

    it('should correctly convert boolean types', async () => {
      const result = await connection.runSQL(
        'SELECT true as true_val, false as false_val',
        {}
      );
      const row = result.rows[0];
      expect(row['true_val']).toBe(true);
      expect(row['false_val']).toBe(false);
    }, 30000);

    it('should correctly convert date/timestamp types', async () => {
      const result = await connection.runSQL(
        `SELECT
          DATE '2023-06-15' as date_val,
          TIMESTAMP '2023-06-15 10:30:00' as timestamp_val
        `,
        {}
      );
      const row = result.rows[0];
      expect(row['date_val']).toBeInstanceOf(Date);
      expect(row['timestamp_val']).toBeInstanceOf(Date);
    }, 30000);
  });

  describe('Error Handling', () => {
    it('should throw error for invalid SQL', async () => {
      await expect(
        connection.runSQL('SELECT * FROM nonexistent_table_xyz', {})
      ).rejects.toThrow();
    }, 30000);

    it('should return empty schema for invalid table', async () => {
      const schema = await connection.fetchTableSchema(
        'nonexistent',
        'nonexistent_table_xyz'
      );
      // Athena returns an empty schema for non-existent tables
      expect(schema.fields).toEqual([]);
    }, 30000);
  });
});
