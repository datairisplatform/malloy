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

import {indent} from '../../model/utils';
import {
  DateUnit,
  Expr,
  ExtractUnit,
  Sampling,
  TimeFieldType,
  TimeValue,
  TimestampUnit,
  TypecastFragment,
  isSamplingEnable,
  isSamplingPercent,
  isSamplingRows,
  mkExpr,
  FieldAtomicTypeDef,
} from '../../model/malloy_types';
import {SNOWFLAKE_FUNCTIONS} from './functions';
import {DialectFunctionOverloadDef} from '../functions';
import {Dialect, DialectFieldList, QueryInfo, inDays, qtz} from '../dialect';

const extractionMap: Record<string, string> = {
  'day_of_week': 'dayofweek',
  'day_of_year': 'dayofyear',
};

const snowflakeToMalloyTypes: {[key: string]: FieldAtomicTypeDef} = {
  // string
  'varchar': {type: 'string'},
  'text': {type: 'string'},
  'string': {type: 'string'},
  'char': {type: 'string'},
  'character': {type: 'string'},
  'nvarchar': {type: 'string'},
  'nvarchar2': {type: 'string'},
  'char varying': {type: 'string'},
  'nchar varying': {type: 'string'},
  // numbers
  'number': {type: 'number', numberType: 'integer'},
  'numeric': {type: 'number', numberType: 'integer'},
  'decimal': {type: 'number', numberType: 'integer'},
  'dec': {type: 'number', numberType: 'integer'},
  'integer': {type: 'number', numberType: 'integer'},
  'int': {type: 'number', numberType: 'integer'},
  'bigint': {type: 'number', numberType: 'integer'},
  'smallint': {type: 'number', numberType: 'integer'},
  'tinyint': {type: 'number', numberType: 'integer'},
  'byteint': {type: 'number', numberType: 'integer'},
  'float': {type: 'number', numberType: 'float'},
  'float4': {type: 'number', numberType: 'float'},
  'float8': {type: 'number', numberType: 'float'},
  'double': {type: 'number', numberType: 'float'},
  'double precision': {type: 'number', numberType: 'float'},
  'real': {type: 'number', numberType: 'float'},
  'boolean': {type: 'boolean'},
  // time and date
  'date': {type: 'date'},
  'timestamp': {type: 'timestamp'},
  'timestampltz': {type: 'timestamp'}, // maybe not
  'timestamp_ltz': {type: 'timestamp'}, // maybe not
  'timestamp with local time zone': {type: 'timestamp'}, // maybe not
  'timestampntz': {type: 'timestamp'},
  'timestamp_ntz': {type: 'timestamp'},
  'timestamp without time zone': {type: 'timestamp'},
};

export class SnowflakeDialect extends Dialect {
  name = 'snowflake';
  defaultNumberType = 'NUMBER';
  defaultDecimalType = 'NUMBER';
  udfPrefix = '__udf';
  hasFinalStage = false;
  divisionIsInteger = false;
  supportsSumDistinctFunction = false;
  defaultSampling = {rows: 50000};
  globalFunctions = SNOWFLAKE_FUNCTIONS;

  // NOTE: safely setting all these to false for now
  // more many be implemented in future
  unnestWithNumbers = false;
  supportUnnestArrayAgg = false;
  supportsAggDistinct = false;
  supportsCTEinCoorelatedSubQueries = false;
  supportsSafeCast = false;
  dontUnionIndex = false;
  supportsQualify = false;
  supportsNesting = false;

  quoteTablePath(tablePath: string): string {
    return tablePath
      .split('.')
      .map(part => `"${part}"`)
      .join('.');
  }

  sqlGroupSetTable(groupSetCount: number): string {
    return `CROSS JOIN (SELECT seq4() as group_set FROM TABLE(GENERATOR(ROWCOUNT => ${
      groupSetCount + 1
    }))) as group_set`;
  }

  /*
  // this just returns null in snowflake
  select
  any_value (t is not null) as av
from
  (
    select
      case
        when group_set = 1 then 'one'
      end as t
    from
      (
        SELECT
          seq4 () as group_set
        FROM
          TABLE (GENERATOR (ROWCOUNT => 3))
      ) as tbl
  );
  */
  sqlAnyValue(groupSet: number, fieldName: string): string {
    return `(ARRAY_AGG(CASE WHEN group_set=${groupSet} THEN ${fieldName} END) WITHIN GROUP (ORDER BY ${fieldName} ASC NULLS LAST))[0]`;
  }

  mapFields(fieldList: DialectFieldList): string {
    return fieldList
      .map(f => `\n  ${f.sqlExpression} as ${f.sqlOutputName}`)
      .join(', ');
  }

  mapFieldsForObjectConstruct(fieldList: DialectFieldList): string {
    return fieldList
      .map(f => `'${f.sqlOutputName}', (${f.sqlExpression})`)
      .join(', ');
  }

  sqlAggregateTurtle(
    groupSet: number,
    fieldList: DialectFieldList,
    orderBy: string | undefined,
    limit: number | undefined
  ): string {
    const fields = this.mapFieldsForObjectConstruct(fieldList);
    const orderByClause = orderBy ? ` WITHIN GROUP (${orderBy})` : '';
    const aggClause = `ARRAY_AGG(CASE WHEN group_set=${groupSet} THEN OBJECT_CONSTRUCT(${fields}) END)${orderByClause}`;
    if (limit === undefined) {
      return `COALESCE(${aggClause}, [])`;
    }
    return `COALESCE(ARRAY_SLICE(${aggClause}, 0, ${limit}), [])`;
  }

  sqlAnyValueTurtle(groupSet: number, fieldList: DialectFieldList): string {
    const fields = this.mapFieldsForObjectConstruct(fieldList);
    return `(ARRAY_AGG(CASE WHEN group_set=${groupSet} THEN OBJECT_CONSTRUCT(${fields}) END) WITHIN GROUP (ORDER BY 1 ASC NULLS LAST))[0]`;
  }

  sqlAnyValueLastTurtle(
    name: string,
    groupSet: number,
    sqlName: string
  ): string {
    return `(ARRAY_AGG(CASE WHEN group_set=${groupSet} THEN ${name} END) WITHIN GROUP (ORDER BY ${name} ASC NULLS LAST))[0] AS ${sqlName}`;
  }

  sqlCoaleseMeasuresInline(
    groupSet: number,
    fieldList: DialectFieldList
  ): string {
    const fields = this.mapFieldsForObjectConstruct(fieldList);
    const nullValues = fieldList
      .map(f => `'${f.sqlOutputName}', NULL`)
      .join(', ');
    return `COALESCE(ARRAY_AGG(CASE WHEN group_set=${groupSet} THEN OBJECT_CONSTRUCT(${fields}) END)[0], OBJECT_CONSTRUCT_KEEP_NULL(${nullValues}))`;
  }

  sqlUnnestAlias(
    _source: string,
    _alias: string,
    _fieldList: DialectFieldList,
    _needDistinctKey: boolean,
    _isArray: boolean,
    _isInNestedPipeline: boolean
  ): string {
    throw new Error('nesting: not fully supported in snowflake');
  }

  /*
  // For comparison against the equivalent function implemented in standardsql dialect
  select
    (
      to_number (
        substr (md5_hex ('hello'), 1, 15),
        repeat ('X', 15)
      ) * 4294967296 + to_number (
        substr (md5_hex ('hello'), 16, 8),
        repeat ('X', 8)
      )
    ) * 0.000000001 as hash;
  +-------------------------------+
  |                          HASH |
  |-------------------------------|
  | 1803811819465386377.040304601 |
  +-------------------------------+
  */
  sqlSumDistinctHashedKey(sqlDistinctKey: string): string {
    sqlDistinctKey = `${sqlDistinctKey}::STRING`;
    const upperPart = `to_number(substr(md5_hex(${sqlDistinctKey}), 1, 15), repeat('X', 15)) * 4294967296`;
    const lowerPart = `to_number(substr(md5_hex(${sqlDistinctKey}), 16, 8), repeat('X', 8))`;
    const precisionShiftMultiplier = '0.000000001';
    return `(${upperPart} + ${lowerPart}) * ${precisionShiftMultiplier}`;
  }

  sqlGenerateUUID(): string {
    return 'UUID_STRING()';
  }

  sqlFieldReference(
    alias: string,
    fieldName: string,
    _fieldType: string,
    isNested: boolean,
    _isArray: boolean
  ): string {
    if (!isNested) {
      return `${alias}."${fieldName}"`;
    }
    throw new Error('nesting: not fully supported in snowflake');
  }

  sqlUnnestPipelineHead(
    _isSingleton: boolean,
    _sourceSQLExpression: string
  ): string {
    throw new Error('nesting: not fully supported in snowflake');
  }

  sqlCreateFunction(_id: string, _funcText: string): string {
    throw new Error('not implemented yet');
  }

  sqlCreateFunctionCombineLastStage(_lastStageName: string): string {
    throw new Error('not implemented yet');
  }

  sqlSelectAliasAsStruct(alias: string): string {
    return `OBJECT_CONSTRUCT(${alias}.*)`;
  }
  sqlMaybeQuoteIdentifier(identifier: string): string {
    return `"${identifier}"`;
  }

  sqlCreateTableAsSelect(tableName: string, sql: string): string {
    return `
CREATE TEMP TABLE IF NOT EXISTS \`${tableName}\`
AS (
${indent(sql)}
);
`;
  }

  sqlNow(): Expr {
    return mkExpr`CURRENT_TIMESTAMP()`;
  }

  // FIXME: does not work with timezone
  sqlTrunc(qi: QueryInfo, sqlTime: TimeValue, units: TimestampUnit): Expr {
    // adjusting for monday/sunday weeks
    const week = units === 'week';
    const truncThis = week
      ? mkExpr`${sqlTime.value} + INTERVAL '1 DAY'`
      : sqlTime.value;
    if (sqlTime.valueType === 'timestamp') {
      const tz = qtz(qi);
      if (tz) {
        const civilSource = mkExpr`CONVERT_TIMEZONE('${tz}', ${truncThis})`;
        let civilTrunc = mkExpr`DATE_TRUNC('${units}', ${civilSource})`;
        // MTOY todo ... only need to do this if this is a date ...
        civilTrunc = mkExpr`${civilTrunc}::TIMESTAMP`;
        const truncTsTz = mkExpr`CONVERT_TIMEZONE('${tz}', ${civilTrunc})`;
        return mkExpr`(${truncTsTz})`;
      }
    }
    let result = mkExpr`DATE_TRUNC('${units}', ${truncThis})`;
    if (week) {
      result = mkExpr`(${result} - INTERVAL '1 DAY')`;
    }
    return result;
  }

  sqlExtract(qi: QueryInfo, from: TimeValue, units: ExtractUnit): Expr {
    const sflUnits = extractionMap[units] || units;
    let extractFrom = from.value;
    if (from.valueType === 'timestamp') {
      const tz = qtz(qi);
      if (tz) {
        extractFrom = mkExpr`CONVERT_TIMEZONE('${tz}', ${extractFrom})`;
      }
    }
    const extracted = mkExpr`EXTRACT(${sflUnits} FROM ${extractFrom})`;
    return units === 'day_of_week' ? mkExpr`(${extracted}+1)` : extracted;
  }

  sqlAlterTime(
    op: '+' | '-',
    expr: TimeValue,
    n: Expr,
    timeframe: DateUnit
  ): Expr {
    const interval = mkExpr`INTERVAL '${n} ${timeframe}'`;
    return mkExpr`((${expr.value})${op}${interval})`;
  }

  sqlCast(qi: QueryInfo, cast: TypecastFragment): Expr {
    const op = `${cast.srcType}::${cast.dstType}`;
    const tz = qtz(qi);
    if (op === 'timestamp::date' && tz) {
      return mkExpr`TO_DATE(CONVERT_TIMEZONE('${tz}', ${cast.expr}::TIMESTAMP))`;
    } else if (op === 'date::timestamp' && tz) {
      return mkExpr`CONVERT_TIMEZONE('${tz}', ${cast.expr}::TIMESTAMP)`;
    }
    if (cast.srcType !== cast.dstType) {
      const dstType =
        typeof cast.dstType === 'string'
          ? this.malloyTypeToSQLType({type: cast.dstType})
          : cast.dstType.raw;
      if (cast.safe) {
        throw new Error("Snowflake dialect doesn't support Safe Cast");
      }
      const castFunc = 'CAST';
      return mkExpr`${castFunc}(${cast.expr} AS ${dstType})`;
    }
    return cast.expr;
  }

  sqlRegexpMatch(expr: Expr, regexp: Expr): Expr {
    return mkExpr`REGEXP_LIKE(${expr}, ${regexp})`;
  }

  sqlLiteralTime(
    qi: QueryInfo,
    timeString: string,
    type: TimeFieldType,
    timezone: string | undefined
  ): string {
    if (type === 'date') {
      return `TO_DATE('${timeString}')`;
    } else if (type === 'timestamp') {
      const tz = timezone || qtz(qi);
      if (tz) {
        return `CONVERT_TIMEZONE('${tz}', TO_TIMESTAMP_NTZ('${timeString}'))`;
      }
      return `TO_TIMESTAMP('${timeString}')`;
    }
    throw new Error(`Unsupported literal time format: ${type}`);
  }

  sqlMeasureTime(from: TimeValue, to: TimeValue, units: string): Expr {
    let lVal = from.value;
    let rVal = to.value;
    if (!inDays(units)) {
      throw new Error(`Unknown or unhandled snowflake time unit: ${units}`);
    }
    if (from.valueType === 'date') {
      lVal = mkExpr`(${lVal})::TIMESTAMP`;
    }
    if (to.valueType === 'date') {
      rVal = mkExpr`(${rVal})::TIMESTAMP`;
    }
    return mkExpr`TIMESTAMPDIFF('${units}',${lVal},${rVal})`;
  }

  sqlSampleTable(tableSQL: string, sample: Sampling | undefined): string {
    if (sample !== undefined) {
      if (isSamplingEnable(sample) && sample.enable) {
        sample = this.defaultSampling;
      }
      if (isSamplingRows(sample)) {
        return `(SELECT * FROM ${tableSQL} TABLESAMPLE (${sample.rows} ROWS))`;
      } else if (isSamplingPercent(sample)) {
        return `(SELECT * FROM ${tableSQL} TABLESAMPLE (${sample.percent}))`;
      }
    }
    return tableSQL;
  }

  sqlOrderBy(orderTerms: string[]): string {
    return `ORDER BY ${orderTerms.map(t => `${t} NULLS LAST`).join(',')}`;
  }

  sqlLiteralString(literal: string): string {
    const noVirgule = literal.replace(/\\/g, '\\\\');
    return "'" + noVirgule.replace(/'/g, "\\'") + "'";
  }

  sqlLiteralRegexp(literal: string): string {
    const noVirgule = literal.replace(/\\/g, '\\\\');
    return "'" + noVirgule.replace(/'/g, "\\'") + "'";
  }

  getGlobalFunctionDef(name: string): DialectFunctionOverloadDef[] | undefined {
    return SNOWFLAKE_FUNCTIONS.get(name);
  }

  malloyTypeToSQLType(malloyType: FieldAtomicTypeDef): string {
    if (malloyType.type === 'number') {
      if (malloyType.numberType === 'integer') {
        return 'integer';
      } else {
        return 'double';
      }
    }
    return malloyType.type;
  }

  sqlTypeToMalloyType(sqlType: string): FieldAtomicTypeDef | undefined {
    // Remove trailing params
    const baseSqlType = sqlType.match(/^([\w\s]+)/)?.at(0) ?? sqlType;
    return snowflakeToMalloyTypes[baseSqlType.trim().toLowerCase()];
  }

  castToString(expression: string): string {
    return `TO_VARCHAR(${expression})`;
  }

  concat(...values: string[]): string {
    return values.join(' || ');
  }

  validateTypeName(sqlType: string): boolean {
    // Letters:              BIGINT
    // Numbers:              INT8
    // Spaces:               TIMESTAMP WITH TIME ZONE
    // Parentheses, Commas:  NUMERIC(5, 2)
    // Square Brackets:      INT64[]
    return sqlType.match(/^[A-Za-z\s(),[\]0-9]*$/) !== null;
  }
}
