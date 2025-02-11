import {
  Dialect,
  DialectFieldList,
  FieldReferenceType,
  QueryInfo,
  qtz,
} from '../dialect';
import {indent} from '../../model/utils';
import {
  Sampling,
  isSamplingEnable,
  isSamplingPercent,
  isSamplingRows,
  AtomicTypeDef,
  TimeDeltaExpr,
  TypecastExpr,
  MeasureTimeExpr,
  LeafAtomicTypeDef,
  RecordLiteralNode,
  ArrayLiteralNode,
  RegexMatchExpr,
  TimeExtractExpr,
  TimeLiteralNode,
  TimeTruncExpr,
  TD,
} from '../../model/malloy_types';
import {
  DialectFunctionOverloadDef,
  expandOverrideMap,
  expandBlueprintMap,
} from '../functions';
import {DATABRICKS_MALLOY_STANDARD_OVERLOADS} from './function_overrides';
import {DATABRICKS_DIALECT_FUNCTIONS} from './dialect_functions';

const inSeconds: Record<string, number> = {
  'second': 1,
  'minute': 60,
  'hour': 3600,
  'day': 24 * 3600,
  'week': 7 * 24 * 3600,
};

const databricksToMalloyTypes: {[key: string]: LeafAtomicTypeDef} = {
  'character varying': {type: 'string'},
  'name': {type: 'string'},
  'string': {type: 'string'},
  'date': {type: 'date'},
  'int': {type: 'number', numberType: 'integer'},
  'bigint': {type: 'number', numberType: 'integer'},
  'double': {type: 'number', numberType: 'float'},
  'timestamp_ntz': {type: 'timestamp'}, // maybe not
  'boolean': {type: 'boolean'},
  // ARRAY: "string",
  'timestamp': {type: 'timestamp'},
  'smallint': {type: 'number', numberType: 'integer'},
  //'real': {type: 'number', numberType: 'float'},
  'interval': {type: 'string'},
  //'regtype': {type: 'string'},
  //'numeric': {type: 'number', numberType: 'float'}, // ?
  //'bytea': {type: 'string'},
  //'pg_ndistinct': {type: 'number', numberType: 'integer'},
  //'varchar': {type: 'string'},
};

export class DatabricksDialect extends Dialect {
  name = 'databricks';
  defaultNumberType = 'BIGINT'; // ?
  defaultDecimalType = 'DECIMAL';
  udfPrefix = 'pg_temp.__udf';
  hasFinalStage = false;
  divisionIsInteger = true;
  supportsSumDistinctFunction = true;
  unnestWithNumbers = false;
  defaultSampling = {rows: 50000};
  supportUnnestArrayAgg = true;
  supportsAggDistinct = true;
  supportsCTEinCoorelatedSubQueries = true;
  supportsSafeCast = false;
  dontUnionIndex = false;
  supportsQualify = false;
  supportsNesting = false; // temp
  experimental = false;
  readsNestedData = false;
  supportsComplexFilteredSources = false;
  compoundObjectInSchema = false;
  supportsPipelinesInViews = false;
  quoteTablePath(tablePath: string): string {
    return tablePath
      .split('.')
      .map(part => `${part}`)
      .join('.');
  }

  sqlGroupSetTable(groupSetCount: number): string {
    return `CROSS JOIN (SELECT EXPLODE(SEQUENCE(0,${groupSetCount},1)) as group_set)`;
  }

  sqlAnyValue(groupSet: number, fieldName: string): string {
    return `ANY(${fieldName})`;
  }

  mapFields(fieldList: DialectFieldList): string {
    return fieldList
      .map(
        f =>
          `\n  ${f.sqlExpression}${
            f.type === 'number' ? `::${this.defaultNumberType}` : ''
          } as ${f.sqlOutputName}`
        //`${f.sqlExpression} ${f.type} as ${f.sqlOutputName}`
      )
      .join(', ');
  }

  sqlAggregateTurtle(
    groupSet: number,
    fieldList: DialectFieldList,
    orderBy: string | undefined,
    limit: number | undefined
  ): string {
    let tail = '';
    if (limit !== undefined) {
      tail += `[1:${limit}]`;
    }
    const fields = this.mapFields(fieldList);
    // return `(ARRAY_AGG((SELECT __x FROM (SELECT ${fields}) as __x) ${orderBy} ) FILTER (WHERE group_set=${groupSet}))${tail}`;
    return `COALESCE(TO_JSONB((ARRAY_AGG((SELECT TO_JSONB(__x) FROM (SELECT ${fields}\n  ) as __x) ${orderBy} ) FILTER (WHERE group_set=${groupSet}))${tail}),'[]'::JSONB)`;
  }

  sqlAnyValueTurtle(groupSet: number, fieldList: DialectFieldList): string {
    const fields = fieldList
      .map(f => `${f.sqlExpression} as ${f.sqlOutputName}`)
      .join(', ');
    return `ANY_VALUE(CASE WHEN group_set=${groupSet} THEN STRUCT(${fields}))`;
  }

  sqlAnyValueLastTurtle(
    name: string,
    groupSet: number,
    sqlName: string
  ): string {
    return `GET((ARRAY_AGG(${name}) FILTER (WHERE group_set=${groupSet} AND ${name} IS NOT NULL)),0) as ${sqlName}`;
  }

  sqlCoaleseMeasuresInline(
    groupSet: number,
    fieldList: DialectFieldList
  ): string {
    const fields = this.mapFields(fieldList);
    return `TO_JSONB((ARRAY_AGG((SELECT __x FROM (SELECT ${fields}) as __x)) FILTER (WHERE group_set=${groupSet}))[1])`;
  }

  getDialectFunctions(): {[name: string]: DialectFunctionOverloadDef[]} {
    return expandBlueprintMap(DATABRICKS_DIALECT_FUNCTIONS);
  }

  // UNNEST((select ARRAY((SELECT ROW(gen_random_uuid()::text, state, airport_count) FROM UNNEST(base.by_state) as by_state(state text, airport_count numeric, by_fac_type record[]))))) as by_state(__distinct_key text, state text, airport_count numeric)

  // sqlUnnestAlias(
  //   source: string,
  //   alias: string,
  //   fieldList: DialectFieldList,
  //   needDistinctKey: boolean
  // ): string {
  //   const fields = [];
  //   for (const f of fieldList) {
  //     let t = undefined;
  //     switch (f.type) {
  //       case "string":
  //         t = "text";
  //         break;
  //       case "number":
  //         t = this.defaultNumberType;
  //         break;
  //       case "struct":
  //         t = "record[]";
  //         break;
  //     }
  //     fields.push(`${f.sqlOutputName} ${t || f.type}`);
  //   }
  //   if (needDistinctKey) {
  //     return `UNNEST((select ARRAY((SELECT ROW(gen_random_uuid()::text, ${fieldList
  //       .map((f) => f.sqlOutputName)
  //       .join(", ")}) FROM UNNEST(${source}) as ${alias}(${fields.join(
  //       ", "
  //     )}))))) as ${alias}(__distinct_key text, ${fields.join(", ")})`;
  //   } else {
  //     return `UNNEST(${source}) as ${alias}(${fields.join(", ")})`;
  //   }
  // }

  sqlUnnestAlias(
    source: string,
    alias: string,
    fieldList: DialectFieldList,
    needDistinctKey: boolean,
    isArray: boolean,
    _isInNestedPipeline: boolean
  ): string {
    if (isArray) {
      if (needDistinctKey) {
        return `LEFT JOIN UNNEST(ARRAY((SELECT jsonb_build_object('__row_id', row_number() over (), 'value', v) FROM JSONB_ARRAY_ELEMENTS(TO_JSONB(${source})) as v))) as ${alias} ON true`;
      } else {
        return `LEFT JOIN UNNEST(ARRAY((SELECT jsonb_build_object('value', v) FROM JSONB_ARRAY_ELEMENTS(TO_JSONB(${source})) as v))) as ${alias} ON true`;
      }
    } else if (needDistinctKey) {
      // return `UNNEST(ARRAY(( SELECT AS STRUCT GENERATE_UUID() as __distinct_key, * FROM UNNEST(${source})))) as ${alias}`;
      return `LEFT JOIN UNNEST(ARRAY((SELECT jsonb_build_object('__row_number', row_number() over())|| __xx::jsonb as b FROM  JSONB_ARRAY_ELEMENTS(${source}) __xx ))) as ${alias} ON true`;
    } else {
      // return `CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(${source}) as ${alias}`;
      return `LEFT JOIN JSONB_ARRAY_ELEMENTS(${source}) as ${alias} ON true`;
    }
  }

  sqlSumDistinctHashedKey(sqlDistinctKey: string): string {
    // return `('x' || MD5(${sqlDistinctKey}::varchar))::bit(64)::bigint::DECIMAL(65,0)  *18446744073709551616 + ('x' || SUBSTR(MD5(${sqlDistinctKey}::varchar),17))::bit(64)::bigint::DECIMAL(65,0)`;
    return `unhex(md5(${sqlDistinctKey}))`;
  } // todo

  sqlGenerateUUID(): string {
    return 'uuid()';
  }

  sqlRegexpMatch(df: RegexMatchExpr): string {
    return `${df.kids.expr.sql} RLIKE ${df.kids.regex.sql}`;
  }

  sqlFieldReference(
    parentAlias: string,
    parentType: FieldReferenceType,
    childName: string,
    childType: string
  ): string {
    if (childName === '__row_id') {
      return `(${parentAlias}->>'__row_id')`;
    }
    if (parentType !== 'table') {
      let ret = `JSONB_EXTRACT_PATH_TEXT(${parentAlias},'${childName}')`;
      switch (childType) {
        case 'string':
          break;
        case 'number':
          ret = `${ret}::double`;
          break;
        case 'struct':
        case 'array':
        case 'record':
        case 'array[record]':
          ret = `JSONB_EXTRACT_PATH(${parentAlias},'${childName}')`;
          break;
      }
      return ret;
    } else {
      const child = this.sqlMaybeQuoteIdentifier(childName);
      return `${parentAlias}.${child}`;
    }
  }

  sqlUnnestPipelineHead(
    isSingleton: boolean,
    sourceSQLExpression: string
  ): string {
    if (isSingleton) {
      return `UNNEST(ARRAY((SELECT ${sourceSQLExpression})))`;
    } else {
      return `JSONB_ARRAY_ELEMENTS(${sourceSQLExpression})`;
    }
  }

  sqlCreateFunction(id: string, funcText: string): string {
    return `CREATE FUNCTION ${id}(JSONB) RETURNS JSONB AS $$\n${indent(
      funcText
    )}\n$$ LANGUAGE SQL;\n`;
  }

  sqlCreateFunctionCombineLastStage(lastStageName: string): string {
    return `SELECT JSONB_AGG(__stage0) FROM ${lastStageName}\n`;
  }

  sqlFinalStage(lastStageName: string, _fields: string[]): string {
    // BRIAN changed
    return `SELECT to_json(struct(*)) AS row FROM ${lastStageName}`;
  }

  sqlSelectAliasAsStruct(alias: string): string {
    return `ROW(${alias})`; // todo
  }

  // The simple way to do this is to add a comment on the table
  //  with the expiration time. https://www.postgresql.org/docs/current/sql-comment.html
  //  and have a reaper that read comments.
  sqlCreateTableAsSelect(_tableName: string, _sql: string): string {
    throw new Error('Not implemented Yet');
  }

  sqlMeasureTimeExpr(df: MeasureTimeExpr): string {
    const from = df.kids.left;
    const to = df.kids.right;
    let lVal = from.sql;
    let rVal = to.sql;
    if (inSeconds[df.units]) {
      lVal = `UNIX_TIMESTAMP(${lVal})`;
      rVal = `UNIX_TIMESTAMP(${rVal})`;
      const duration = `${rVal}-${lVal}`;
      return df.units === 'second'
        ? `FLOOR(${duration})`
        : `FLOOR((${duration})/${inSeconds[df.units].toString()}.0)`;
    }
    throw new Error(`Unknown or unhandled databricks time unit: ${df.units}`);
  }

  sqlAlterTimeExpr(df: TimeDeltaExpr): string {
    let timeframe = df.units;
    let n = df.kids.delta.sql;
    if (timeframe === 'quarter') {
      timeframe = 'month';
      n = `${n}*3`;
    } else if (timeframe === 'week') {
      timeframe = 'day';
      n = `${n}*7`;
    }
    return `DATE_ADD(${df.kids.base.sql}, INTERVAL ${n} ${timeframe})`;
  }

  sqlSumDistinct(key: string, value: string, funcName: string): string {
    // In Spark SQL, we can use the same functions: concat, md5, substring, conv, etc.
    // Create a distinct key expression by converting the key to a string.
    const sqlDistinctKey = `concat(${key}, '')`;

    // Compute the hash key in two parts.
    // The first part: take the first 16 characters of the MD5 hash, convert from hexadecimal to decimal,
    // cast to DECIMAL(38,0) and multiply by 4294967296.
    const upperPart = `CAST(conv(substring(md5(${sqlDistinctKey}), 1, 16), 16, 10) AS DECIMAL(38,0)) * 4294967296`;

    // The second part: take the next 8 characters of the MD5 hash and convert them similarly.
    const lowerPart = `CAST(conv(substring(md5(${sqlDistinctKey}), 16, 8), 16, 10) AS DECIMAL(38,0))`;

    // The full hash key is the sum of both parts.
    const hashKey = `(${upperPart} + ${lowerPart})`;

    // Ensure the value is not null.
    const v = `COALESCE(${value}, 0)`;

    // The symmetric distinct SUM is computed as:
    //   SUM(DISTINCT (hashKey + value)) - SUM(DISTINCT hashKey)
    const sqlSum = `(SUM(DISTINCT ${hashKey} + ${v}) - SUM(DISTINCT ${hashKey}))`;

    // Return the appropriate SQL expression based on the aggregation function.
    if (funcName.toUpperCase() === 'SUM') {
      return sqlSum;
    } else if (funcName.toUpperCase() === 'AVG') {
      // For the AVG case, divide the sum by the distinct count of non-null keys.
      return `(${sqlSum}) / NULLIF(COUNT(DISTINCT CASE WHEN ${value} IS NOT NULL THEN ${key} END), 0)`;
    }

    throw new Error(`Unknown Symmetric Aggregate function ${funcName}`);
  }

  // TODO this does not preserve the types of the arguments, meaning we have to hack
  // around this in the definitions of functions that use this to cast back to the correct
  // type (from text). See the postgres implementation of stddev.
  sqlAggDistinct(
    key: string,
    values: string[],
    func: (valNames: string[]) => string
  ): string {
    return `(
    SELECT ${func(values.map((v, i) => `(a::json->>'f${i + 2}')`))} as value
    FROM (
      SELECT UNNEST(array_agg(distinct row_to_json(row(${key},${values.join(
        ','
      )}))::text)) a
    ) a
  )`;
  } //nesting

  sqlSampleTable(tableSQL: string, sample: Sampling | undefined): string {
    if (sample !== undefined) {
      if (isSamplingEnable(sample) && sample.enable) {
        sample = this.defaultSampling;
      }
      if (isSamplingRows(sample)) {
        return `(SELECT * FROM ${tableSQL} TABLESAMPLE SYSTEM_ROWS(${sample.rows}))`;
      } else if (isSamplingPercent(sample)) {
        return `(SELECT * FROM ${tableSQL} TABLESAMPLE SYSTEM (${sample.percent}))`;
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
    return "'" + literal.replace(/'/g, "''") + "'";
  }

  getDialectFunctionOverrides(): {
    [name: string]: DialectFunctionOverloadDef[];
  } {
    return expandOverrideMap(DATABRICKS_MALLOY_STANDARD_OVERLOADS);
  }

  malloyTypeToSQLType(malloyType: AtomicTypeDef): string {
    if (malloyType.type === 'number') {
      if (malloyType.numberType === 'integer') {
        return 'integer';
      } else {
        return 'double';
      }
    } else if (malloyType.type === 'string') {
      return 'string';
    }
    return malloyType.type;
  }

  sqlTypeToMalloyType(sqlType: string): LeafAtomicTypeDef {
    // Remove trailing params
    const baseSqlType = sqlType.match(/^([\w\s]+)/)?.at(0) ?? sqlType;
    return (
      databricksToMalloyTypes[baseSqlType.trim().toLowerCase()] ?? {
        type: 'sql native',
        rawType: sqlType,
      }
    );
  }

  castToString(expression: string): string {
    return `CAST(${expression} as STRING)`;
  }

  concat(...values: string[]): string {
    return 'CONCAT(' + values.join(',') + ')';
  }

  validateTypeName(sqlType: string): boolean {
    // Letters:              BIGINT
    // Numbers:              INT8
    // Spaces:               TIMESTAMP WITH TIME ZONE
    // Parentheses, Commas:  NUMERIC(5, 2)
    // Square Brackets:      INT64[]
    return sqlType.match(/^[A-Za-z\s(),[\]0-9]*$/) !== null;
  }

  sqlLiteralRecord(lit: RecordLiteralNode): string {
    const props: string[] = [];
    for (const [kName, kVal] of Object.entries(lit.kids)) {
      props.push(`'${kName}',${kVal.sql}`);
    }
    return `JSONB_BUILD_OBJECT(${props.join(', ')})`;
  }

  sqlLiteralArray(lit: ArrayLiteralNode): string {
    const array = lit.kids.values.map(val => val.sql);
    return 'JSONB_BUILD_ARRAY(' + array.join(',') + ')';
  }

  sqlMaybeQuoteIdentifier(ident: string): string {
    return `\`${ident}\``;
  }

  sqlNowExpr(): string {
    return 'CURRENT_TIMESTAMP()';
  }

  sqlTruncExpr(qi: QueryInfo, toTrunc: TimeTruncExpr): string {
    return `DATE_TRUNC('${toTrunc.units}', ${toTrunc.e.sql})`;
  }

  sqlTimeExtractExpr(qi: QueryInfo, from: TimeExtractExpr): string {
    return `EXTRACT(${from.units} FROM ${from.e.sql})`;
  }

  sqlLiteralTime(qi: QueryInfo, lt: TimeLiteralNode): string {
    const tz = qtz(qi);
    let ret = `'${lt.literal}'`;

    // If we have a timezone (either from the literal or query info)
    const targetTimeZone = lt.timezone ?? tz;
    if (targetTimeZone) {
      // For Databricks, we can use the from_utc_timestamp and to_utc_timestamp functions
      // to handle timezone conversions
      ret = `from_utc_timestamp(timestamp'${lt.literal}', '${targetTimeZone}')`;
    } else {
      ret = `timestamp${ret}`;
    }

    // If the type is date, convert the timestamp to a date
    if (TD.isDate(lt.typeDef)) {
      return `DATE(${ret})`;
    }

    return ret;
  }

  sqlCast(qi: QueryInfo, cast: TypecastExpr): string {
    const src = cast.e.sql || '';
    const {op, srcTypeDef, dstTypeDef, dstSQLType} = this.sqlCastPrep(cast);

    if (TD.eq(srcTypeDef, dstTypeDef)) {
      return src;
    }

    // Databricks doesn't have a TRY_CAST equivalent, so we'll need to handle safe casting differently
    if (cast.safe) {
      // For safe casting in Databricks, we can use a CASE statement to handle NULL and invalid casts
      return `CASE
        WHEN ${src} IS NULL THEN NULL
        WHEN TRY_CAST(${src} AS ${dstSQLType}) IS NOT NULL THEN CAST(${src} AS ${dstSQLType})
        ELSE NULL
      END`;
    }

    // Handle special timestamp and date casting cases
    if (op === 'timestamp::date') {
      return `DATE(${src})`;
    } else if (op === 'date::timestamp') {
      return `TIMESTAMP(${src})`;
    }

    // Default case - standard CAST
    return `CAST(${src} AS ${dstSQLType})`;
  }
}
