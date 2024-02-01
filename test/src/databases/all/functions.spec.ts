/* eslint-disable no-console */
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

import {RuntimeList, allDatabases} from '../../runtimes';
import {databasesFromEnvironmentOr} from '../../util';
import '../../util/db-jest-matchers';
import * as malloy from '@malloydata/malloy';

const runtimes = new RuntimeList(databasesFromEnvironmentOr(allDatabases));

function modelText(databaseName: string) {
  return `
source: aircraft_models is ${databaseName}.table('malloytest.aircraft_models') extend {
  primary_key: aircraft_model_code
}

source: aircraft is ${databaseName}.table('malloytest.aircraft') extend {
  primary_key: tail_num
  join_one: aircraft_models with aircraft_model_code
  measure: aircraft_count is count()
}

source: airports is ${databaseName}.table('malloytest.airports')

source: state_facts is ${databaseName}.table('malloytest.state_facts')

source: flights is ${databaseName}.table('malloytest.flights')

source: carriers is ${databaseName}.table('malloytest.carriers')
`;
}

const expressionModels = new Map<string, malloy.ModelMaterializer>();
runtimes.runtimeMap.forEach((runtime, databaseName) =>
  expressionModels.set(databaseName, runtime.loadModel(modelText(databaseName)))
);

expressionModels.forEach((expressionModel, databaseName) => {
  const funcTestGeneral = async (
    expr: string,
    type: 'group_by' | 'aggregate',
    expected:
      | {error: string; success?: undefined}
      | {success: string | boolean | number | null; error?: undefined}
  ) => {
    const run = async () => {
      return await expressionModel
        .loadQuery(
          `
      run: aircraft -> { ${type}: f is ${expr} }`
        )
        .run();
    };

    if (expected.success !== undefined) {
      const result = await run();
      expect(result.data.path(0, 'f').value).toBe(expected.success);
    } else {
      expect(run).rejects.toThrowError(expected.error);
    }
  };

  const funcTest = (expr: string, expexted: string | boolean | number | null) =>
    funcTestGeneral(expr, 'group_by', {success: expexted});

  const funcTestAgg = (
    expr: string,
    expexted: string | boolean | number | null
  ) => funcTestGeneral(expr, 'aggregate', {success: expexted});

  const funcTestMultiple = async (
    ...testCases: [string, string | boolean | number | null][]
  ) => {
    const run = async () => {
      return await expressionModel
        .loadQuery(
          `
      run: aircraft -> { ${testCases.map(
        (testCase, i) => `group_by: f${i} is ${testCase[0]}`
      )} }`
        )
        .run();
    };

    const result = await run();
    testCases.forEach((testCase, i) => {
      expect(result.data.path(0, `f${i}`).value).toBe(testCase[1]);
    });
  };

  describe('concat', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ["concat('foo', 'bar')", 'foobar'],
        ["concat(1, 'bar')", '1bar'],
        [
          "concat('cons', true)",
          databaseName === 'postgres' ? 'const' : 'construe',
        ],
        ["concat('foo', @2003)", 'foo2003-01-01'],
        [
          "concat('foo', @2003-01-01 12:00:00)",
          databaseName === 'bigquery'
            ? 'foo2003-01-01 12:00:00+00'
            : 'foo2003-01-01 12:00:00',
        ],
        // TODO Maybe implement consistent null behavior
        // ["concat('foo', null)", null],
        ['concat()', '']
      );
    });
  });

  describe('round', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ['round(1.2)', 1],
        // TODO Remove when we upgrade to DuckDB 0.8.X -- DuckDB has some bugs with rounding
        // that are fixed in 0.8.
        ...(databaseName === 'duckdb_wasm'
          ? []
          : ([['round(12.222, 1)', 12.2]] as [string, number][])),
        ['round(12.2, -1)', 10],
        ['round(null)', null],
        ['round(1, null)', null]
      );
    });
  });

  describe('floor', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ['floor(1.9)', 1],
        // TODO Remove when we upgrade to DuckDB 0.8.X -- DuckDB has some bugs with rounding
        // that are fixed in 0.8.
        ...(databaseName === 'duckdb_wasm'
          ? []
          : ([['floor(-1.9)', -2]] as [string, number][])),
        ['floor(null)', null]
      );
      await funcTest('floor(1.9)', 1);
    });
  });

  describe('ceil', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ['ceil(1.9)', 2],
        // TODO Remove when we upgrade to DuckDB 0.8.X -- DuckDB has some bugs with rounding
        // that are fixed in 0.8.
        ...(databaseName === 'duckdb_wasm'
          ? []
          : ([['ceil(-1.9)', -1]] as [string, number][])),
        ['ceil(null)', null]
      );
    });
  });

  describe('length', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(["length('foo')", 3], ['length(null)', null]);
    });
  });

  describe('lower', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(["lower('FoO')", 'foo'], ['lower(null)', null]);
    });
  });

  describe('upper', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(["upper('fOo')", 'FOO'], ['upper(null)', null]);
    });
  });

  describe('regexp_extract', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ["regexp_extract('I have a dog', r'd[aeiou]g')", 'dog'],
        ["regexp_extract(null, r'd[aeiou]g')", null],
        ["regexp_extract('foo', null)", null],
        ["regexp_extract('I have a d0g', r'd\\dg')", 'd0g']
      );
    });
  });

  describe('replace', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ["replace('aaaa', 'a', 'c')", 'cccc'],
        ["replace('aaaa', r'.', 'c')", 'cccc'],
        [
          "replace('axbxc', r'(a).(b).(c)', '\\\\0 - \\\\1 - \\\\2 - \\\\3')",
          databaseName === 'postgres' ? '\\0 - a - b - c' : 'axbxc - a - b - c',
        ],
        ["replace('aaaa', '', 'c')", 'aaaa'],
        ["replace(null, 'a', 'c')", null],
        ["replace('aaaa', null, 'c')", null],
        ["replace('aaaa', 'a', null)", null]
      );
    });
  });

  describe('substr', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ["substr('foo', 2)", 'oo'],
        ["substr('foo', 2, 1)", 'o'],
        ["substr('foo bar baz', -3)", 'baz'],
        ['substr(null, 1, 2)', null],
        ["substr('aaaa', null, 1)", null],
        ["substr('aaaa', 1, null)", null]
      );
    });
  });

  describe('raw function call', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ['floor(cbrt!(27)::number)', 3],
        ['floor(cbrt!number(27))', 3],
        ["substr('foo bar baz', -3)", 'baz'],
        ['substr(null, 1, 2)', null],
        ["substr('aaaa', null, 1)", null],
        ["substr('aaaa', 1, null)", null]
      );
    });
  });

  describe('stddev', () => {
    // TODO symmetric aggregates don't work with custom aggregate functions in BQ currently
    if (databaseName === 'bigquery') return;
    it(`works - ${databaseName}`, async () => {
      await funcTestAgg('round(stddev(aircraft_models.seats))', 29);
    });

    it(`works with struct - ${databaseName}`, async () => {
      await funcTestAgg(
        'round(aircraft_models.stddev(aircraft_models.seats))',
        41
      );
    });

    it(`works with implicit parameter - ${databaseName}`, async () => {
      await funcTestAgg('round(aircraft_models.seats.stddev())', 41);
    });

    it(`works with filter - ${databaseName}`, async () => {
      await funcTestAgg(
        'round(aircraft_models.seats.stddev() { where: 1 = 1 })',
        41
      );
      await funcTestAgg(
        'round(aircraft_models.seats.stddev() { where: aircraft_models.seats > 4 })',
        69
      );
    });
  });

  describe('row_number', () => {
    it(`works when the order by is a dimension  - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `run: state_facts -> {
          group_by: state
          calculate: row_num is row_number()
        }`
        )
        .run();
      expect(result.data.path(0, 'row_num').value).toBe(1);
      expect(result.data.path(1, 'row_num').value).toBe(2);
    });

    it(`works when the order by is a dimension in the other order  - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `run: state_facts -> {
            calculate: row_num is row_number()
            group_by: state
        }`
        )
        .run();
      expect(result.data.path(0, 'row_num').value).toBe(1);
      expect(result.data.path(1, 'row_num').value).toBe(2);
    });

    it(`works when the order by is a measure - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `run: state_facts -> {
          group_by: popular_name
          aggregate: c is count()
          calculate: row_num is row_number()
        }`
        )
        .run();
      expect(result.data.path(0, 'row_num').value).toBe(1);
      expect(result.data.path(1, 'row_num').value).toBe(2);
    });

    it(`works when the order by is a measure but there is no group by - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `run: state_facts -> {
            aggregate: c is count()
            calculate: row_num is row_number()
          }`
        )
        .run();
      expect(result.data.path(0, 'row_num').value).toBe(1);
    });

    it(`works inside nest - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `run: state_facts extend { join_one: airports on airports.state = state } -> {
            group_by: state
            nest: q is {
              group_by: airports.county
              calculate: row_num is row_number()
            }
          }`
        )
        .run();
      expect(result.data.path(0, 'q', 0, 'row_num').value).toBe(1);
      expect(result.data.path(0, 'q', 1, 'row_num').value).toBe(2);
      expect(result.data.path(1, 'q', 0, 'row_num').value).toBe(1);
      expect(result.data.path(1, 'q', 1, 'row_num').value).toBe(2);
    });

    test(`works outside nest, but with a nest nearby - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `run: state_facts -> {
            group_by: state
            calculate: row_num is row_number()
            nest: nested is {
              group_by: state
            }
          }`
        )
        .run();
      expect(result.data.path(0, 'row_num').value).toBe(1);
      expect(result.data.path(1, 'row_num').value).toBe(2);
    });
  });

  describe('rank', () => {
    it(`works ordered by dimension - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `run: state_facts -> {
            group_by:
              state,
              births_ballpark is ceil(births / 1000000) * 1000000
            order_by: births_ballpark desc
            calculate: births_ballpark_rank is rank()
            limit: 20
          }`
        )
        .run({rowLimit: 20});
      expect(result.data.path(0, 'births_ballpark_rank').value).toBe(1);
      expect(result.data.path(1, 'births_ballpark_rank').value).toBe(2);
      expect(result.data.path(8, 'births_ballpark_rank').value).toBe(9);
      expect(result.data.path(9, 'births_ballpark_rank').value).toBe(9);
      expect(result.data.path(10, 'births_ballpark_rank').value).toBe(9);
      expect(result.data.path(11, 'births_ballpark_rank').value).toBe(12);
    });

    it(`works ordered by aggregate - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `run: state_facts -> {
            group_by: first_letter is substr(state, 1, 1)
            aggregate: states_with_first_letter_ish is round(count() / 2) * 2
            calculate: r is rank()
          }`
        )
        .run();
      expect(result.data.path(0, 'r').value).toBe(1);
      expect(result.data.path(1, 'r').value).toBe(1);
      expect(result.data.path(2, 'r').value).toBe(3);
      expect(result.data.path(3, 'r').value).toBe(3);
    });
  });

  describe('lag', () => {
    it(`works with one param - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `run: state_facts -> {
          group_by: state
          calculate: prev_state is lag(state)
        }`
        )
        .run();
      expect(result.data.path(0, 'state').value).toBe('AK');
      expect(result.data.path(0, 'prev_state').value).toBe(null);
      expect(result.data.path(1, 'prev_state').value).toBe('AK');
      expect(result.data.path(1, 'state').value).toBe('AL');
      expect(result.data.path(2, 'prev_state').value).toBe('AL');
    });

    it(`works with expression field - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `run: state_facts -> {
          group_by: lower_state is lower(state)
          calculate: prev_state is lag(lower_state)
        }`
        )
        .run();
      expect(result.data.path(0, 'lower_state').value).toBe('ak');
      expect(result.data.path(0, 'prev_state').value).toBe(null);
      expect(result.data.path(1, 'prev_state').value).toBe('ak');
      expect(result.data.path(1, 'lower_state').value).toBe('al');
      expect(result.data.path(2, 'prev_state').value).toBe('al');
    });

    it(`works with expression - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `run: state_facts -> {
          group_by: state
          calculate: prev_state is lag(lower(state))
        }`
        )
        .run();
      expect(result.data.path(0, 'state').value).toBe('AK');
      expect(result.data.path(0, 'prev_state').value).toBe(null);
      expect(result.data.path(1, 'prev_state').value).toBe('ak');
      expect(result.data.path(1, 'state').value).toBe('AL');
      expect(result.data.path(2, 'prev_state').value).toBe('al');
    });

    it(`works with field, ordering by expression field - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `run: state_facts -> {
          group_by: lower_state is lower(state)
          aggregate: c is count()
          order_by: lower_state
          calculate: prev_count is lag(c)
        }`
        )
        .run();
      expect(result.data.path(0, 'lower_state').value).toBe('ak');
      expect(result.data.path(0, 'prev_count').value).toBe(null);
      expect(result.data.path(1, 'prev_count').value).toBe(1);
      expect(result.data.path(1, 'lower_state').value).toBe('al');
      expect(result.data.path(2, 'prev_count').value).toBe(1);
    });

    it(`works with offset - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `run: state_facts -> {
          group_by: state
          calculate: prev_prev_state is lag(state, 2)
        }`
        )
        .run();
      expect(result.data.path(0, 'state').value).toBe('AK');
      expect(result.data.path(0, 'prev_prev_state').value).toBe(null);
      expect(result.data.path(1, 'prev_prev_state').value).toBe(null);
      expect(result.data.path(2, 'prev_prev_state').value).toBe('AK');
      expect(result.data.path(1, 'state').value).toBe('AL');
      expect(result.data.path(3, 'prev_prev_state').value).toBe('AL');
    });

    it(`works with default value - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `run: state_facts -> {
          group_by: state
          calculate: prev_state is lag(state, 1, 'NONE')
        }`
        )
        .run();
      expect(result.data.path(0, 'prev_state').value).toBe('NONE');
    });

    it(`works with now as the default value - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `
          run: state_facts -> {
            group_by: state
            calculate: lag_val is lag(@2011-11-11 11:11:11, 1, now).year = now.year
          }`
        )
        .run();
      expect(result.data.path(0, 'lag_val').value).toBe(true);
      expect(result.data.path(1, 'lag_val').value).toBe(false);
    });
  });

  describe('output field in calculate', () => {
    it(`output field referenceable in calculate - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `run: aircraft -> {
            group_by: s is aircraft_models.seats
            calculate: a is lag(s)
          }`
        )
        .run();
      expect(result.data.path(1, 'a').value).toBe(
        result.data.path(0, 's').value
      );
    });
  });

  describe('first_value', () => {
    test(`works in nest - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `
          run: aircraft -> {
            group_by: state
            where: state != null
            nest: by_county is {
              limit: 2
              group_by: county
              aggregate: aircraft_count
              calculate: first_count is first_value(count())
            }
          }`
        )
        .run();
      expect(result.data.path(0, 'by_county', 1, 'first_count').value).toBe(
        result.data.path(0, 'by_county', 0, 'aircraft_count').value
      );
      expect(result.data.path(1, 'by_county', 1, 'first_count').value).toBe(
        result.data.path(1, 'by_county', 0, 'aircraft_count').value
      );
    });
    it(`works outside nest - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `
          run: state_facts -> {
            group_by: state, births
            order_by: births desc
            calculate: most_births is first_value(births)
          }`
        )
        .run();
      const firstBirths = result.data.path(0, 'births').value;
      expect(result.data.path(0, 'most_births').value).toBe(firstBirths);
      expect(result.data.path(1, 'most_births').value).toBe(firstBirths);
    });
    it(`works with an aggregate which is not in the query - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `
          run: airports extend { measure: airport_count is count() } -> {
            group_by: state
            where: state != null
            calculate: prev_airport_count is lag(airport_count)
          }`
        )
        .run();
      expect(result.data.path(0, 'prev_airport_count').value).toBe(null);
      expect(result.data.path(1, 'prev_airport_count').value).toBe(608);
      expect(result.data.path(2, 'prev_airport_count').value).toBe(260);
    });
    it(`works with a localized aggregate - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `
          run: aircraft -> {
            group_by: aircraft_models.seats,
            calculate: prev_sum_of_seats is lag(aircraft_models.seats.sum())
          }`
        )
        .run();
      expect(result.data.path(0, 'prev_sum_of_seats').value).toBe(null);
      expect(result.data.path(1, 'prev_sum_of_seats').value).toBe(0);
      expect(result.data.path(2, 'prev_sum_of_seats').value).toBe(230);
    });
  });

  describe('trunc', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ['trunc(1.9)', 1],
        // TODO Remove when we upgrade to DuckDB 0.8.X -- DuckDB has some bugs with rounding
        // that are fixed in 0.8.
        ...(databaseName === 'duckdb_wasm'
          ? []
          : ([['trunc(-1.9)', -1]] as [string, number][])),
        ['trunc(12.29, 1)', 12.2],
        ['trunc(19.2, -1)', 10],
        ['trunc(null)', null],
        ['trunc(1, null)', null]
      );
    });
  });
  describe('log', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ['log(10, 10)', 1],
        ['log(100, 10)', 2],
        ['log(32, 2)', 5],
        ['log(null, 2)', null],
        ['log(10, null)', null]
      );
    });
  });
  describe('ln', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ['ln(exp(1))', 1],
        ['ln(exp(2))', 2],
        ['ln(null)', null]
      );
    });
  });
  describe('exp', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ['exp(0)', 1],
        ['ln(exp(1))', 1],
        ['exp(null)', null]
      );
    });
  });

  // TODO trig functions could have more exhaustive tests -- these are mostly just here to
  // ensure they exist
  describe('cos', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(['cos(0)', 1], ['cos(null)', null]);
    });
  });
  describe('acos', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(['acos(1)', 0], ['acos(null)', null]);
    });
  });

  describe('sin', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(['sin(0)', 0], ['sin(null)', null]);
    });
  });
  describe('asin', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(['asin(0)', 0], ['asin(null)', null]);
    });
  });

  describe('tan', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(['tan(0)', 0], ['tan(null)', null]);
    });
  });
  describe('atan', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(['atan(0)', 0], ['atan(null)', null]);
    });
  });
  describe('atan2', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ['atan2(0, 1)', 0],
        ['atan2(null, 1)', null],
        ['atan2(1, null)', null]
      );
    });
  });
  describe('sqrt', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ['sqrt(9)', 3],
        ['sqrt(6.25)', 2.5],
        ['sqrt(null)', null]
      );
    });
  });
  describe('pow', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ['pow(2, 3)', 8],
        ['pow(null, 3)', null],
        ['pow(2, null)', null]
      );
    });
  });
  describe('abs', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ['abs(-3)', 3],
        ['abs(3)', 3],
        ['abs(null)', null]
      );
    });
  });
  describe('sign', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ['sign(100)', 1],
        ['sign(-2)', -1],
        ['sign(0)', 0],
        ['sign(null)', null]
      );
    });
  });
  describe('is_inf', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ["is_inf('+inf'::number)", true],
        ['is_inf(100)', false],
        ['is_inf(null)', false]
      );
    });
  });
  describe('is_nan', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ["is_nan('NaN'::number)", true],
        ['is_nan(100)', false],
        ['is_nan(null)', false]
      );
    });
  });
  describe('greatest', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ['greatest(1, 10, -100)', 10],
        ['greatest(@2003, @2004, @1994) = @2004', true],
        [
          'greatest(@2023-05-26 11:58:00, @2023-05-26 11:59:00) = @2023-05-26 11:59:00',
          true,
        ],
        ["greatest('a', 'b')", 'b'],
        ['greatest(1, null, 0)', null],
        ['greatest(null, null)', null]
      );
    });
  });
  describe('least', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ['least(1, 10, -100)', -100],
        ['least(@2003, @2004, @1994) = @1994', true],
        [
          'least(@2023-05-26 11:58:00, @2023-05-26 11:59:00) = @2023-05-26 11:58:00',
          true,
        ],
        ["least('a', 'b')", 'a'],
        ['least(1, null, 0)', null],
        ['least(null, null)', null]
      );
    });
  });
  describe('div', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ['div(3, 2)', 1],
        ['div(null, 2)', null],
        ['div(2, null)', null]
      );
    });
  });
  describe('strpos', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ["strpos('123456789', '3')", 3],
        ["strpos('123456789', '0')", 0],
        ["strpos(null, '0')", null],
        ["strpos('123456789', null)", null]
      );
    });
  });
  describe('starts_with', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ["starts_with('hello world', 'hello')", true],
        ["starts_with('hello world', 'world')", false],
        ["starts_with(null, 'world')", false],
        ["starts_with('hello world', null)", false]
      );
    });
  });
  describe('ends_with', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ["ends_with('hello world', 'world')", true],
        ["ends_with('hello world', 'hello')", false],
        ["ends_with(null, 'world')", false],
        ["ends_with('hello world', null)", false]
      );
    });
  });
  describe('trim', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ["trim('  keep this  ')", 'keep this'],
        ["trim('_ _keep_this_ _', '_ ')", 'keep_this'],
        ["trim(' keep everything ', '')", ' keep everything '],
        ["trim('null example', null)", null],
        ["trim(null, 'a')", null],
        ['trim(null)', null]
      );
    });
  });
  describe('ltrim', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ["ltrim('  keep this ->  ')", 'keep this ->  '],
        ["ltrim('_ _keep_this -> _ _', '_ ')", 'keep_this -> _ _'],
        ["ltrim(' keep everything ', '')", ' keep everything '],
        ["ltrim('null example', null)", null],
        ["ltrim(null, 'a')", null],
        ['ltrim(null)', null]
      );
    });
  });
  describe('rtrim', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ["rtrim('  <- keep this  ')", '  <- keep this'],
        ["rtrim('_ _ <- keep_this_ _', '_ ')", '_ _ <- keep_this'],
        ["rtrim(' keep everything ', '')", ' keep everything '],
        ["rtrim('null example', null)", null],
        ["rtrim(null, 'a')", null],
        ['rtrim(null)', null]
      );
    });
  });
  describe('rand', () => {
    it(`is usually not the same value - ${databaseName}`, async () => {
      // There are around a billion values that rand() can be, so if this
      // test fails, most likely something is broken. Otherwise, you're the lucky
      // one in a billion!
      await funcTest('rand() = rand()', false);
    });
  });
  describe('pi', () => {
    it(`is pi - ${databaseName}`, async () => {
      await funcTest('abs(pi() - 3.141592653589793) < 0.0000000000001', true);
    });
  });

  describe('byte_length', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ["byte_length('hello')", 5],
        ["byte_length('©')", 2],
        ['byte_length(null)', null]
      );
    });
  });
  describe('ifnull', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ["ifnull('a', 'b')", 'a'],
        ["ifnull(null, 'b')", 'b'],
        ["ifnull('a', null)", 'a'],
        ['ifnull(null, null)', null]
      );
    });
  });
  describe('coalesce', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ["coalesce('a')", 'a'],
        ["coalesce('a', 'b')", 'a'],
        ["coalesce(null, 'a', 'b')", 'a'],
        ["coalesce(null, 'b')", 'b'],
        ["coalesce('a', null)", 'a'],
        ['coalesce(null, null)', null],
        ['coalesce(null)', null]
      );
    });
  });
  describe('nullif', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ["nullif('a', 'a')", null],
        ["nullif('a', 'b')", 'a'],
        ["nullif('a', null)", 'a'],
        ['nullif(null, null)', null],
        ['nullif(null, 2)', null]
      );
    });
  });
  describe('chr', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ['chr(65)', 'A'],
        ['chr(255)', 'ÿ'],
        ['chr(null)', null],
        // BigQuery's documentation says that `chr(0)` returns the empty string, but it doesn't,
        // it actually returns the null character. We generate code so that it does this.
        ['chr(0)', '']
      );
    });
  });
  describe('ascii', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ["ascii('A')", 65],
        ["ascii('ABC')", 65],
        ["ascii('')", 0],
        ['ascii(null)', null]
      );
    });
  });
  describe('unicode', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ["unicode('A')", 65],
        ["unicode('â')", 226],
        ["unicode('âBC')", 226],
        ["unicode('')", 0],
        ['unicode(null)', null]
      );
    });
  });

  describe('repeat', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ["repeat('foo', 0)", ''],
        ["repeat('foo', 1)", 'foo'],
        ["repeat('foo', 2)", 'foofoo'],
        ['repeat(null, 2)', null],
        ["repeat('foo', null)", null]
      );
    });
    // TODO how does a user do this: the second argument needs to be an integer, but floor doesn't cast to "integer" type.
    it.skip(`works floor decimal - ${databaseName}`, async () => {
      await funcTest("repeat('foo', floor(2.5))", 'foofoo');
    });
    // undefined behavior when negative, undefined behavior (likely error) when non-integer
  });
  describe('reverse', () => {
    it(`works - ${databaseName}`, async () => {
      await funcTestMultiple(
        ["reverse('foo')", 'oof'],
        ["reverse('')", ''],
        ['reverse(null)', null]
      );
    });
  });

  describe('lead', () => {
    it(`works with one param - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `run: state_facts -> {
          group_by: state
          calculate: next_state is lead(state)
        }`
        )
        .run();
      expect(result.data.path(0, 'state').value).toBe('AK');
      expect(result.data.path(0, 'next_state').value).toBe('AL');
      expect(result.data.path(1, 'state').value).toBe('AL');
    });

    it(`works with offset - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `run: state_facts -> {
          group_by: state
          calculate: next_next_state is lead(state, 2)
        }`
        )
        .run();
      expect(result.data.path(0, 'state').value).toBe('AK');
      expect(result.data.path(0, 'next_next_state').value).toBe('AR');
      expect(result.data.path(1, 'next_next_state').value).toBe('AZ');
      expect(result.data.path(1, 'state').value).toBe('AL');
      expect(result.data.path(2, 'state').value).toBe('AR');
      expect(result.data.path(3, 'state').value).toBe('AZ');
    });

    it(`works with default value - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `run: state_facts -> { select: *; limit: 10 } -> {
          group_by: state
          calculate: next_state is lead(state, 1, 'NONE')
        }`
        )
        .run();
      expect(result.data.path(9, 'next_state').value).toBe('NONE');
    });
  });
  describe('last_value', () => {
    it(`works - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `
          run: state_facts -> {
            group_by: state, births
            order_by: births desc
            calculate: least_births is last_value(births)
          }`
        )
        .run({rowLimit: 100});
      const numRows = result.data.toObject().length;
      const lastBirths = result.data.path(numRows - 1, 'births').value;
      expect(result.data.path(0, 'least_births').value).toBe(lastBirths);
      expect(result.data.path(1, 'least_births').value).toBe(lastBirths);
    });
  });
  describe('avg_moving', () => {
    it(`works - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `
          run: state_facts -> {
            group_by: state, births
            order_by: births desc
            calculate: rolling_avg is avg_moving(births, 2)
          }`
        )
        .run({rowLimit: 100});
      const births0 = result.data.path(0, 'births').number.value;
      const births1 = result.data.path(1, 'births').number.value;
      const births2 = result.data.path(2, 'births').number.value;
      const births3 = result.data.path(3, 'births').number.value;
      expect(result.data.path(0, 'rolling_avg').number.value).toBe(births0);
      expect(Math.floor(result.data.path(1, 'rolling_avg').number.value)).toBe(
        Math.floor((births0 + births1) / 2)
      );
      expect(Math.floor(result.data.path(2, 'rolling_avg').number.value)).toBe(
        Math.floor((births0 + births1 + births2) / 3)
      );
      expect(Math.floor(result.data.path(3, 'rolling_avg').number.value)).toBe(
        Math.floor((births1 + births2 + births3) / 3)
      );
    });

    it(`works forward - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `
          run: state_facts -> { select: *; limit: 3 } -> {
            group_by: state, births
            order_by: births desc
            calculate: rolling_avg is avg_moving(births, 0, 2)
          }`
        )
        .run({rowLimit: 100});
      const births0 = result.data.path(0, 'births').number.value;
      const births1 = result.data.path(1, 'births').number.value;
      const births2 = result.data.path(2, 'births').number.value;
      expect(Math.floor(result.data.path(0, 'rolling_avg').number.value)).toBe(
        Math.floor((births0 + births1 + births2) / 3)
      );
      expect(Math.floor(result.data.path(1, 'rolling_avg').number.value)).toBe(
        Math.floor((births1 + births2) / 2)
      );
      expect(result.data.path(2, 'rolling_avg').number.value).toBe(births2);
    });
  });
  describe('min, max, sum / window, cumulative', () => {
    it(`works - ${databaseName}`, async () => {
      const result = await expressionModel
        .loadQuery(
          `
          run: state_facts -> { select: *; limit: 5 } -> {
            group_by: state, births
            order_by: births asc
            calculate: min_c is min_cumulative(births)
            calculate: max_c is max_cumulative(births)
            calculate: sum_c is sum_cumulative(births)
            calculate: min_w is min_window(births)
            calculate: max_w is max_window(births)
            calculate: sum_w is sum_window(births)
          }`
        )
        .run({rowLimit: 100});
      const births0 = result.data.path(0, 'births').number.value;
      const births1 = result.data.path(1, 'births').number.value;
      const births2 = result.data.path(2, 'births').number.value;
      const births3 = result.data.path(3, 'births').number.value;
      const births4 = result.data.path(4, 'births').number.value;
      const births = [births0, births1, births2, births3, births4];
      for (let r = 0; r < 5; r++) {
        expect(result.data.path(r, 'min_c').number.value).toBe(births0);
        expect(result.data.path(r, 'max_c').number.value).toBe(births[r]);
        expect(result.data.path(r, 'sum_c').number.value).toBe(
          births.slice(0, r + 1).reduce((a, b) => a + b)
        );
        expect(result.data.path(r, 'min_w').number.value).toBe(births0);
        expect(result.data.path(r, 'max_w').number.value).toBe(births4);
        expect(result.data.path(r, 'sum_w').number.value).toBe(
          births.reduce((a, b) => a + b)
        );
      }
    });
  });
});

describe.each(runtimes.runtimeList)('%s', (databaseName, runtime) => {
  const expressionModel = runtime.loadModel(modelText(databaseName));

  describe('string_agg', () => {
    it(`works no order by - ${databaseName}`, async () => {
      expect(`run: aircraft -> {
        where: name = 'RUTHERFORD PAT R JR'
        aggregate: f is string_agg(name)
      }`).malloyResultMatches(expressionModel, {f: 'RUTHERFORD PAT R JR'});
    });

    it(`works with dotted shortcut - ${databaseName}`, async () => {
      expect(`run: aircraft -> {
        where: name = 'RUTHERFORD PAT R JR'
        aggregate: f is name.string_agg()
      }`).malloyResultMatches(expressionModel, {f: 'RUTHERFORD PAT R JR'});
    });

    it(`works with order by field - ${databaseName}`, async () => {
      expect(`##! experimental { function_order_by }
      run: aircraft -> {
        where: name ~ r'.*RUTHERFORD.*'
        aggregate: f is string_agg(name, ',') {
          order_by: name
        }
      }`).malloyResultMatches(expressionModel, {
        f: 'RUTHERFORD JAMES C,RUTHERFORD PAT R JR',
      });
    });

    it(`works with multiple order_bys - ${databaseName}`, async () => {
      expect(`##! experimental { function_order_by }
      run: aircraft -> {
        where: name ~ r'.*RUTHERFORD.*'
        aggregate: f is string_agg(name, ',') {
          order_by: city, name
        }
      }`).malloyResultMatches(expressionModel, {
        f: 'RUTHERFORD PAT R JR,RUTHERFORD JAMES C',
      });
    });

    it(`works with order by expression - ${databaseName}`, async () => {
      expect(`##! experimental { function_order_by }
      run: aircraft -> {
        where: name ~ r'.*FLY.*'
        group_by: name
        order_by: name desc
        limit: 3
      } -> {
        aggregate: f is string_agg(name, ',') {
          order_by: length(name)
        }
      }`).malloyResultMatches(expressionModel, {
        f: 'YANKEE FLYING CLUB INC,WESTCHESTER FLYING CLUB,WILSON FLYING SERVICE INC',
      });
    });

    it(`works with order by join expression - ${databaseName}`, async () => {
      expect(`##! experimental { function_order_by }
      run: aircraft -> {
        where: name ~ r'.*ADVENTURE.*'
        aggregate: f is string_agg(name, ',') { order_by: aircraft_models.model }
      }`).malloyResultMatches(expressionModel, {
        f: 'ADVENTURE INC,SEA PLANE ADVENTURE INC,A BALLOON ADVENTURES ALOFT,A AERONAUTICAL ADVENTURE INC',
      });
    });

    it(`works with order asc - ${databaseName}`, async () => {
      expect(`##! experimental { function_order_by }
      run: aircraft -> {
        where: name ~ r'.*FLY.*'
        group_by: name
        order_by: name desc
        limit: 3
      } -> {
        aggregate: f is string_agg(name, ',') { order_by: name asc }
      }`).malloyResultMatches(expressionModel, {
        f: 'WESTCHESTER FLYING CLUB,WILSON FLYING SERVICE INC,YANKEE FLYING CLUB INC',
      });
    });

    it(`works with order desc - ${databaseName}`, async () => {
      expect(`##! experimental { function_order_by }
      run: aircraft -> {
        where: name ~ r'.*FLY.*'
        group_by: name
        order_by: name desc
        limit: 3
      } -> {
        aggregate: f is string_agg(name, ',') { order_by: name desc }
      }`).malloyResultMatches(expressionModel, {
        f: 'YANKEE FLYING CLUB INC,WILSON FLYING SERVICE INC,WESTCHESTER FLYING CLUB',
      });
    });

    it(`works with fanout - ${databaseName}`, async () => {
      expect(`
      run: state_facts extend { join_many:
        state_facts2 is ${databaseName}.table('malloytest.state_facts')
          on state_facts2.state = state
      } -> {
        aggregate: c is state_facts2.count()
        aggregate: s is string_agg(state) {
          order_by: popular_name
        }
      }`).malloyResultMatches(expressionModel, {
        s: 'MN,IA,LA,AR,IN,ME,MT,AL,NC,AZ,OH,WY,MA,OK,CO,NY,KY,HI,RI,CA,PA,NJ,TX,CT,NV,NM,FL,GA,MO,KS,TN,IL,WV,MS,SC,DC,ID,NE,VA,UT,NH,MD,AK,OR,SD,WA,MI,VT,WI,DE,ND',
        c: 51,
      });
    });

    it(`works with limit - ${databaseName}`, async () => {
      const query = `##! experimental { function_order_by aggregate_limit }
      run: aircraft -> {
          where: name ~ r'.*FLY.*'
          group_by: name
          order_by: name desc
          limit: 3
        } -> {
          aggregate: f is string_agg(name, ',') {
            order_by: name desc
            limit: 2
          }
        }`;
      if (databaseName === 'bigquery') {
        expect(query).malloyResultMatches(expressionModel, {
          f: 'YANKEE FLYING CLUB INC,WILSON FLYING SERVICE INC',
        });
      } else {
        await expect(expressionModel.loadQuery(query).run()).rejects.toThrow(
          'Function string_agg does not support limit'
        );
      }
    });
  });

  describe('string_agg_distinct', () => {
    it(`actually distincts - ${databaseName}`, async () => {
      expect(`##! experimental { function_order_by }
        source: aircraft is ${databaseName}.table('malloytest.aircraft') extend {
          primary_key: tail_num
        }

        source: aircraft_models is ${databaseName}.table('malloytest.aircraft_models') extend {
          primary_key: aircraft_model_code
          join_many: aircraft on aircraft_model_code = aircraft.aircraft_model_code
        }

        run: aircraft_models -> {
          where: aircraft.name = 'RAYTHEON AIRCRAFT COMPANY' | 'FOWLER IRA R DBA'
          aggregate: f_dist is aircraft.name.string_agg_distinct() { order_by: aircraft.name }
          aggregate: f_all is aircraft.name.string_agg() { order_by: aircraft.name }
      }`).malloyResultMatches(runtime, {
        f_dist: 'FOWLER IRA R DBA,RAYTHEON AIRCRAFT COMPANY',
        f_all:
          'FOWLER IRA R DBA,FOWLER IRA R DBA,RAYTHEON AIRCRAFT COMPANY,RAYTHEON AIRCRAFT COMPANY',
      });
    });

    it(`works no order by - ${databaseName}`, async () => {
      expect(`run: aircraft -> {
        where: name = 'RUTHERFORD PAT R JR'
        aggregate: f is string_agg_distinct(name)
      }`).malloyResultMatches(expressionModel, {
        f: 'RUTHERFORD PAT R JR',
      });
    });

    it(`works with dotted shortcut - ${databaseName}`, async () => {
      expect(`run: aircraft -> {
        where: name = 'RUTHERFORD PAT R JR'
        aggregate: f is name.string_agg_distinct()
      }`).malloyResultMatches(expressionModel, {
        f: 'RUTHERFORD PAT R JR',
      });
    });

    it(`works with order by field - ${databaseName}`, async () => {
      expect(`##! experimental { function_order_by }
      run: aircraft -> {
        where: name ~ r'.*RUTHERFORD.*'
        aggregate: f is string_agg_distinct(name, ',') {
          order_by: name
        }
      }`).malloyResultMatches(expressionModel, {
        f: 'RUTHERFORD JAMES C,RUTHERFORD PAT R JR',
      });
    });

    // TODO there is a requirement (at least in BQ that the order_by: must be the same as the first argument
    // when using distinct)

    it(`works with order asc - ${databaseName}`, async () => {
      expect(`##! experimental { function_order_by }
      run: aircraft -> {
        where: name ~ r'.*FLY.*'
        group_by: name
        order_by: name desc
        limit: 3
      } -> {
        aggregate: f is string_agg_distinct(name, ',') { order_by: name asc }
      }`).malloyResultMatches(expressionModel, {
        f: 'WESTCHESTER FLYING CLUB,WILSON FLYING SERVICE INC,YANKEE FLYING CLUB INC',
      });
    });

    it(`works with order desc - ${databaseName}`, async () => {
      expect(`##! experimental { function_order_by }
      run: aircraft -> {
        where: name ~ r'.*FLY.*'
        group_by: name
        order_by: name desc
        limit: 3
      } -> {
        aggregate: f is string_agg_distinct(name, ',') { order_by: name desc }
      }`).malloyResultMatches(expressionModel, {
        f: 'YANKEE FLYING CLUB INC,WILSON FLYING SERVICE INC,WESTCHESTER FLYING CLUB',
      });
    });

    it(`works with limit - ${databaseName}`, async () => {
      const query = `##! experimental { function_order_by aggregate_limit }
        run: aircraft -> {
          where: name ~ r'.*FLY.*'
          group_by: name
          order_by: name desc
          limit: 3
        } -> {
          aggregate: f is string_agg_distinct(name, ',') {
            order_by: name desc
            limit: 2
          }
        }`;
      if (databaseName === 'bigquery') {
        expect(query).malloyResultMatches(expressionModel, {
          f: 'YANKEE FLYING CLUB INC,WILSON FLYING SERVICE INC',
        });
      } else {
        await expect(expressionModel.loadQuery(query).run()).rejects.toThrow(
          'Function string_agg_distinct does not support limit'
        );
      }
    });
  });

  describe('partition_by', () => {
    it(`works - ${databaseName}`, async () => {
      expect(`##! experimental { function_order_by partition_by }
      run: flights -> {
        group_by:
          yr is year(dep_time)
          qtr is quarter(dep_time)

        aggregate:
          qtr_flights is count()

        calculate:
          last_yr_qtr_flights is lag(qtr_flights) {
            partition_by: qtr
            order_by: yr asc
          }
        order_by: yr, qtr
        where: dep_time < @2002
      }`).malloyResultMatches(expressionModel, [
        {yr: 2000, qtr: 1, qtr_flights: 12148, last_yr_qtr_flights: null},
        {yr: 2000, qtr: 2, qtr_flights: 11599, last_yr_qtr_flights: null},
        {yr: 2000, qtr: 3, qtr_flights: 12075, last_yr_qtr_flights: null},
        {yr: 2000, qtr: 4, qtr_flights: 11320, last_yr_qtr_flights: null},
        {yr: 2001, qtr: 1, qtr_flights: 11612, last_yr_qtr_flights: 12148},
        {yr: 2001, qtr: 2, qtr_flights: 13186, last_yr_qtr_flights: 11599},
        {yr: 2001, qtr: 3, qtr_flights: 12663, last_yr_qtr_flights: 12075},
        {yr: 2001, qtr: 4, qtr_flights: 11714, last_yr_qtr_flights: 11320},
      ]);
    });

    it(`works with multiple order_bys - ${databaseName}`, async () => {
      expect(`##! experimental { function_order_by partition_by }
      run: aircraft -> {
        where: name =
          "UNITED AIR LINES INC"
          | "FEDERAL EXPRESS CORP"
          | "AMERICAN AIRLINES INC"
          | "CESSNA AIRCRAFT COMPANY"
        group_by: name
        calculate:
          # label="Rank by model count then seat count"
          r is rank() {
            order_by:
              aircraft_models.count() desc,
              aircraft_models.seats.sum() desc
          }
        order_by: name
      }`).malloyResultMatches(expressionModel, [
        {name: 'AMERICAN AIRLINES INC', r: 3},
        {name: 'CESSNA AIRCRAFT COMPANY', r: 4},
        {name: 'FEDERAL EXPRESS CORP', r: 2},
        {name: 'UNITED AIR LINES INC', r: 1},
      ]);
    });
  });
});

afterAll(async () => {
  await runtimes.closeAll();
});
