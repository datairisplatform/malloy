/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import {RuntimeList, allDatabases} from '../../runtimes';
import {databasesFromEnvironmentOr} from '../../util';
import '../../util/db-jest-matchers';

const runtimes = new RuntimeList(databasesFromEnvironmentOr(allDatabases));

afterAll(async () => {
  await runtimes.closeAll();
});

runtimes.runtimeMap.forEach((runtime, databaseName) => {
  it(`basic composite usage - ${databaseName}`, async () => {
    await expect(`
      ##! experimental.composite_sources
      source: state_facts is ${databaseName}.table('malloytest.state_facts')
      source: x is compose(state_facts, state_facts extend { dimension: foo is 1 })
      run: x -> { group_by: foo }
    `).malloyResultMatches(runtime, {foo: 1});
  });
  it(`composite source used in join - ${databaseName}`, async () => {
    await expect(`
      ##! experimental.composite_sources
      source: state_facts is ${databaseName}.table('malloytest.state_facts')
      source: x is compose(state_facts, state_facts extend { dimension: foo is 1 })
      source: y is ${databaseName}.table('malloytest.state_facts') extend {
        join_one: x on x.state = state
      }
      run: y -> { group_by: x.foo }
    `).malloyResultMatches(runtime, {foo: 1});
  });
  it(`composite used in join on - ${databaseName}`, async () => {
    await expect(`
      ##! experimental.composite_sources
      source: state_facts is ${databaseName}.table('malloytest.state_facts')
      source: x is compose(state_facts, state_facts extend { dimension: state_copy is state })
      source: y is ${databaseName}.table('malloytest.state_facts') extend {
        // Join california by testing state copy;
        // composite field usage is in join on, so the composite source with state_copy should
        // be selected whenever this join is used
        join_one: ca is x on ca.state_copy = 'CA'
      }
      run: y -> { group_by: ca.state; where: state = 'IL' }
    `).malloyResultMatches(runtime, {state: 'CA'});
  });
  // TODO test always join composite field usage
  it(`composite field used in view - ${databaseName}`, async () => {
    await expect(`
      ##! experimental.composite_sources
      source: state_facts is ${databaseName}.table('malloytest.state_facts')
      source: x is compose(state_facts, state_facts extend { dimension: foo is 1 }) extend {
        view: v is { group_by: foo }
      }
      run: x -> v
    `).malloyResultMatches(runtime, {foo: 1});
  });
  it(`composite field used in view refined with scalar - ${databaseName}`, async () => {
    await expect(`
      ##! experimental.composite_sources
      source: state_facts is ${databaseName}.table('malloytest.state_facts')
      source: x is compose(state_facts, state_facts extend { dimension: foo is 1 }) extend {
        view: v is {
          group_by: state
          where: state = 'CA'
          limit: 1
        }
      }
      run: x -> v + foo
    `).malloyResultMatches(runtime, {foo: 1, state: 'CA'});
  });
  it(`composite field used in view refined with literal view - ${databaseName}`, async () => {
    await expect(`
      ##! experimental.composite_sources
      source: state_facts is ${databaseName}.table('malloytest.state_facts')
      source: x is compose(state_facts, state_facts extend { dimension: foo is 1 }) extend {
        view: v is {
          group_by: state
          where: state = 'CA'
          limit: 1
        }
      }
      run: x -> v + { group_by: foo }
    `).malloyResultMatches(runtime, {foo: 1, state: 'CA'});
  });
  it(`composite field used in refined query - ${databaseName}`, async () => {
    await expect(`
      ##! experimental.composite_sources
      source: state_facts is ${databaseName}.table('malloytest.state_facts')
      source: x is compose(state_facts, state_facts extend { dimension: foo is 1 }) extend {
        view: v is {
          group_by: state
          where: state = 'CA'
          limit: 1
        }
      }
      query: v is x -> v
      run: v + { group_by: foo }
    `).malloyResultMatches(runtime, {foo: 1, state: 'CA'});
  });
  it(`composite of a composite - ${databaseName}`, async () => {
    await expect(`
      ##! experimental.composite_sources
      source: state_facts is ${databaseName}.table('malloytest.state_facts')
      source: x is compose(
        state_facts,
        state_facts extend { dimension: foo is 1 }
      )
      source: y is compose(
        x,
        x extend { dimension: bar is 2 }
      )
      // in order to get bar, we need to use the second composite input, which is itself a composite source
      // then in order to get foo, we need to resolve the inner composite source to its second input
      run: y -> { group_by: foo, bar }
    `).malloyResultMatches(runtime, {foo: 1, bar: 2});
  });
  it(`definitions from composite extension carry through - ${databaseName}`, async () => {
    await expect(`
      ##! experimental.composite_sources
      source: state_facts is ${databaseName}.table('malloytest.state_facts')
      source: x is compose(
        state_facts,
        state_facts extend { dimension: foo is 1 }
      ) extend {
        dimension: bar is 2
      }
      run: x -> { group_by: foo, bar }
    `).malloyResultMatches(runtime, {foo: 1, bar: 2});
  });
  it(`filters from composite extension carry through - ${databaseName}`, async () => {
    await expect(`
      ##! experimental.composite_sources
      source: state_facts is ${databaseName}.table('malloytest.state_facts')
      source: x is compose(
        state_facts,
        state_facts extend { dimension: foo is 1 }
      ) extend {
        where: state = 'CA'
      }
      run: x -> { group_by: foo, state }
    `).malloyResultMatches(runtime, {foo: 1, state: 'CA'});
  });
  it(`composite of a composite where greedy is bad- ${databaseName}`, async () => {
    await expect(`
      ##! experimental.composite_sources
      source: state_facts is ${databaseName}.table('malloytest.state_facts')
      source: x is compose(
        compose(
          state_facts extend { dimension: foo is 1.1, bar is 2.1 },
          state_facts extend { dimension: foo is 1.2, baz is 3.2 }
        ),
        state_facts extend { dimension: foo is 1.3, bar is 2.3, baz is 3.3 }
      )
      // even though the first composite has all the fields foo, bar, baz; it is impossible
      // to resolve it using the first composite, because you can't have both bar and baz
      // so the second input source is used instead
      run: x -> { group_by: foo, bar, baz }
    `).malloyResultMatches(runtime, {foo: 1.3, bar: 2.3, baz: 3.3});
  });
  it(`composite with parameters - ${databaseName}`, async () => {
    await expect(`
      ##! experimental { composite_sources parameters }
      source: state_facts is ${databaseName}.table('malloytest.state_facts')
      source: x(param is 1) is compose(
        state_facts extend { dimension: a is param },
        state_facts extend { dimension: b is param + 1 }
      )
      run: x(param is 2) -> { group_by: b }
    `).malloyResultMatches(runtime, {b: 3});
  });
});
