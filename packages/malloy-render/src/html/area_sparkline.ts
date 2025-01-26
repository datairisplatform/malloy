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

import {HTMLSparkLineRenderer} from './sparkline';
import {DataArray, Explore, Field} from '@datairis/malloy';
import * as lite from 'vega-lite';
import {getColorScale} from './utils';
import {DEFAULT_SPEC} from './vega_spec';
import {RendererFactory} from './renderer_factory';
import {Renderer} from './renderer';
import {SparkLineRenderOptions, StyleDefaults} from './data_styles';
import {RendererOptions} from './renderer_types';

export class HTMLAreaSparkLineRenderer extends HTMLSparkLineRenderer {
  override getVegaLiteSpec(data: DataArray): lite.TopLevelSpec {
    const fields = data.field.allFields;
    const xField = fields[0];
    const yField = fields[1];
    const colorField = fields[2];

    const xType = this.getDataType(xField);
    const yType = this.getDataType(yField);
    const colorType = colorField ? this.getDataType(colorField) : undefined;

    const colorDef =
      colorField !== undefined
        ? {
            field: colorField.name,
            type: colorType,
            axis: {title: null},
            scale: getColorScale(colorType, false),
          }
        : {value: '#4285F4'};

    const xSort = xType === 'nominal' ? null : undefined;
    const ySort = yType === 'nominal' ? null : undefined;

    const xDef = {
      field: xField.name,
      type: xType,
      sort: xSort,
      axis: {
        title: null,
        domain: false,
        grid: false,
        lables: false,
        ticks: false,
        values: [],
      },
      scale: {zero: false},
    };

    const yDef = {
      field: yField.name,
      type: yType,
      sort: ySort,
      axis: {
        title: null,
        domain: false,
        ticks: false,
        grid: false,
        lables: false,
        values: [],
      },
      scale: {zero: false},
    };

    return {
      ...DEFAULT_SPEC,
      ...this.getSize(),
      data: {
        values: this.mapData(data),
      },
      config: {
        view: {
          stroke: 'transparent',
        },
      },
      mark: {
        type: 'area',
        line: {
          color: '#4285F4',
        },
        point: false,
      },
      encoding: {
        x: xDef,
        y: yDef,
        color: colorDef,
      },
      background: 'transparent',
    };
  }
}

export class AreaSparkLineRendererFactory extends RendererFactory<SparkLineRenderOptions> {
  public static readonly instance = new AreaSparkLineRendererFactory();

  isValidMatch(field: Field | Explore): boolean {
    return field.name.endsWith('area');
  }

  create(
    document: Document,
    styleDefaults: StyleDefaults,
    rendererOptions: RendererOptions,
    _field: Field | Explore,
    options: SparkLineRenderOptions,
    timezone?: string
  ): Renderer {
    return new HTMLAreaSparkLineRenderer(
      document,
      styleDefaults,
      rendererOptions,
      options,
      timezone
    );
  }

  get rendererName() {
    return 'sparkline';
  }
}
