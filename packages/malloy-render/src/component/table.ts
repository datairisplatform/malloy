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

import {DataArray, DataRecord, Field} from '@malloydata/malloy';
import {LitElement, TemplateResult, css, html} from 'lit';
import {customElement, eventOptions, property, state} from 'lit/decorators.js';
import {classMap} from 'lit/directives/class-map.js';
import {createContext, provide, consume} from '@lit/context';
import {isFirstChild, isLastChild} from './util';

const tableContext = createContext<{
  root: boolean;
}>('table');

type RenderOptions = {
  pinnedHeader?: boolean;
};

// TODO: replace with an estimator per column
function getColumnWidth() {
  return 130;
}

const getContentStyle = (f: Field) => {
  if (f.isAtomicField()) {
    const width = getColumnWidth();
    return `width: ${width}px; min-width: ${width}px; max-width: ${width}px;`;
  }
  return '';
};

const renderCell = (
  f: Field,
  value: unknown,
  options: {
    hideStartGutter: boolean;
    hideEndGutter: boolean;
  }
) => {
  return html`<div class="cell-wrapper">
    <div
      class=${classMap({
        'cell-gutter': true,
        'hide-gutter-border': options.hideStartGutter,
      })}
    ></div>
    <div class="cell-content" style="${getContentStyle(f)}">${value}</div>
    <div
      class=${classMap({
        'cell-gutter': true,
        'hide-gutter-border': options.hideEndGutter,
      })}
    ></div>
  </div>`;
};

const renderFieldContent = (
  row: DataRecord,
  f: Field,
  options: RenderOptions
) => {
  if (f.isExploreField()) {
    return html`<malloy-table
      .data=${row.cell(f) as DataArray}
      .pinnedHeader=${options.pinnedHeader ?? false}
      .rowLimit=${options.pinnedHeader ? 1 : Infinity}
    ></malloy-table>`;
  }
  if (options.pinnedHeader)
    return renderCell(f, '', {
      hideStartGutter: isFirstChild(f),
      hideEndGutter: isLastChild(f),
    });
  return renderCell(f, row.cell(f).value, {
    hideStartGutter: isFirstChild(f),
    hideEndGutter: isLastChild(f),
  });
};

const renderField = (row: DataRecord, f: Field, options: RenderOptions) => {
  return html`<td class="column-cell">
    ${renderFieldContent(row, f, options)}
  </td>`;
};

const renderHeader = (f: Field) => {
  const isFirst = isFirstChild(f);
  const isParentFirst = isFirstChild(f.parentExplore);
  const isParentNotAField = !f.parentExplore.isExploreField();
  const hideStartGutter = isFirst && (isParentFirst || isParentNotAField);

  const isLast = isLastChild(f);
  const isParentLast = isLastChild(f.parentExplore);
  const hideEndGutter = isLast && (isParentLast || isParentNotAField);

  return html`<th class="column-cell">
    ${renderCell(f, f.name, {
      hideStartGutter,
      hideEndGutter,
    })}
  </th>`;
};

@customElement('malloy-table')
export class Table extends LitElement {
  static override styles = css`
    .table-wrapper {
      width: 100%;
      height: 100%;
      position: relative;
      overflow: auto;
    }

    .sticky-header {
      position: sticky;
      top: 0px;
      z-index: 100;
    }

    .sticky-header-content {
      position: absolute;
      top: 0px;
      left: 0px;
      pointer-events: none;
    }

    .sticky-header-content th {
      pointer-events: all;
    }

    table {
      border-collapse: collapse;
      background: var(--table-background);
      font-variant-numeric: tabular-nums;
    }

    th {
      transition: background-color 0.25s;
    }

    table * {
      box-sizing: border-box;
    }

    .column-cell {
      height: var(--table-row-height);
      overflow: hidden;
      white-space: nowrap;
      text-align: left;
      padding: 0px;
      vertical-align: top;
      position: relative;
    }

    td.column-cell {
      font-weight: var(--table-body-weight);
      color: var(--table-body-color);
    }

    th.column-cell {
      font-weight: var(--table-header-weight);
      color: var(--table-header-color);
    }

    .cell-wrapper {
      height: var(--table-row-height);
      display: flex;
      align-items: center;
      overflow: hidden;
    }

    .cell-content {
      border-top: var(--table-border);
      height: var(--table-row-height);
      line-height: var(--table-row-height);
      flex: 1;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .cell-gutter {
      border-top: var(--table-border);
      height: var(--table-row-height);
      width: var(--table-gutter-size);
    }

    .cell-gutter.hide-gutter-border {
      border-top: none;
    }

    .pinned-header table {
      background: transparent;
    }

    .pinned-header th {
      background: var(--table-background);
    }

    .pinned-header.scrolled th {
      background: var(--table-pinned-background);
      box-shadow: 0 0 0.5em rgba(0, 0, 0, 0.5);
    }

    .pinned-header.scrolled {
      .cell-content,
      .cell-gutter,
      .cell-gutter.hide-gutter-border {
        border-top: var(--table-pinned-border);
      }
    }
  `;

  @property({attribute: false})
  data!: DataArray;

  @property({type: Number})
  rowLimit = Infinity;

  @property({type: Boolean})
  pinnedHeader = false;

  @state()
  protected _scrolling = false;

  @consume({context: tableContext})
  @property({attribute: false})
  public parentCtx;

  @provide({context: tableContext})
  ctx = {root: false};

  override connectedCallback() {
    super.connectedCallback();
    if (typeof this.parentCtx === 'undefined') {
      this.ctx = {
        root: true,
      };
    }
  }

  @eventOptions({passive: true})
  private _handleScroll(e: Event) {
    const target = e.target as HTMLElement;
    this._scrolling = target.scrollTop > 0;
  }

  // If rendering a pinned header, render it within the current ShadowDOM root so we can use CSS to style the nested table headers when scrolling
  protected override createRenderRoot(): HTMLElement | DocumentFragment {
    if (this.pinnedHeader) return this;
    return super.createRenderRoot();
  }

  override render() {
    const fields = this.data.field.allFields;

    const headers = fields.map(f => renderHeader(f));

    const renderOptions: RenderOptions = {
      pinnedHeader: this.pinnedHeader,
    };

    const rows: TemplateResult[] = [];
    let i = 0;
    for (const row of this.data) {
      if (i >= this.rowLimit) break;
      rows.push(
        html`<tr>
          ${fields.map(f => renderField(row, f, renderOptions))}
        </tr>`
      );
      i++;
    }

    const renderStickyHeader = () => {
      if (this.ctx.root)
        return html`<div class="sticky-header">
          <div class="sticky-header-content">
            <malloy-table
              class=${classMap({
                'pinned-header': true,
                'scrolled': this._scrolling,
              })}
              .rowLimit=${1}
              .data=${this.data}
              .pinnedHeader=${true}
            ></malloy-table>
          </div>
        </div>`;
    };

    return html`<div @scroll=${this._handleScroll} class="table-wrapper">
      ${renderStickyHeader()}
      <table>
        <thead>
          <tr>
            ${headers}
          </tr>
        </thead>
        <tbody>
          ${rows}
        </tbody>
      </table>
    </div>`;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'malloy-table': Table;
  }
}
