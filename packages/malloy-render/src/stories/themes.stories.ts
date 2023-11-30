import {Meta} from '@storybook/html';
import script from './static/themes.malloy?raw';
import {createLoader} from './util';
import './themes.css';
import '../component/render';

const meta: Meta = {
  title: 'Malloy Next/Themes',
  render: ({classes}, context) => {
    const parent = document.createElement('div');
    parent.style.height = '1000px';
    parent.style.position = 'relative';
    const el = document.createElement('malloy-render');
    if (classes) el.classList.add(classes);
    el.result = context.loaded['result'];
    parent.appendChild(el);
    return parent;
  },
  loaders: [createLoader(script)],
  argTypes: {},
};

export default meta;

export const ModelThemeOverride = {
  args: {
    source: 'products',
    view: `records`,
  },
};

export const ViewThemeOverride = {
  args: {
    source: 'products',
    view: `records_override`,
  },
};
