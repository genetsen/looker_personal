import {
  buildInitialState,
  buildAssignmentRows,
  generateAdswerveOutput,
  summarizeFixture,
  validateAgainstApollo,
} from './generator.mjs';

const state = {
  fixture: null,
  model: null,
  activeView: 'workflow',
};

const views = [
  ['workflow', 'Workflow'],
  ['prisma', 'Prisma Import'],
  ['creatives', 'Creative Assets'],
  ['assignments', 'Assignments'],
  ['utms', 'UTMs'],
  ['adswerve', 'Adswerve Output'],
  ['qa', 'QA'],
];

function el(tag, props = {}, children = []) {
  const node = document.createElement(tag);
  Object.entries(props).forEach(([key, value]) => {
    if (key === 'className') node.className = value;
    else if (key === 'text') node.textContent = value;
    else if (key.startsWith('on')) node.addEventListener(key.slice(2).toLowerCase(), value);
    else node.setAttribute(key, value);
  });
  children.forEach((child) => node.append(child));
  return node;
}

function renderTable(rows, limit = 80) {
  const table = el('table', { className: 'data-table' });
  rows.slice(0, limit).forEach((row, rowIndex) => {
    const tr = el('tr');
    row.forEach((cell) => {
      tr.append(el(rowIndex === 0 ? 'th' : 'td', { text: String(cell ?? '') }));
    });
    table.append(tr);
  });
  if (rows.length > limit) {
    const tr = el('tr');
    tr.append(el('td', {
      colspan: String(Math.max(...rows.slice(0, limit).map((row) => row.length), 1)),
      text: `${rows.length - limit} more rows hidden in this preview`,
    }));
    table.append(tr);
  }
  return el('div', { className: 'table-wrap' }, [table]);
}

function toCsv(rows) {
  return rows
    .map((row) =>
      row
        .map((value) => {
          const text = String(value ?? '');
          return /[",\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
        })
        .join(','),
    )
    .join('\n');
}

function downloadAdswerveCsv() {
  const rows = generateAdswerveOutput(state.model);
  const blob = new Blob([toCsv(rows)], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const link = el('a', {
    href: url,
    download: 'apollo_adswerve_output_from_prototype.csv',
  });
  document.body.append(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function stat(label, value) {
  return el('div', { className: 'stat' }, [
    el('span', { text: label }),
    el('strong', { text: String(value) }),
  ]);
}

function renderWorkflow() {
  const summary = summarizeFixture(state.model);
  return [
    el('section', { className: 'hero-panel' }, [
      el('div', {}, [
        el('p', { className: 'eyebrow', text: 'Manual Canonical Prototype' }),
        el('h1', { text: 'Creative Assignment Sheet' }),
        el('p', {
          className: 'lede',
          text:
            'A durable prototype of the manual Prisma paste, creative assignment, UTM generation, and Adswerve export workflow. The Prisma import is treated as immutable.',
        }),
      ]),
      el('div', { className: 'validation-card' }, [
        el('span', { text: 'Apollo parity' }),
        el('strong', {
          text: validateAgainstApollo(state.model).passed ? 'PASS' : 'CHECK',
        }),
        el('code', { text: summary.hash }),
      ]),
    ]),
    el('section', { className: 'stats-grid' }, [
      stat('Prisma preview rows', summary.prismaPreviewRows),
      stat('Creative rows', summary.creativeRows),
      stat('Assignment pairs detected', summary.assignmentRows),
      stat('Adswerve output rows', summary.adswerveRows),
    ]),
    el('section', { className: 'workflow-grid' }, [
      ...[
        ['0', 'Paste Prisma export', 'Raw import stays immutable.'],
        ['1', 'Add creative details', 'Creative inventory feeds dropdowns and output metadata.'],
        ['2', 'Assign creatives', 'Matrix stays user-friendly; app model normalizes assignments.'],
        ['3', 'Generate UTMs', 'Rules should become config-driven per client.'],
        ['4', 'Export Adswerve doc', 'Output is validated against the Apollo fixture.'],
      ].map(([step, title, body]) =>
        el('article', { className: 'workflow-step' }, [
          el('span', { text: step }),
          el('h3', { text: title }),
          el('p', { text: body }),
        ]),
      ),
    ]),
  ];
}

function renderPrisma() {
  return [
    el('section', { className: 'content-header' }, [
      el('h2', { text: 'Prisma Import' }),
      el('p', {
        text:
          'The import shape is immutable. This prototype reads the Apollo fixture exactly as the sheet produced it and treats downstream normalization as the improvement layer.',
      }),
    ]),
    renderTable(state.model.prismaPreview, 40),
  ];
}

function renderCreatives() {
  return [
    el('section', { className: 'content-header' }, [
      el('h2', { text: 'Creative Assets' }),
      el('p', { text: 'Creative asset rows drive the assignment dropdowns and output naming.' }),
    ]),
    renderTable(state.model.creativeDetails, 80),
  ];
}

function renderAssignments() {
  const assignmentRows = buildAssignmentRows(state.model.assignmentMatrixPreview);
  const tableRows = [
    ['Assignment ID', 'Source Row', 'Slot', 'Placement Label', 'Creative Name'],
    ...assignmentRows.map((row) => [
      row.assignmentId,
      row.sourceRowNumber,
      row.creativeSlot,
      row.placementLabel,
      row.creativeName,
    ]),
  ];
  return [
    el('section', { className: 'content-header' }, [
      el('h2', { text: 'Creative Assignments' }),
      el('p', {
        text:
          'This view shows the normalized target model: one row per placement and creative pairing, instead of comma-separated assignment strings.',
      }),
    ]),
    renderTable(tableRows, 120),
  ];
}

function renderUtms() {
  return [
    el('section', { className: 'content-header' }, [
      el('h2', { text: 'UTM Builder Preview' }),
      el('p', {
        text:
          'The current Apollo fixture preserves the exact sheet output. Future client rules should move into configuration tables rather than formula branches.',
      }),
    ]),
    renderTable(state.model.utmBuilderPreview, 80),
  ];
}

function renderAdswerve() {
  const rows = generateAdswerveOutput(state.model);
  return [
    el('section', { className: 'content-header split' }, [
      el('div', {}, [
        el('h2', { text: 'Adswerve Output' }),
        el('p', {
          text:
            'Generated in Apollo fixture mode to exactly match the current Google Sheet output for the same inputs.',
        }),
      ]),
      el('button', { className: 'primary-btn', onClick: downloadAdswerveCsv, text: 'Export CSV' }),
    ]),
    renderTable(rows, 120),
  ];
}

function renderQa() {
  const validation = validateAgainstApollo(state.model);
  const checks = [
    ['Adswerve output hash', validation.passed ? 'PASS' : 'FAIL', validation.actualHash],
    ['Expected Apollo hash', 'REFERENCE', validation.expectedHash],
    ['Output rows', String(validation.rows), 'Populated rows returned by the fixture output range'],
    ['Output columns', String(validation.columns), 'Adswerve output width'],
    [
      'Deprecated auto import',
      'WATCH',
      'Known legacy sheet residue; prototype treats Prisma import as manual and immutable.',
    ],
    [
      'Formula simplification',
      'NEXT',
      'Move assignment normalization and UTM rules into config-driven code.',
    ],
  ];
  return [
    el('section', { className: 'content-header' }, [
      el('h2', { text: 'QA Checks' }),
      el('p', { text: 'Validation is built around exact Apollo Adswerve output parity.' }),
    ]),
    renderTable([['Check', 'Status', 'Detail'], ...checks], 20),
  ];
}

function renderActiveView() {
  const container = document.querySelector('#view');
  container.innerHTML = '';
  const renderers = {
    workflow: renderWorkflow,
    prisma: renderPrisma,
    creatives: renderCreatives,
    assignments: renderAssignments,
    utms: renderUtms,
    adswerve: renderAdswerve,
    qa: renderQa,
  };
  renderers[state.activeView]().forEach((node) => container.append(node));
}

function renderNav() {
  const nav = document.querySelector('#nav');
  nav.innerHTML = '';
  views.forEach(([id, label]) => {
    nav.append(
      el('button', {
        className: id === state.activeView ? 'active' : '',
        text: label,
        onClick: () => {
          state.activeView = id;
          renderNav();
          renderActiveView();
        },
      }),
    );
  });
}

async function bootstrap() {
  const response = await fetch('/data/apollo_fixture.json');
  state.fixture = await response.json();
  state.model = buildInitialState(state.fixture);
  renderNav();
  renderActiveView();
}

bootstrap().catch((error) => {
  document.querySelector('#view').textContent = error.message;
});
