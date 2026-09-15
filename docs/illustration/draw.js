// The README illustration: a timeline. A job runs, its topic goes quiet, after idle-after Siesta
// suspends it, the job sleeps with a savepoint and no pods, a record arrives, Siesta resumes it
// and the job continues where it stopped.
const rough = require('roughjs/bundled/rough.cjs.js');
const opentype = require('opentype.js');
const fs = require('fs');

// Labels are drawn as glyph outlines, because GitHub shows the SVG through an <img> tag that
// cannot load fonts. Patrick Hand, SIL Open Font License, see fonts/OFL.txt.
const face = opentype.parse(fs.readFileSync(`${__dirname}/fonts/PatrickHand-Regular.ttf`).buffer);

const themes = {
  light: { ink: '#0b2951', fill: '#cbe7fb', mid: '#6e8ca9', faint: '#b0cde4', text: '#0b2951' },
  dark:  { ink: '#cbe7fb', fill: '#27599a', mid: '#84a0bb', faint: '#3d5f85', text: '#cbe7fb' },
};
const W = 1200, H = 345;

function build(name, t) {
  const g = rough.generator({ options: { roughness: 1.4, bowing: 1, stroke: t.ink, strokeWidth: 2, seed: 7 } });
  const out = [];
  const paths = (d) => g.toPaths(d).map(p =>
    `<path d="${p.d}" stroke="${p.stroke}" stroke-width="${p.strokeWidth}" fill="${p.fill || 'none'}"${p.strokeDasharray ? ` stroke-dasharray="${p.strokeDasharray}"` : ''} stroke-linecap="round" stroke-linejoin="round"/>`).join('\n');
  // handwriting faces run small, so sizes are scaled up a little; weight above 400 adds a stroke
  const text = (x, y, s, size = 18, anchor = 'start', weight = 400, color = t.text) => {
    const px = size * 1.18;
    const w = face.getAdvanceWidth(s, px);
    const sx = anchor === 'middle' ? x - w / 2 : anchor === 'end' ? x - w : x;
    const d = face.getPath(s, sx, y, px).toPathData(1);
    const stroke = weight > 400 ? ` stroke="${color}" stroke-width="${(weight - 400) / 400}" stroke-linejoin="round"` : '';
    return `<path d="${d}" fill="${color}"${stroke}/>`;
  };
  const tick = (x, y, half, width) =>
    paths(g.line(x, y - half, x, y + half, { stroke: t.ink, strokeWidth: width, roughness: 0.6, bowing: 0.3 }));

  // geometry
  const x0 = 170, x1 = 1150;          // time axis
  const topicY = 85, jobY = 210;      // lanes
  const sleepAt = 560, newRecord = 900, wakeAt = 935;

  // lanes
  out.push(text(30, topicY + 6, 'Kafka topic', 20, 'start', 600));
  out.push(text(30, jobY + 6, 'Flink job', 20, 'start', 600));
  out.push(paths(g.line(x0, topicY, x1, topicY, { stroke: t.faint, strokeWidth: 1.5 })));

  // records: an uneven burst, silence, one record
  const burst = [0, 14, 26, 52, 62, 70, 80, 104, 118, 150, 158, 166, 176, 200, 236, 246];
  for (const d of burst) out.push(tick(x0 + 10 + d, topicY, 14, 2.5));
  const lastRecord = x0 + 10 + burst[burst.length - 1];
  out.push(tick(newRecord, topicY, 16, 3));
  out.push(text(newRecord, topicY - 26, 'a record', 16, 'middle'));

  // idle-after bracket over the quiet stretch
  const by = topicY + 34;
  out.push(paths(g.line(lastRecord + 8, by, sleepAt, by, { stroke: t.mid, strokeWidth: 1.5 })));
  out.push(paths(g.line(lastRecord + 8, by - 6, lastRecord + 8, by + 6, { stroke: t.mid, strokeWidth: 1.5 })));
  out.push(paths(g.line(sleepAt, by - 6, sleepAt, by + 6, { stroke: t.mid, strokeWidth: 1.5 })));
  out.push(text((lastRecord + 8 + sleepAt) / 2, by + 22, 'idle-after', 16, 'middle', 400, t.mid));

  // job: running band, sleeping gap, running band
  out.push(paths(g.rectangle(x0, jobY - 22, sleepAt - x0, 44, { fill: t.fill, fillStyle: 'solid', stroke: t.ink })));
  out.push(text((x0 + sleepAt) / 2, jobY + 6, 'running', 17, 'middle', 500));
  out.push(paths(g.line(sleepAt + 6, jobY, wakeAt - 6, jobY, { stroke: t.mid, strokeWidth: 2, strokeLineDash: [6, 8] })));
  out.push(text((sleepAt + wakeAt) / 2, jobY - 16, 'asleep', 17, 'middle', 500, t.mid));
  out.push(text((sleepAt + wakeAt) / 2, jobY + 28, 'no JobManager, no TaskManagers, no cost', 16, 'middle', 400, t.mid));
  out.push(paths(g.rectangle(wakeAt, jobY - 22, x1 - wakeAt, 44, { fill: t.fill, fillStyle: 'solid', stroke: t.ink })));
  out.push(text((wakeAt + x1) / 2, jobY + 6, 'running', 17, 'middle', 500));

  // savepoint bookmark at the sleep edge
  const fx = sleepAt, fy = jobY - 22;
  out.push(paths(g.line(fx, fy, fx, fy - 46, { stroke: t.ink, strokeWidth: 2 })));
  out.push(paths(g.polygon([[fx, fy - 46], [fx + 34, fy - 38], [fx, fy - 30]], { fill: t.ink, fillStyle: 'solid', stroke: t.ink })));
  out.push(text(fx + 42, fy - 32, 'savepoint', 16, 'start'));

  // the record wakes the job: a curved arrow from the record down to the resume edge
  out.push(paths(g.curve([[newRecord, topicY + 20], [newRecord + 12, topicY + 70], [wakeAt - 14, jobY - 50], [wakeAt - 8, jobY - 30]], { stroke: t.ink, strokeWidth: 2 })));
  out.push(paths(g.polygon([[wakeAt - 6, jobY - 26], [wakeAt - 18, jobY - 40], [wakeAt - 2, jobY - 44]], { fill: t.ink, fillStyle: 'solid', stroke: t.ink })));
  out.push(text(wakeAt + 30, jobY - 50, 'wakes within a minute,', 16, 'start'));
  out.push(text(wakeAt + 30, jobY - 30, 'continues where it stopped', 16, 'start'));

  // Siesta at the two decision points, hanging from the band edges
  const marker = (x, label) => {
    const w = 128, h = 26, top = jobY + 22 + 16;
    out.push(paths(g.line(x, jobY + 22, x, top, { stroke: t.ink, strokeWidth: 1.5 })));
    out.push(paths(g.rectangle(x - w / 2, top, w, h, { fill: t.fill, fillStyle: 'solid', stroke: t.ink, strokeWidth: 1.5, roughness: 1 })));
    out.push(text(x, top + 18, label, 14, 'middle', 600));
  };
  marker(sleepAt, 'Siesta suspends');
  marker(wakeAt, 'Siesta resumes');

  // time axis
  const ay = 310;
  out.push(paths(g.line(x0, ay, x1, ay, { stroke: t.mid, strokeWidth: 1.5 })));
  out.push(paths(g.polygon([[x1, ay], [x1 - 14, ay - 6], [x1 - 14, ay + 6]], { fill: t.mid, fillStyle: 'solid', stroke: t.mid })));
  out.push(text(x1 - 20, ay + 26, 'time', 16, 'end', 400, t.mid));

  const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="A job runs while records arrive. After idle-after Siesta suspends it with a savepoint. A record arrives, Siesta resumes it and the job continues where it stopped.">
${out.join('\n')}
</svg>`;
  fs.writeFileSync(`${__dirname}/../siesta-${name}.svg`, svg);
  console.log(`siesta-${name}.svg`, svg.length, 'bytes');
}
for (const [name, t] of Object.entries(themes)) build(name, t);
