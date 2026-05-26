export async function slide01(presentation, ctx) {
  const slide = presentation.slides.add();

  const ink = "#16324f";
  const slate = "#34455c";
  const muted = "#667389";
  const blue = "#dbe9f6";
  const blue2 = "#eff6fb";
  const green = "#e5f2e9";
  const amber = "#fff4dc";
  const teal = "#437780";
  const rule = "#bfd0df";
  const white = "#ffffff";

  ctx.addShape(slide, {
    name: "background",
    x: 0,
    y: 0,
    width: ctx.W,
    height: ctx.H,
    fill: white,
    line: ctx.line("#00000000", 0),
  });

  ctx.addShape(slide, {
    name: "top-rule",
    x: 64,
    y: 54,
    width: 92,
    height: 5,
    fill: teal,
    line: ctx.line("#00000000", 0),
  });

  ctx.addText(slide, {
    name: "kicker",
    text: "GS INTERNAL",
    x: 64,
    y: 72,
    width: 320,
    height: 24,
    fontSize: 10.5,
    bold: true,
    color: muted,
    typeface: "Arial",
  });

  ctx.addText(slide, {
    name: "title",
    text: "Manual Data Editor",
    x: 64,
    y: 102,
    width: 560,
    height: 56,
    fontSize: 36,
    bold: true,
    color: ink,
    typeface: "Arial",
  });

  ctx.addText(slide, {
    name: "subtitle",
    text: "One shared Google Sheet for dashboard corrections, planned-value fixes, and missing package data.",
    x: 66,
    y: 166,
    width: 720,
    height: 44,
    fontSize: 15.5,
    color: slate,
    typeface: "Arial",
  });

  ctx.addText(slide, {
    name: "common-fixes-head",
    text: "What it is for",
    x: 66,
    y: 254,
    width: 260,
    height: 30,
    fontSize: 18,
    bold: true,
    color: ink,
    typeface: "Arial",
  });

  ctx.addText(slide, {
    name: "common-fixes-list",
    text:
      "Correct pacing from outdated Prisma dates or planned imps\n" +
      "Add delivery metrics for packages not tracked in Prisma\n" +
      "Update channel classification, like Display to Newsletter\n" +
      "Add planned values for social or other non-Prisma packages\n" +
      "Keep corrections centralized and trackable",
    x: 66,
    y: 298,
    width: 600,
    height: 198,
    fontSize: 14.5,
    color: slate,
    typeface: "Arial",
    insets: { left: 0, right: 6, top: 0, bottom: 0 },
  });

  const rows = [
    { n: "1", label: "Find the package", body: "Filter to the right row in the sheet.", y: 250, fill: blue2 },
    { n: "2", label: "Edit the value", body: "Type directly into the visible cell.", y: 354, fill: amber },
    { n: "3", label: "Request refresh", body: "Check the box when it is ready to load.", y: 458, fill: green },
  ];

  for (const row of rows) {
    ctx.addShape(slide, {
      name: `step-${row.n}-bg`,
      x: 790,
      y: row.y,
      width: 390,
      height: 78,
      fill: row.fill,
      line: ctx.line(rule, 1),
    });
    ctx.addShape(slide, {
      name: `step-${row.n}-num`,
      x: 812,
      y: row.y + 19,
      width: 40,
      height: 40,
      fill: ink,
      line: ctx.line("#00000000", 0),
    });
    ctx.addText(slide, {
      name: `step-${row.n}-num-label`,
      text: row.n,
      x: 812,
      y: row.y + 23,
      width: 40,
      height: 30,
      fontSize: 17,
      bold: true,
      color: white,
      align: "center",
      valign: "middle",
      typeface: "Arial",
    });
    ctx.addText(slide, {
      name: `step-${row.n}-label`,
      text: row.label,
      x: 874,
      y: row.y + 16,
      width: 260,
      height: 24,
      fontSize: 15,
      bold: true,
      color: ink,
      typeface: "Arial",
    });
    ctx.addText(slide, {
      name: `step-${row.n}-body`,
      text: row.body,
      x: 874,
      y: row.y + 42,
      width: 270,
      height: 24,
      fontSize: 12.5,
      color: slate,
      typeface: "Arial",
    });
  }

  ctx.addShape(slide, {
    name: "note-bg",
    x: 790,
    y: 570,
    width: 390,
    height: 58,
    fill: blue,
    line: ctx.line("#00000000", 0),
  });
  ctx.addText(slide, {
    name: "note",
    text: "Today: package-level adjustments. Placement and creative-level edits stay separate for now.",
    x: 810,
    y: 585,
    width: 350,
    height: 34,
    fontSize: 11.5,
    color: slate,
    typeface: "Arial",
  });

  ctx.addShape(slide, {
    name: "footer-bg",
    x: 66,
    y: 612,
    width: 610,
    height: 42,
    fill: blue,
    line: ctx.line("#00000000", 0),
  });
  ctx.addText(slide, {
    name: "footer-link",
    text: "Open the editor: Manual Data Editor Google Sheet",
    x: 86,
    y: 624,
    width: 560,
    height: 22,
    fontSize: 12.5,
    bold: false,
    color: ink,
    typeface: "Arial",
  });

  return slide;
}
