export async function slide01(presentation, ctx) {
  const slide = presentation.slides.add();
  const ink = "#16324F";
  const blue = "#DDEAF7";
  const gray = "#5B6775";

  ctx.addShape(slide, { x: 0, y: 0, width: ctx.W, height: ctx.H, fill: "#FFFFFF", line: ctx.line("#FFFFFF", 0) });
  ctx.addShape(slide, { x: 0, y: 0, width: ctx.W, height: 86, fill: ink, line: ctx.line(ink, 0) });

  ctx.addText(slide, {
    x: 54, y: 24, width: 850, height: 44,
    text: "Manual Data Editor",
    typeface: "Arial", fontSize: 32, bold: true, color: "#FFFFFF",
  });
  ctx.addText(slide, {
    x: 54, y: 138, width: 900, height: 58,
    text: "A single Google Sheet for controlled dashboard corrections",
    typeface: "Arial", fontSize: 30, bold: true, color: ink,
  });
  ctx.addText(slide, {
    x: 56, y: 218, width: 620, height: 90,
    text: "Instead of editing backend tables or juggling separate override columns, users find the package row and edit the value they want corrected.",
    typeface: "Arial", fontSize: 20, color: gray,
  });

  const rows = [
    ["Who uses it", "Media buyers or operators correcting a specific package line"],
    ["Where edits go", "Backend manual fields, then final dashboard fields"],
    ["What updates", "Client dashboards and external partner reporting after the loader runs"],
  ];
  rows.forEach((row, i) => {
    const y = 348 + i * 78;
    ctx.addShape(slide, { x: 56, y, width: 1040, height: 54, fill: i % 2 === 0 ? blue : "#F4F7FA", line: ctx.line("#C8D4E0", 1) });
    ctx.addText(slide, { x: 80, y: y + 14, width: 190, height: 24, text: row[0], typeface: "Arial", fontSize: 17, bold: true, color: ink });
    ctx.addText(slide, { x: 294, y: y + 14, width: 760, height: 24, text: row[1], typeface: "Arial", fontSize: 17, color: "#27384A" });
  });

  ctx.addText(slide, {
    x: 56, y: 640, width: 1100, height: 24,
    text: "Meeting goal: make sure everyone knows when to use the sheet and what happens after a value is changed.",
    typeface: "Arial", fontSize: 14, color: gray,
  });

  return slide;
}
