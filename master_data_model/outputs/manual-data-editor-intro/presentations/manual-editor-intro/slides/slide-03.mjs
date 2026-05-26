export async function slide03(presentation, ctx) {
  const slide = presentation.slides.add();
  const ink = "#16324F";
  const gray = "#5B6775";
  const border = "#C8D4E0";

  ctx.addShape(slide, { x: 0, y: 0, width: ctx.W, height: ctx.H, fill: "#FFFFFF", line: ctx.line("#FFFFFF", 0) });
  ctx.addText(slide, { x: 54, y: 42, width: 980, height: 42, text: "What users need to remember", typeface: "Arial", fontSize: 30, bold: true, color: ink });
  ctx.addText(slide, { x: 56, y: 92, width: 900, height: 30, text: "Simple rules keep edits predictable and make the backend audit trail clean.", typeface: "Arial", fontSize: 16, color: gray });

  const columns = [
    {
      title: "Package-level edits",
      subtitle: "Apply everywhere for the Package ID",
      bullets: ["Flight Start / End", "Advertiser, campaign, supplier", "Package name, initiative, GS Channel"],
      fill: "#EAF3FA",
    },
    {
      title: "Delivered metric edits",
      subtitle: "Apply only to the selected delivery dates",
      bullets: ["Spend and impressions", "Clicks", "Video plays and completions"],
      fill: "#FFF7E6",
    },
    {
      title: "New manual rows",
      subtitle: "Allowed when the package is missing",
      bullets: ["Package ID, site, friendly name", "Flight and delivery dates", "Required metadata plus one metric"],
      fill: "#EAF7EF",
    },
  ];

  columns.forEach((col, i) => {
    const x = 62 + i * 382;
    ctx.addShape(slide, { x, y: 168, width: 330, height: 310, fill: col.fill, line: ctx.line(border, 1) });
    ctx.addText(slide, { x: x + 24, y: 196, width: 276, height: 28, text: col.title, typeface: "Arial", fontSize: 21, bold: true, color: ink });
    ctx.addText(slide, { x: x + 24, y: 232, width: 276, height: 42, text: col.subtitle, typeface: "Arial", fontSize: 15.5, color: gray });
    col.bullets.forEach((bullet, j) => {
      const y = 306 + j * 45;
      ctx.addText(slide, { x: x + 28, y, width: 24, height: 22, text: "•", typeface: "Arial", fontSize: 18, bold: true, color: ink });
      ctx.addText(slide, { x: x + 54, y: y + 1, width: 240, height: 24, text: bullet, typeface: "Arial", fontSize: 15.5, color: "#27384A" });
    });
  });

  ctx.addShape(slide, { x: 62, y: 536, width: 1096, height: 70, fill: "#F3F6FA", line: ctx.line(border, 1) });
  ctx.addText(slide, { x: 88, y: 554, width: 260, height: 28, text: "Backend rule", typeface: "Arial", fontSize: 18, bold: true, color: ink });
  ctx.addText(slide, { x: 310, y: 552, width: 800, height: 32, text: "Validated sheet edits become backend manual fields and take priority for final reporting fields. Blank cells leave normal source values alone.", typeface: "Arial", fontSize: 16, color: "#27384A" });

  ctx.addText(slide, { x: 62, y: 648, width: 1080, height: 22, text: "Bottom line: edit only what needs correction, then let the loader validate and publish the change.", typeface: "Arial", fontSize: 14, color: gray });

  return slide;
}
