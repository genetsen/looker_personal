export async function slide02(presentation, ctx) {
  const slide = presentation.slides.add();
  const ink = "#16324F";
  const gray = "#5B6775";
  const light = "#F3F6FA";
  const border = "#C8D4E0";

  ctx.addShape(slide, { x: 0, y: 0, width: ctx.W, height: ctx.H, fill: "#FFFFFF", line: ctx.line("#FFFFFF", 0) });
  ctx.addText(slide, { x: 54, y: 42, width: 900, height: 42, text: "How the team uses it", typeface: "Arial", fontSize: 30, bold: true, color: ink });
  ctx.addText(slide, { x: 56, y: 92, width: 900, height: 30, text: "The sheet is intentionally one main tab: find the package, edit the visible value, request refresh.", typeface: "Arial", fontSize: 16, color: gray });

  const steps = [
    ["1", "Filter", "Use slicers for Advertiser, Channel, Campaign, and Site."],
    ["2", "Find", "Confirm the row by Package ID, Site, and Package Friendly Name."],
    ["3", "Edit", "Change the visible value directly. There are no separate override columns."],
    ["4", "Request refresh", "Check the request box when the edits are ready for review/load."],
  ];

  steps.forEach((step, i) => {
    const x = 74 + i * 286;
    ctx.addShape(slide, { x, y: 190, width: 235, height: 250, fill: light, line: ctx.line(border, 1) });
    ctx.addShape(slide, { x: x + 18, y: 212, width: 44, height: 44, fill: ink, line: ctx.line(ink, 0) });
    ctx.addText(slide, { x: x + 18, y: 220, width: 44, height: 28, text: step[0], typeface: "Arial", fontSize: 20, bold: true, color: "#FFFFFF", align: "center" });
    ctx.addText(slide, { x: x + 18, y: 278, width: 190, height: 28, text: step[1], typeface: "Arial", fontSize: 21, bold: true, color: ink });
    ctx.addText(slide, { x: x + 18, y: 322, width: 188, height: 76, text: step[2], typeface: "Arial", fontSize: 15.5, color: "#27384A" });
  });

  ctx.addShape(slide, { x: 78, y: 522, width: 1034, height: 78, fill: "#FFF7E6", line: ctx.line("#E5C26A", 1) });
  ctx.addText(slide, { x: 104, y: 542, width: 170, height: 24, text: "Color cues", typeface: "Arial", fontSize: 18, bold: true, color: ink });
  const markers = [["Orange", "changed"], ["Purple", "already manual"], ["Red", "fix before load"]];
  const colors = ["#F59E0B", "#7C3AED", "#D11F2A"];
  markers.forEach((item, i) => {
    const x = 310 + i * 220;
    ctx.addShape(slide, { x, y: 548, width: 18, height: 18, fill: colors[i], line: ctx.line(colors[i], 0) });
    ctx.addText(slide, { x: x + 28, y: 544, width: 180, height: 28, text: `${item[0]} = ${item[1]}`, typeface: "Arial", fontSize: 15.5, color: "#27384A" });
  });

  return slide;
}
