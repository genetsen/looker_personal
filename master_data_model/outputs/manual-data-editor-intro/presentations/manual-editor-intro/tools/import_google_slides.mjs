import { execFileSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";

const root = path.resolve(
  "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/master_data_model/outputs/manual-data-editor-intro/presentations/manual-editor-intro",
);
const pptxPath = path.join(root, "output/manual-data-editor-team-intro.pptx");
const proofDir = path.join(root, "qa/google-slides-proof");
mkdirSync(proofDir, { recursive: true });

const token = execFileSync("gcloud", ["auth", "print-access-token"], {
  encoding: "utf8",
}).trim();

async function googleJson(url, options = {}) {
  const res = await fetch(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${token}`,
      "X-Goog-User-Project": "looker-studio-pro-452620",
      ...(options.headers ?? {}),
    },
  });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}\n${text}`);
  }
  return JSON.parse(text);
}

const boundary = `manual-editor-deck-${Date.now()}`;
const metadata = {
  name: "Manual Data Editor Team Intro",
  mimeType: "application/vnd.google-apps.presentation",
};
const pptx = readFileSync(pptxPath);
const body = Buffer.concat([
  Buffer.from(
    `--${boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n${JSON.stringify(metadata)}\r\n`,
  ),
  Buffer.from(
    `--${boundary}\r\nContent-Type: application/vnd.openxmlformats-officedocument.presentationml.presentation\r\n\r\n`,
  ),
  pptx,
  Buffer.from(`\r\n--${boundary}--\r\n`),
]);

const upload = await googleJson(
  "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,name,mimeType,webViewLink",
  {
    method: "POST",
    headers: {
      "Content-Type": `multipart/related; boundary=${boundary}`,
      "Content-Length": String(body.length),
    },
    body,
  },
);

const drive = await googleJson(
  `https://www.googleapis.com/drive/v3/files/${upload.id}?fields=id,name,mimeType,webViewLink,createdTime,modifiedTime`,
);
const slides = await googleJson(
  `https://slides.googleapis.com/v1/presentations/${upload.id}`,
);

const thumbnailUrls = [];
for (const [index, slide] of slides.slides.entries()) {
  const thumb = await googleJson(
    `https://slides.googleapis.com/v1/presentations/${upload.id}/pages/${slide.objectId}/thumbnail?thumbnailProperties.mimeType=PNG&thumbnailProperties.thumbnailSize=LARGE`,
  );
  thumbnailUrls.push({
    index: index + 1,
    objectId: slide.objectId,
    contentUrl: thumb.contentUrl,
  });
}

const proof = {
  checkedAt: new Date().toISOString(),
  drive,
  slideCount: slides.slides.length,
  slideObjectIds: slides.slides.map((slide) => slide.objectId),
  textSnippets: slides.slides.map((slide, index) => ({
    slide: index + 1,
    text: slide.pageElements
      ?.flatMap((el) => el.shape?.text?.textElements ?? [])
      .map((textEl) => textEl.textRun?.content ?? "")
      .join("")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 500),
  })),
  thumbnailUrls,
};

writeFileSync(
  path.join(proofDir, "google-slides-proof.json"),
  `${JSON.stringify(proof, null, 2)}\n`,
);

console.log(JSON.stringify(proof, null, 2));
