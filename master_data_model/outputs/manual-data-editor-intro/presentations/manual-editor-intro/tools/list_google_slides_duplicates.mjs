import { execFileSync } from "node:child_process";

const token = execFileSync("gcloud", ["auth", "print-access-token"], {
  encoding: "utf8",
}).trim();
const params = new URLSearchParams({
  q: "name = 'Manual Data Editor Team Intro' and mimeType = 'application/vnd.google-apps.presentation' and trashed = false",
  fields: "files(id,name,createdTime,webViewLink)",
  orderBy: "createdTime desc",
});

const res = await fetch(`https://www.googleapis.com/drive/v3/files?${params}`, {
  headers: {
    Authorization: `Bearer ${token}`,
    "X-Goog-User-Project": "looker-studio-pro-452620",
  },
});
const text = await res.text();
if (!res.ok) {
  throw new Error(`${res.status} ${res.statusText}\n${text}`);
}
console.log(text);
