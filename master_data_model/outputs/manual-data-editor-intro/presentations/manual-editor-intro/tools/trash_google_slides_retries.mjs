import { execFileSync } from "node:child_process";

const keepId = "1Ra831EU7B1BISjX9PpJZrPYjVcX7Tc1IjDNcCllXZmk";
const token = execFileSync("gcloud", ["auth", "print-access-token"], {
  encoding: "utf8",
}).trim();
const params = new URLSearchParams({
  q: "name = 'Manual Data Editor Team Intro' and mimeType = 'application/vnd.google-apps.presentation' and trashed = false",
  fields: "files(id,name,createdTime)",
});

const listRes = await fetch(`https://www.googleapis.com/drive/v3/files?${params}`, {
  headers: {
    Authorization: `Bearer ${token}`,
    "X-Goog-User-Project": "looker-studio-pro-452620",
  },
});
const list = await listRes.json();
if (!listRes.ok) {
  throw new Error(JSON.stringify(list, null, 2));
}

const trashed = [];
for (const file of list.files ?? []) {
  if (file.id === keepId) continue;
  const res = await fetch(
    `https://www.googleapis.com/drive/v3/files/${file.id}?fields=id,name,trashed`,
    {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${token}`,
        "X-Goog-User-Project": "looker-studio-pro-452620",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ trashed: true }),
    },
  );
  const body = await res.json();
  if (!res.ok) {
    throw new Error(JSON.stringify(body, null, 2));
  }
  trashed.push(body);
}

console.log(JSON.stringify({ keepId, trashed }, null, 2));
