# 🕷️ TikTok Scraper — GitHub Actions Setup

## 📁 File Structure

```
repo/
├── scraper-fixed.py          ← Main scraper script
├── requirements.txt          ← Python dependencies
├── links.txt                 ← Apni TikTok URLs yahan daalo
├── README.md                 ← Ye file
└── .github/
    └── workflows/
        └── scrape.yml        ← GitHub Actions workflow
```

---

## 🔄 Tracking System (Redis-free)

| File | Kaam |
|------|------|
| `completed.txt` | Successfully done URLs — next run mein skip honge |
| `failed.txt` | Failed URLs — next run mein automatically retry honge |
| `tracking_report.txt` | Full log: SUCCESS / FAILED / SKIPPED + summary |
| `scraper_log.txt` | Detailed scraper debug log |

**Ye sab files Mega pe bhi upload hongi** → `vfx:/<BatchFolder>/_Reports/`

---

## ⚙️ Setup Steps

### Step 1 — GitHub Repo banao
1. GitHub pe new **private** repo banao
2. Sab files upload karo (ya git push karo)

### Step 2 — rclone config nikalo
Apni local machine pe:
```bash
cat ~/.config/rclone/rclone.conf
```
Ya Windows pe:
```
C:\Users\<YourName>\AppData\Roaming\rclone\rclone.conf
```
Poora content copy karo.

### Step 3 — GitHub Secret add karo
```
Repo → Settings → Secrets and variables → Actions → New repository secret
```

| Secret Name | Value |
|---|---|
| `RCLONE_CONFIG_CONTENT` | rclone.conf ka poora content paste karo |

### Step 4 — links.txt update karo
`links.txt` mein apni TikTok URLs daalo, ek line mein ek:
```
https://www.tiktok.com/@user/video/123456
https://www.tiktok.com/@user/video/789012
```
Commit aur push karo.

### Step 5 — Run karo
**Manual run:**
```
GitHub → Actions → 🕷️ TikTok Scraper → Run workflow → Run workflow
```

**Auto run:** Har roz 2 AM Pakistan time automatically chalega.

---

## 🔁 Retry Logic

```
Run 1:
  links.txt → 10 URLs
  7 success → completed.txt mein
  3 fail    → failed.txt mein

Run 2 (links.txt same):
  7 skip    (completed.txt mein already hain)
  3 retry   (failed.txt se wapas lete hain)
```

---

## ☁️ Mega Upload Structure

```
vfx:/
└── Batch--2025-04-22-Tuesday_02-00-00-AM/
    ├── @author_caption_videoId/
    │   ├── @author_caption_Video_...mp4
    │   ├── @author_caption_Audio_...mp3
    │   ├── @author_caption_Meta_...json
    │   └── @author_caption_RAWComments_...json
    └── _Reports/
        ├── tracking_report.txt
        ├── scraper_log.txt
        ├── completed.txt
        └── failed.txt
```

---

## ⚠️ Limits

| Item | Limit |
|---|---|
| GitHub Actions free tier | 2000 min/month |
| Per-job timeout | 6 hours (350 min set) |
| Runner disk space | ~14 GB |
