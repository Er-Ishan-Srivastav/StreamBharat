# Publish this repository to GitHub

The local Git repository is prepared with branch `main`, tag `v0.1.0`, commit history and this remote destination:

`https://github.com/Er-Ishan-Srivastav/StreamBharat.git`

The connected GitHub integration available during preparation can write files and commits to existing repositories, but it does not expose the GitHub **Create repository** operation. Therefore, the repository has been packaged as a verified Git bundle rather than being published into an unrelated existing repository.

## 1. Create the empty GitHub repository

Open [GitHub — create a new repository](https://github.com/new).

Use:

- **Repository name:** `StreamBharat`
- **Description:** `OTT streaming analytics platform built with PySpark, Spark Structured Streaming and Delta Lake`
- **Visibility:** Private is a sensible default for a class/team workspace; change it to Public only after confirming the team is comfortable publishing the work.
- Do **not** initialise it with a README, `.gitignore` or license. The local repository already has its initial commit and project files.

## 2. Clone the supplied Git bundle and push it

Download `StreamBharat-Project.gitbundle` from the conversation. In PowerShell, from the folder where it was downloaded:

```powershell
git clone .\StreamBharat-Project.gitbundle StreamBharat
cd .\StreamBharat
git remote add origin https://github.com/Er-Ishan-Srivastav/StreamBharat.git
git push -u origin main
git push origin --tags
```

GitHub will request authentication through your configured Git Credential Manager/browser flow. Do not paste a personal access token into chat or commit it to the repository.

## 3. Verify

Open `https://github.com/Er-Ishan-Srivastav/StreamBharat` and confirm that the README, `docs/`, `src/`, `tests/` and workflow are visible. The `v0.1.0` tag should also be present.

## Alternative: publish from the source ZIP

The source ZIP contains the tracked project files but not the `.git` metadata. After extracting it, run:

```powershell
git init -b main
git add .
git commit -m "Initialise StreamBharat OTT analytics project"
git remote add origin https://github.com/Er-Ishan-Srivastav/StreamBharat.git
git push -u origin main
```

The Git bundle is preferred because it preserves the prepared commit and tag.
