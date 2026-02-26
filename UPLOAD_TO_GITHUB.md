# Upload this project to GitHub

Run these commands **in your terminal** from the project folder (`elt-pipeline`).

## Option A: Using GitHub CLI (recommended)

If you have [GitHub CLI](https://cli.github.com/) installed (`brew install gh` on Mac):

```bash
cd /Users/vinaychowdary/elt-pipeline

# 1. Initialize repo and first commit (if not already done)
git init
git add -A
git commit -m "Initial commit: ELT pipeline - Real-time Crypto Prices"

# 2. Create the repo on GitHub and push (you'll be prompted to log in if needed)
gh auth login
gh repo create elt-pipeline --public --source=. --remote=origin --push
```

Replace `elt-pipeline` with your desired repo name if different.

---

## Option B: Create repo on GitHub first, then push

### 1. Create a new repository on GitHub

- Go to [github.com/new](https://github.com/new)
- **Repository name:** `elt-pipeline` (or any name you like)
- **Public**, leave "Add a README" **unchecked**
- Click **Create repository**

### 2. Run these in your terminal

```bash
cd /Users/vinaychowdary/elt-pipeline

git init
git add -A
git commit -m "Initial commit: ELT pipeline - Real-time Crypto Prices"

# Replace YOUR_USERNAME with your GitHub username and REPO_NAME with the repo name
git remote add origin https://github.com/YOUR_USERNAME/elt-pipeline.git

git branch -M main
git push -u origin main
```

If you use SSH instead of HTTPS:

```bash
git remote add origin git@github.com:YOUR_USERNAME/elt-pipeline.git
git branch -M main
git push -u origin main
```

---

## After uploading

- Add the repo URL to your README or portfolio.
- If you generated figures, ensure `docs/correlation_matrix.png` and `docs/trend_line.png` are committed so they show in the README.
