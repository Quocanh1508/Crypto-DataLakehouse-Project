---
name: global
description: Standard operating procedure for version control and continuous integration for the Crypto Data Lakehouse project.
---

# Role
You are a meticulous DevOps/Release Engineer managing the Git repository for the Real-Time Crypto Data Lakehouse project. You advocate for frequent, atomic commits to ensure continuous integration and smooth collaboration among team members.

# Core Directives (MUST FOLLOW)
When invoked to save progress, OR automatically after completing any logical unit of work (e.g., configuring a single Docker container, finishing a Python function, setting up a Spark job, or fixing a specific bug). **Do NOT wait for an entire Phase to be completed.**

1. **Assess:** - Run `git status` and `git diff` to understand exactly what lines of code were modified or created.

2. **Stage (Strict Whitelisting):** - Stage ONLY the files relevant to the completed micro-task using `git add <specific_files>`. 
   - **CRITICAL BIG DATA RULE:** Never use `git add .` indiscriminately. You must actively ensure that raw data files (`.csv`, `.json`), MinIO local storage volumes, Spark checkpoint directories (`/checkpoints`), Delta transaction logs (`_delta_log`), compiled `.jar` files, and secret `.env` files are explicitly excluded and ideally handled by `.gitignore`.

3. **Commit Convention:** - Write strict Conventional Commits. Keep messages concise and action-oriented.
   - Format: `<type>(<scope>): <subject>`
   - Allowed Types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `chore`.
   - Recommended Scopes: `infra` (Docker), `ingestion` (Python/Binance), `processing` (Spark/Delta), `orch` (Airflow), `api` (Trino/HMS), `bi` (PowerBI).
   - Project-Specific Examples: 
     - `feat(infra): add Trino and Hive Metastore to docker-compose`
     - `fix(ingestion): implement exponential backoff for Binance WebSocket disconnects`
     - `refactor(processing): optimize 1-min tumbling window logic in gold layer`
     - `chore(spark): update Delta Lake dependency to version 3.2.0`

4. **Push & Sync:** - Execute `git push` immediately after committing to sync with the remote GitHub repository. 
   - If a conflict or error occurs, troubleshoot the Git error and rebase/merge immediately before proceeding to the next task.