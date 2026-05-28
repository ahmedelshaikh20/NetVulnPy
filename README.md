# NetVulnPy

Large-scale vulnerability analysis of open-source Python networking repositories using static application security testing.

NetVulnPy is an automated pipeline that harvests Python networking repos from GitHub, scans them with Bandit or Semgrep, and loads the findings into a SQLite database with an interactive Streamlit dashboard.

## Pipeline Stages

1. **Harvest** -- query GitHub API for Python networking repos -> `repos.json`
2. **Download** -- download ZIPs and extract `.py` files -> `downloads/`
3. **Analyze** -- run Bandit or Semgrep on extracted files -> `results/`
4. **Load DB** -- import results into `findings.sqlite`

## Installation

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/NetVulnPy.git
cd NetVulnPy

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### GitHub Token

A GitHub personal access token is required for the harvest stage (unauthenticated requests are limited to 10/min).

Create a `.env` file in the project root:

```bash
GITHUB_TOKEN=ghp_your_token_here
```

Or pass it directly via `--token`.

## Usage

```bash
# Full pipeline with Bandit (default), 100 repos
python main.py --max-repos 100 --verbose

# Full pipeline with Semgrep
python main.py --max-repos 100 --analyzer semgrep --verbose

# Limit to 10 repos
python main.py --max-repos 10 --limit 10 --verbose

# Re-run analysis only (skip harvest and download)
python main.py --skip-harvest --skip-download --verbose

# Re-run DB load only
python main.py --skip-harvest --skip-download --skip-analyze --verbose
```

### Command-Line Arguments

| Argument | Default | Description |
|---|---|---|
| `--analyzer {bandit,semgrep}` | `bandit` | SAST tool to use |
| `--max-repos N` | `100` | Max repos to harvest from GitHub |
| `--limit N` | all | Max repos to download/analyze |
| `--token TOKEN` | `GITHUB_TOKEN` env | GitHub personal access token |
| `--output-dir DIR` | `.` | Root output directory |
| `--db PATH` | `findings.sqlite` | SQLite output path |
| `--keep-files` | off | Keep downloaded files after analysis |
| `--verbose` | off | Print progress to stderr |
| `--skip-harvest` | off | Skip the GitHub harvest stage |
| `--skip-download` | off | Skip the download stage |
| `--skip-analyze` | off | Skip the analysis stage |
| `--skip-db` | off | Skip the database load stage |

## Dashboard

After running the pipeline, launch the interactive dashboard:

```bash
streamlit run dashboard/app.py
```

## Project Structure

```
NetVulnPy/
  main.py                  # Pipeline orchestrator
  github_repo_harvester.py # Stage 1: GitHub API harvester
  repo_downloader.py       # Stage 2: ZIP download and extraction
  repo_analyzer.py         # Stage 3: Bandit / Semgrep analysis
  db_loader.py             # Stage 4: SQLite loader
  data.py                  # Data access layer
  dashboard/               # Streamlit dashboard app
  seminar/                 # Academic paper (ACM sigconf LaTeX)
  requirements.txt         # Python dependencies
```

## Requirements

- Python 3.10+
- [Bandit](https://bandit.readthedocs.io/) (installed via `pip`)
- [Semgrep](https://semgrep.dev/) (installed via `pip`, optional)
