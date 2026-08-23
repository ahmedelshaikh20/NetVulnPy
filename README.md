# NetVulnPy

Large-scale vulnerability analysis of open-source Python networking repositories using static application security testing and LLM-based analysis.

NetVulnPy is an automated pipeline that harvests Python networking repos from GitHub, scans them with Bandit, Semgrep, pip-audit, Skylos, or an LLM-based analyzer, and loads the findings into a SQLite database with an interactive Streamlit dashboard.

## Pipeline Stages

1. **Harvest** -- query GitHub API for Python networking repos -> `repos.json`
2. **Download** -- download ZIPs and extract `.py` files -> `downloads/`
3. **Analyze** -- run Bandit, Semgrep, pip-audit, Skylos, or LLM on extracted files -> `results/`
4. **Load DB** -- import results into `findings.sqlite`
5. **Triage** (optional) -- LLM-assisted false-positive classification

## Installation

```bash
# Clone the repository
git clone https://github.com/ahmedelshaikh20/NetVulnPy.git
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

### LLM API Key (for LLM analyzer)

Set the API key for the LLM inference endpoint:

```bash
export LLM_API_KEY=your_key_here
```

Or pass it via `--llm-api-key`.

## Usage

```bash
# Full pipeline with Bandit (default), 100 repos
python main.py --max-repos 100 --verbose

# Full pipeline with Semgrep
python main.py --max-repos 100 --analyzer semgrep --verbose

# Full pipeline with LLM analyzer
python main.py --max-repos 100 --analyzer llm --verbose

# Full pipeline with pip-audit (dependency vulnerabilities)
python main.py --max-repos 100 --analyzer pip-audit --verbose

# Full pipeline with Skylos
python main.py --max-repos 100 --analyzer skylos --verbose

# Limit to 10 repos
python main.py --max-repos 10 --limit 10 --verbose

# Re-run analysis only (skip harvest and download)
python main.py --skip-harvest --skip-download --verbose

# Re-run DB load only
python main.py --skip-harvest --skip-download --skip-analyze --verbose

# Run with LLM triage on findings
python main.py --skip-harvest --skip-download --triage --verbose
```

### Command-Line Arguments

| Argument | Default | Description |
|---|---|---|
| `--analyzer {bandit,semgrep,skylos,pip-audit,llm}` | `bandit` | Security analyzer to use |
| `--max-repos N` | `2000` | Max repos to harvest from GitHub |
| `--limit N` | all | Max repos to download/analyze |
| `--token TOKEN` | `GITHUB_TOKEN` env | GitHub personal access token |
| `--output-dir DIR` | `.` | Root output directory |
| `--workers N` | `4` | Parallel workers for download/analyze stages |
| `--db PATH` | `findings.sqlite` | SQLite output path |
| `--keep-files` | off | Keep downloaded files after analysis |
| `--verbose` | off | Print progress to stderr |
| `--skip-harvest` | off | Skip the GitHub harvest stage |
| `--skip-download` | off | Skip the download stage |
| `--skip-analyze` | off | Skip the analysis stage |
| `--skip-db` | off | Skip the database load stage |
| `--triage` | off | Run LLM-assisted false-positive triage |
| `--llm-api-key KEY` | `LLM_API_KEY` env | API key for LLM inference endpoint |
| `--llm-endpoint URL` | `https://llms.innkube.fim.uni-passau.de` | OpenAI-compatible LLM endpoint |
| `--llm-model MODEL` | `qwen3-next-80b-a3b-instruct` | Model name for LLM analyzer |

## Dashboard

After running the pipeline, launch the interactive dashboard:

```bash
streamlit run dashboard/app.py
```

The dashboard includes an **Analyzer** filter in the sidebar to switch between results from different tools (bandit, semgrep, llm, pip-audit, skylos).

## Validation

Validate the results of each analyzer:

```bash
# Schema and consistency validation
python validate_results.py --verbose

# Ground-truth validation: sample 100 findings per analyzer,
# verify against actual source code using LLM
python sample_validation.py --verbose

# Validate a single analyzer
python sample_validation.py --analyzer llm --verbose

# Detailed LLM findings validation with custom sample size
python validate_llm_findings.py --all --sample 50 --verbose
```

## Project Structure

```
NetVulnPy/
  main.py                  # Pipeline orchestrator
  github_repo_harvester.py # Stage 1: GitHub API harvester
  repo_downloader.py       # Stage 2: ZIP download and extraction
  repo_analyzer.py         # Stage 3: Bandit / Semgrep / LLM / pip-audit / Skylos
  db_loader.py             # Stage 4: SQLite loader
  llm_triage.py            # Stage 5: LLM-assisted false-positive triage
  data.py                  # Data access layer (dashboard queries)
  validate_results.py      # Schema and consistency validation
  validate_llm_findings.py # Ground-truth validation (all analyzers)
  sample_validation.py     # 100-sample precision validation per analyzer
  dashboard/               # Streamlit dashboard app
    app.py                 # Entry point
    sidebar.py             # Sidebar filters (severity, stars, analyzer)
    pages/                 # Dashboard pages (overview, explorer, correlation, etc.)
  seminar/                 # Academic paper (ACM sigconf LaTeX)
  requirements.txt         # Python dependencies
```

## Requirements

- Python 3.10+
- [Bandit](https://bandit.readthedocs.io/) (installed via `pip`)
- [Semgrep](https://semgrep.dev/) (installed via `pip`, optional)
- [pip-audit](https://github.com/pypa/pip-audit) (installed via `pip`, optional)
- [Skylos](https://github.com/nicoroos/skylos) (installed via `pip`, optional)
- [OpenAI Python SDK](https://github.com/openai/openai-python) (for LLM analyzer, installed via `pip`)
