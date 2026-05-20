# rda-python-common

Python common library codes to be shared by other RDA python utility programs.

## Environment setup

### Option A — Python venv (DECS machines)

```bash
python3 -m venv $ENVHOME          # e.g. /glade/u/home/gdexdata/gdexmsenv
source $ENVHOME/bin/activate
pip install rda_python_common
```

### Option B — Conda (DAV/Casper)

```bash
conda create -n pg-gdex python=3.10
conda activate pg-gdex            # e.g. /glade/work/gdexdata/conda-envs/pg-gdex
pip install rda_python_common
```

The conda environment is typically at `/glade/work/gdexdata/conda-envs/pg-gdex`.

## Installing and using in another RDA python repo

`rda-python-common` is the foundation that every other `rda-python-*` repo
builds on.  To consume it from a new or existing repo, follow these steps.

### 1. Install the package

For local development, clone this repo alongside your project and install it
in editable mode so that changes are picked up without re-installing:

```bash
git clone https://github.com/NCAR/rda-python-common.git
cd rda-python-common
pip install -e .
```

For a regular (non-editable) install from a checkout:

```bash
pip install /path/to/rda-python-common
```

For a production install on a system that uses the published distribution:

```bash
pip install rda_python_common
```

The package brings in its own transitive dependencies (`psycopg2-binary`,
`rda-python-globus`, `unidecode`, `hvac`).

### 2. Declare it as a dependency in your project

Add `rda_python_common` to the `dependencies` list of your project's
`pyproject.toml` so that downstream installs pull it in automatically:

```toml
[project]
name = "rda_python_yourtool"
version = "0.1.0"
dependencies = [
  "rda_python_common",
  # ... other deps
]
```

This is the same pattern used by `rda-python-dsarch`, `rda-python-dsupdt`,
`rda-python-dsrqst`, `rda-python-dscheck`, `rda-python-metrics`, and
`rda-python-miscs`.

### 3. Import the modules you need

Two import styles are supported (see [Usage examples](#usage-examples) below):

```python
# Preferred for new code -- import the class from the lower-case module
from rda_python_common.pg_log import PgLOG
from rda_python_common.pg_dbi import PgDBI

# Legacy module-style imports remain supported for back-compatibility
from rda_python_common import PgLOG, PgDBI
PgLOG.pglog("hello", PgLOG.LOGWRN)
```

### 4. Verify the install

```bash
python -c "import rda_python_common; print(rda_python_common.__version__)"
```

You should see the installed version (currently `2.1.13`).  If the import
fails, double-check that the active Python environment is the one where you
ran `pip install`.

## Modules

All shared functionality lives under `src/rda_python_common/` and is organised as
a single-inheritance class hierarchy.  Each module defines exactly one class;
later classes extend earlier ones, so an application that instantiates the
top-of-chain class (typically `PgOPT` or `PgCMD`) gets every helper through one
object.

Inheritance tree (top-down; multi-inheritance shown as two arrows
converging on the same child):

```
                          PgLOG
                       ┌────┴────┐
                       ▼         ▼
                    PgUtil     PgDBI
                     │ │        │ │ │
                     │ └────┐ ┌─┘ │ └─► PgPassword
                     │      ▼ ▼   │
                     │    PgSplit │       (multi-inherits
                     │            │        PgUtil + PgDBI)
                     │            ▼
                     │          PgSIG
                     │            │
                     │ ┌──────────┘
                     ▼ ▼
                   PgFile                 (multi-inherits
                     │                     PgUtil + PgSIG)
                     ├─► PgOPT
                     │
                     └─► PgLock
                          │
                          └─► PgCMD
```

The tree is single inheritance everywhere except at two join points:

- **`PgFile(PgUtil, PgSIG)`** — combines date/record utilities (`PgUtil`
  via `PgLOG`) with daemon/signal/DB control (`PgSIG` → `PgDBI` → `PgLOG`),
  so its descendants `PgOPT`, `PgLock`, and `PgCMD` inherit logging, DB,
  util, signal, and file facilities through one MRO.
- **`PgSplit(PgUtil, PgDBI)`** — combines record-manipulation helpers
  (`PgUtil`) with the `pgadd`/`pgget`/`pgmget`/`pgupdt`/`pgdel` DB
  operations (`PgDBI`) it needs to keep the shared `wfile` table and the
  per-dataset `wfile_<dsid>` partitions in sync.

- **`pg_log.py`** — `PgLOG`.  Root of the hierarchy.  Provides the central
  logging facility (bit-mask `logact` flags such as `MSGLOG`, `WARNLG`,
  `ERRLOG`, `EXITLG`), e-mail dispatch, system-command execution, process
  metadata lookup, and the global `PGLOG` settings dictionary used by every
  other module.

- **`pg_util.py`** — `PgUtil(PgLOG)`.  Miscellaneous date/time, dataset-ID,
  and column-oriented record-manipulation helpers.  Holds the `DATEFMTS`
  regex table, `MONTHS`/`MNS`/`WDAYS`/`WDS` lookup lists, and the `MDAYS`
  days-per-month array used for date arithmetic, formatting, parsing, and
  record sort/search/classification across all RDA tools.

- **`pg_file.py`** — `PgFile(PgUtil, PgSIG)`.  Unified file-operation layer
  spanning local file systems, remote hosts (rsync/ssh/scp), AWS S3 / object
  store, and Globus endpoints.  Used by `rdacp`, `dsarch`, `dsupdt`, and
  related tools whenever data is moved, listed, or stat-ed.

- **`pg_lock.py`** — `PgLock(PgFile)`.  RDADB record-locking primitives for
  the `dscheck`, `dsrqst`, `dlupdt`, `dcupdt`, `ptrqst`, and `dataset`
  tables.  Acquires, refreshes, and releases per-record locks so that
  long-running batch jobs coordinate cleanly.

- **`pg_dbi.py`** — `PgDBI(PgLOG)`.  PostgreSQL database interface built on
  `psycopg` (v3 by default, with `psycopg2` as an opt-in fallback).  Wraps
  connection management, batch `INSERT`/`SELECT`/
  `UPDATE`/`DELETE`, transaction control, and credential lookup from
  `.pgpass` or OpenBao.  All RDA tools talk to the `rdadb` database through
  this class.

- **`pg_sig.py`** — `PgSIG(PgDBI)`.  Daemon process control, POSIX signal
  handling, child/background-process management, and PBS/Torque batch-job
  status queries.  Provides the `PGSIG` runtime dictionary plus `VUSERS`,
  `CPIDS`, `CBIDS`, and `SDUMP` tables that drive RDA daemon programs.

- **`pg_cmd.py`** — `PgCMD(PgLock)`.  Manages `dscheck` batch and delayed-
  mode command tracking.  Records, updates, and reaps the per-command rows
  that let RDA utilities resume or be monitored across PBS batch jobs.

- **`pg_split.py`** — `PgSplit(PgUtil, PgDBI)`.  Synchronises `wfile` records
  between the shared `wfile` table and the per-dataset `wfile_<dsid>`
  partition tables.  Provides compare/add/update/delete helpers used when
  archiving or reconciling dataset file inventories.

- **`pg_opt.py`** — `PgOPT(PgFile)`.  Command-line option parsing and
  application configuration framework for RDA tools (`dsarch`, `dsupdt`,
  `dsrqst`, ...).  Holds the master `OPTS` definition table, parsed
  `params`, command-line vs. input-file option tracking (`CMDOPTS`/
  `INOPTS`), output formatting, dataset/help/media/storage/backup type
  maps, and the global `PGOPT` settings.

- **`pgpassword.py`** — `PgPassword(PgDBI)`.  Standalone CLI entry point
  (`pgpassword`) that resolves a PostgreSQL login password from OpenBao
  (`get_baopassword`) or `~/.pgpass` (`get_pgpassword()`) given database/schema/
  host/port/user selectors via `-d`, `-c`, `-h`, `-p`, `-u`, `-l`, `-k`.
  Prints the resolved password to stdout so shell scripts can capture it.

## Usage examples

Each class lives in its own submodule.  Import the class you need, then
either instantiate it directly or subclass it to add application-specific
state and methods.

### 1. Direct instantiation — use the helpers as-is

```python
# Logging only
from rda_python_common.pg_log import PgLOG

log = PgLOG()
log.pglog("dsarch started", log.LOGWRN)

# Database access (PgDBI inherits PgLOG, so you get logging too)
from rda_python_common.pg_dbi import PgDBI

db = PgDBI()
rec = db.pgget('dataset', 'dsid, title', "dsid = 'd633000'")
print(rec)
```

### 2. Subclassing a single common class

```python
# A small utility that needs date/record helpers plus logging.
from rda_python_common.pg_util import PgUtil

class DateReport(PgUtil):
   def __init__(self):
      super().__init__()           # initialise PgUtil (and PgLOG)
      self.today = self.curtime()  # method inherited from PgUtil

   def run(self):
      self.pglog(f"report date: {self.today}", self.LOGWRN)

DateReport().run()
```

### 3. Subclassing one of the multi-inheriting joins

```python
# A worker that needs file I/O (PgFile) and dscheck command tracking (PgCMD).
# PgCMD already extends PgFile via PgLock, so a single base is enough.
from rda_python_common.pg_cmd import PgCMD

class Worker(PgCMD):
   def __init__(self):
      super().__init__()
      self.jobs = []

   def archive_one(self, src, dst):
      # PgFile method, available through the inheritance chain
      self.local_copy_local(src, dst)
      # PgDBI method, available through PgCMD -> PgLock -> PgFile -> PgSIG -> PgDBI
      self.pgupdt('wfile', {'status': 'A'}, f"wfile = '{dst}'")

Worker().archive_one('/in/file', '/out/file')
```

### 4. Combining multiple common classes (application action class)

This mirrors how RDA tools such as `dsarch` are structured.  The leaf class
multi-inherits several common classes so a single object exposes options,
command tracking, and wfile splitting.

```python
# Excerpt of the pattern used by rda_python_dsarch/dsarch.py
from rda_python_common.pg_opt   import PgOPT
from rda_python_common.pg_cmd   import PgCMD
from rda_python_common.pg_split import PgSplit

class PgArch(PgOPT, PgCMD, PgSplit):
   """Shared state + helpers for a CLI archiving tool."""
   def __init__(self):
      super().__init__()
      self.RTPATH = {}          # runtime path cache
      self.OPTS   = {}          # option table (populated by subclass)

class DsArch(PgArch):
   def __init__(self):
      super().__init__()
      self.ALLCNT = self.ADDCNT = self.MODCNT = 0

   def main(self):
      self.read_parameters()    # from PgOPT
      self.start_actions()      # dispatch

if __name__ == "__main__":
   DsArch().main()
```

### 5. Reading a PostgreSQL password from OpenBao or ~/.pgpass

```python
from rda_python_common.pgpassword import PgPassword

pw = PgPassword()
pw.default_scinfo('rdadb', 'dssdb', 'rda-pgdb', 'gdexweb', None, 5432)
password = pw.get_baopassword() or pw.get_pgpassword()
```

In every case `super().__init__()` cooperates correctly across the
multi-inheriting joins (`PgFile` and `PgSplit`), so subclasses only need
to call it once.
