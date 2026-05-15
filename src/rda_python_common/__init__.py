"""rda_python_common: shared utility package for RDA Python tools.

This package exposes two parallel APIs:

1. Legacy module-based API (back-compat). Import the capitalized submodules
   and call their module-level functions, e.g.::

       from rda_python_common import PgLOG
       PgLOG.pglog("message", PgLOG.LOGWRN)

2. Class-based API (preferred for new code). Import the class from the
   lower-case module and either instantiate or subclass it, e.g.::

       from rda_python_common.pg_log import PgLOG
       log = PgLOG()
       log.pglog("message", log.LOGWRN)

The legacy submodules are eagerly imported below so that
``from rda_python_common import PgLOG`` continues to return the module
object that existing callers expect.
"""

from . import PgLOG, PgUtil, PgDBI, PgFile, PgLock, PgCMD, PgSIG, PgOPT, PgSplit

__version__ = "2.1.9"

__all__ = [
   "PgLOG",
   "PgUtil",
   "PgDBI",
   "PgFile",
   "PgLock",
   "PgCMD",
   "PgSIG",
   "PgOPT",
   "PgSplit",
   "__version__",
]
