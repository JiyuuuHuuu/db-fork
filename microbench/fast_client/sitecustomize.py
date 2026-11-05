"""Loads microbench/fast_client/patches.py into every Python process of a runner command, the runner's spawned client
processes included (they inherit the environment). Python imports `sitecustomize` at startup from sys.path, so
this file acts only when this directory is on PYTHONPATH, and patches only when BENCH_FAST_CLIENT=1 (not a DBFORK_
name: scripts/workloads/common.sh clears every DBFORK_* variable before a run):

    BENCH_FAST_CLIENT=1 PYTHONPATH=$PWD/microbench/fast_client scripts/workloads/scaling_data.sh ...

It installs an import hook that applies each patch right after its target module is first imported, so processes
that never import the runner (neon_ctl, CPU agents, ...) pay nothing. `python -m microbench.runner2` (how
scripts/run_throughput_bench.sh starts the runner) would run the runner as __main__, which no import hook sees:
runpy._run_module_as_main is wrapped so that, for that one module, it imports microbench.runner2 as a regular
(patched) module and calls its main(). The runner's spawned client processes then unpickle their targets from
microbench.runner2 as well, and are patched by the same hook (they inherit the environment). If another
sitecustomize exists further down sys.path (none in this repo's venv), it is chained.
"""
import importlib.abc
import importlib.machinery
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))


def _chain_next_sitecustomize():
    import importlib.util
    for p in sys.path:
        if os.path.abspath(p or ".") == _HERE:
            continue
        cand = os.path.join(p or ".", "sitecustomize.py")
        if os.path.isfile(cand):
            spec = importlib.util.spec_from_file_location("_chained_sitecustomize", cand)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return


class _PatchAfterImport(importlib.abc.MetaPathFinder):
    def __init__(self, targets):
        self.targets = targets

    def find_spec(self, fullname, path, target=None):
        if fullname not in self.targets:
            return None
        for finder in sys.meta_path:
            if finder is self or not hasattr(finder, "find_spec"):
                continue
            spec = finder.find_spec(fullname, path, target)
            if spec is not None and spec.loader is not None:
                break
        else:
            return None
        loader, patch = spec.loader, self.targets[fullname]

        class _Loader(importlib.abc.Loader):
            def create_module(self, s):
                return loader.create_module(s)

            def exec_module(self, module):
                loader.exec_module(module)
                patch(module)

        spec.loader = _Loader()
        return spec


def _install():
    import importlib.util
    spec = importlib.util.spec_from_file_location("_dbfork_fast_client_patches", os.path.join(_HERE, "patches.py"))
    patches = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = patches
    spec.loader.exec_module(patches)
    sys.meta_path.insert(0, _PatchAfterImport(patches.TARGETS))
    import runpy
    orig_run = runpy._run_module_as_main

    def _run_module_as_main(mod_name, alter_argv=True):
        if mod_name != "microbench.runner2":
            return orig_run(mod_name, alter_argv)
        import importlib
        mod = importlib.import_module(mod_name)
        if alter_argv:
            sys.argv[0] = mod.__file__
        return mod.main()

    runpy._run_module_as_main = _run_module_as_main
    for name, patch in patches.TARGETS.items():  # already imported (unusual at startup)
        if name in sys.modules:
            patch(sys.modules[name])


if os.environ.get("BENCH_FAST_CLIENT") == "1":
    _install()
_chain_next_sitecustomize()
