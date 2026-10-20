"""
Branch operations for benchmarking version control databases.

Create, connect to, and delete branches through the git-like DBToolSuite
API. Connect is measured as an exec() with an empty script on the target
branch, so the CONNECT row carries the switch cost.

The other verbs (commit, diff, log, merge, rebase, revert, reset) each need
a branch in a particular state. That state is built untimed, so the only
row an operation records is the verb itself. A verb the backend does not
implement is recorded as UNSUPPORTED and the run continues.
"""

from typing import TYPE_CHECKING

from dblib import result_pb2 as rslt
from microbench.operations.base import Operation

if TYPE_CHECKING:
    from microbench.runner2 import WorkerContext


class BranchCreateOperation(Operation):
    """Create a new branch from the worker's current branch.

    The branch name is generated from the thread ID and a shared counter.
    """

    def __init__(self):
        pass

    def _generate_branch_name(self, context: 'WorkerContext') -> str:
        branch_id = context.get_next_branch_id()
        return f"branch_tid{context.thread_id}_{branch_id}"

    def execute(self, context: 'WorkerContext') -> None:
        branch_name = self._generate_branch_name(context)
        # The row carries the branches existing before this create (shared
        # list, so approximate under concurrency).
        context.record_branch_count(context.live_branch_count())
        context.db_tools.branch(
            branch_name,
            from_ref=context.current_ref,
            storage=context.measure_storage,
        ).raise_for_status()
        context.add_branch(branch_name)
        # Every k-th new branch is also connected to (a timed CONNECT row:
        # on backends with a compute per branch, its start), then the worker
        # returns untimed to the branch it creates from.
        cfg = getattr(context, "config", None)
        k = cfg.branch_op_config.connect_every if cfg is not None else 0
        if k > 0:
            self._created = getattr(self, "_created", 0) + 1
            if self._created % k == 0:
                origin = context.current_branch
                context.connect(branch_name)
                context.connect(origin, timed=False)

    def requires_setup_data(self) -> bool:
        return False  # Can create branches without data

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.BRANCH


class BranchConnectOperation(Operation):
    """Switch the worker's connection to a random existing branch."""

    def __init__(self):
        pass

    def _select_branch(self, context: 'WorkerContext') -> str:
        branch_to_connect = context.get_random_branch()
        if not branch_to_connect:
            raise ValueError("No branches available to connect to")
        return branch_to_connect

    def execute(self, context: 'WorkerContext') -> None:
        branch_to_connect = self._select_branch(context)
        context.connect(branch_to_connect)
        context.clear_pk_cache()

    async def execute_async(self, context: 'WorkerContext') -> None:
        branch_to_connect = self._select_branch(context)
        await context.connect_async(branch_to_connect)
        context.clear_pk_cache()

    def requires_setup_data(self) -> bool:
        return True  # Needs branches to exist from setup

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.CONNECT


class BranchDeleteOperation(Operation):
    """Delete a random branch other than the worker's current one."""

    def __init__(self):
        pass

    def _select_branch_to_delete(self, context: 'WorkerContext') -> str:
        """A random branch other than the worker's current one and the
        backend's root branch (Dolt refuses to delete its default)."""
        excluded = {context.current_branch, context.root_branch}
        candidates = [b for b in context.get_all_branches() if b not in excluded]
        if not candidates:
            raise ValueError("No other branches available to delete")
        return context.rnd.choice(candidates)

    def execute(self, context: 'WorkerContext') -> None:
        branch_to_delete = self._select_branch_to_delete(context)
        context.db_tools.delete(
            branch_to_delete, storage=context.measure_storage
        ).raise_for_status()
        context.remove_branch(branch_to_delete)

    def requires_setup_data(self) -> bool:
        return True  # Needs branches to exist from setup

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.DELETE


# ----------------------------------------------------------------------------
# Verbs that need a prepared state
# ----------------------------------------------------------------------------


def _change(context: 'WorkerContext', table_name: str, rows: int) -> None:
    """Insert ``rows`` rows into the current branch, untimed."""
    for _ in range(rows):
        context._insert_without_timing(table_name)


def _forget_changes(context: 'WorkerContext', rows: int) -> None:
    """Drop the last ``rows`` keys _change() tracked on the current branch;
    the verb just removed them."""
    keys = context._modified_keys.get(context.current_branch)
    if keys:
        del keys[-rows:]


def _commit(context: 'WorkerContext', message: str) -> str:
    """Commit the current branch, untimed; returns the commit id."""
    return context.db_tools.commit(
        context.current_ref, message, timed=False, storage=False,
        raise_on_error=True,
    ).value


def _fork(context: 'WorkerContext', table_name: str, rows: int) -> str:
    """Create a child of the current branch holding ``rows`` committed
    inserts, untimed. The worker ends up back on the parent."""
    parent = context.current_branch
    child = f"op_tid{context.thread_id}_{context.get_next_branch_id()}"
    context.db_tools.branch(
        child, from_ref=context.current_ref, timed=False, storage=False,
        raise_on_error=True,
    )
    context.connect(child, timed=False)
    try:
        _change(context, table_name, rows)
        _commit(context, f"{child}: {rows} rows")
    finally:
        context._modified_keys.pop(child, None)
        context.connect(parent, timed=False)
    return child


def _drop(context: 'WorkerContext', branch: str) -> None:
    """Delete a branch _fork() made, untimed. Best effort."""
    context.db_tools.delete(branch, timed=False, storage=False)


def _return_to(context: 'WorkerContext', branch: str) -> None:
    """Put the worker back on ``branch``, untimed. A verb or a delete may
    leave the connection on another branch, or on none."""
    ref = context.db_tools.current_ref
    if ref is None or ref.branch != branch or ref.commit:
        context.connect(branch, timed=False)


class _VerbOperation(Operation):
    """Base for one git-like verb measured on a prepared state."""

    VERB = ""
    OP_TYPE = rslt.OpType.UNSPECIFIED

    def __init__(self, table_name: str, changes_per_op: int = 1):
        self.table_name = table_name
        self.changes = max(1, changes_per_op)

    def execute(self, context: 'WorkerContext') -> None:
        if not context.db_tools.supports(self.VERB):
            # Nothing to prepare: the verb records its UNSUPPORTED row.
            self._unsupported(context)
            return
        result = self._run(context)
        if result.failed:
            result.raise_for_status()

    def _unsupported(self, context: 'WorkerContext') -> None:
        ref = context.current_ref
        getattr(context.db_tools, self.VERB)(ref, ref)

    def _run(self, context: 'WorkerContext'):
        """Prepare untimed, run the timed verb, return its OpResult."""
        raise NotImplementedError

    def requires_setup_data(self) -> bool:
        return False  # Builds the state it needs

    def get_operation_type(self) -> rslt.OpType:
        return self.OP_TYPE


class BranchCommitOperation(_VerbOperation):
    """Commit ``changes_per_op`` freshly inserted rows on the current branch."""

    VERB = "commit"
    OP_TYPE = rslt.OpType.COMMIT

    def _unsupported(self, context: 'WorkerContext') -> None:
        context.db_tools.commit(context.current_ref)

    def _run(self, context: 'WorkerContext'):
        _change(context, self.table_name, self.changes)
        return context.db_tools.commit(
            context.current_ref, "microbench commit",
            storage=context.measure_storage,
        )


class BranchDiffOperation(_VerbOperation):
    """Diff the current branch against a child that is ``changes_per_op``
    committed rows ahead. The child is made once per branch and reused."""

    VERB = "diff"
    OP_TYPE = rslt.OpType.DIFF

    def __init__(self, table_name: str, changes_per_op: int = 1):
        super().__init__(table_name, changes_per_op)
        self._children = {}  # parent branch -> child branch

    def _run(self, context: 'WorkerContext'):
        parent = context.current_branch
        if parent not in self._children:
            self._children[parent] = _fork(context, self.table_name, self.changes)
        return context.db_tools.diff(
            context.current_ref, self._children[parent],
            storage=context.measure_storage,
        )


class BranchLogOperation(_VerbOperation):
    """Read the last ``log_limit`` commits of the current branch, which is
    given ``history_commits`` commits first."""

    VERB = "log"
    OP_TYPE = rslt.OpType.LOG

    def __init__(self, table_name: str, changes_per_op: int = 1,
                 history_commits: int = 10, log_limit: int = 10):
        super().__init__(table_name, changes_per_op)
        self.history_commits = history_commits
        self.log_limit = log_limit
        self._prepared = set()  # branches that already have their history

    def _unsupported(self, context: 'WorkerContext') -> None:
        context.db_tools.log(context.current_ref, self.log_limit)

    def _run(self, context: 'WorkerContext'):
        branch = context.current_branch
        if branch not in self._prepared:
            for i in range(self.history_commits):
                _change(context, self.table_name, self.changes)
                _commit(context, f"history {i}")
            self._prepared.add(branch)
        return context.db_tools.log(
            context.current_ref, self.log_limit,
            storage=context.measure_storage,
        )


class BranchMergeOperation(_VerbOperation):
    """Merge a child into the current branch. Both sides hold
    ``changes_per_op`` committed rows the other lacks, with different keys,
    so the merge is neither a fast-forward nor a conflict."""

    VERB = "merge"
    OP_TYPE = rslt.OpType.MERGE

    def _run(self, context: 'WorkerContext'):
        parent = context.current_branch
        child = _fork(context, self.table_name, self.changes)
        try:
            _change(context, self.table_name, self.changes)
            _commit(context, "parent moves on")
            return context.db_tools.merge(
                parent, child, f"merge {child}",
                storage=context.measure_storage,
            )
        finally:
            _drop(context, child)
            _return_to(context, parent)


class BranchRebaseOperation(_VerbOperation):
    """Rebase a child onto the current branch after the current branch
    moved on. Same shape as BranchMergeOperation, replayed the other way."""

    VERB = "rebase"
    OP_TYPE = rslt.OpType.REBASE

    def _run(self, context: 'WorkerContext'):
        parent = context.current_branch
        child = _fork(context, self.table_name, self.changes)
        try:
            _change(context, self.table_name, self.changes)
            _commit(context, "parent moves on")
            return context.db_tools.rebase(
                child, parent, storage=context.measure_storage,
            )
        finally:
            _drop(context, child)
            _return_to(context, parent)


class BranchRevertOperation(_VerbOperation):
    """Revert the commit that added ``changes_per_op`` rows to the current
    branch. The branch gets one earlier commit first, so the reverted
    commit is never the branch's first."""

    VERB = "revert"
    OP_TYPE = rslt.OpType.REVERT

    def __init__(self, table_name: str, changes_per_op: int = 1):
        super().__init__(table_name, changes_per_op)
        self._prepared = set()  # branches that already have a base commit

    def _unsupported(self, context: 'WorkerContext') -> None:
        context.db_tools.revert(context.current_ref, "")

    def _run(self, context: 'WorkerContext'):
        branch = context.current_branch
        if branch not in self._prepared:
            _change(context, self.table_name, self.changes)
            _commit(context, "base")
            self._prepared.add(branch)
        _change(context, self.table_name, self.changes)
        commit = _commit(context, "to revert")
        result = context.db_tools.revert(
            branch, commit, storage=context.measure_storage,
        )
        _return_to(context, branch)
        if result.ok:
            _forget_changes(context, self.changes)
        return result


class BranchResetOperation(_VerbOperation):
    """Reset the current branch to a commit that is one commit of
    ``changes_per_op`` rows behind its head."""

    VERB = "reset"
    OP_TYPE = rslt.OpType.RESET

    def _unsupported(self, context: 'WorkerContext') -> None:
        context.db_tools.reset(context.current_ref, "")

    def _run(self, context: 'WorkerContext'):
        branch = context.current_branch
        _change(context, self.table_name, self.changes)
        target = _commit(context, "reset target")
        _change(context, self.table_name, self.changes)
        _commit(context, "to discard")
        result = context.db_tools.reset(
            branch, target, storage=context.measure_storage,
        )
        _return_to(context, branch)
        if result.ok:
            _forget_changes(context, self.changes)
        return result
