"""
Runner2: Clean microbenchmark runner for single-operation benchmarks.

This module provides a cleaner, more maintainable architecture for running
database microbenchmarks with clear separation of concerns:
- Configuration loading and validation
- Backend setup and cleanup
- Untimed setup phase (branches + data)
- Timed execution phase (measure operations)
- Result collection and reporting

Usage:
    python -m microbench.runner2 --config path/to/config.textproto

Design principles:
- Single Responsibility: Each class has one clear purpose
- Extensibility: Easy to add new operation types
- Testability: Components can be tested independently
- Clarity: Clear execution flow from config to results
"""

import argparse
import re
import asyncio
import json
import os
import random
import sys
import time
import threading
import multiprocessing as mp
import queue
import resource
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, List
from contextlib import contextmanager

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from google.protobuf import text_format
from tqdm import tqdm

# Protobuf imports
from microbench import task2_pb2 as tp
from dblib import result_pb2 as rslt

# Database backend imports
from dblib import dolt
from dblib.dolt import DoltToolSuite, commit_dolt_schema
from dblib import dolt_mysql
from dblib.dolt_mysql import DoltMySQLToolSuite
from dblib import seekdb
from dblib.seekdb import SeekDBToolSuite
from dblib import matrixone
from dblib.matrixone import MatrixOneToolSuite
from dblib import bauplan
from dblib.bauplan import BauplanToolSuite
from dblib import neon as neon_mod
from dblib.neon import NeonToolSuite
from dblib.file_copy import FileCopyToolSuite
from dblib.chronos import ChronosToolSuite
from dblib import chronos as chronos_mod
from dblib.xata import XataToolSuite
from dblib import terminusdb
from dblib.terminusdb import TerminusDBToolSuite
from dblib import neon_local
from dblib.neon_local import NeonLocalToolSuite
from dblib.db_api import DBToolSuite, Ref
from dblib import result_collector as rc

# Operation imports
from microbench.operations import (
    OperationRegistry,
    ReadOperation,
    InsertOperation,
    UpdateOperation,
    DeleteOperation,
    RangeReadOperation,
    RangeUpdateOperation,
    MixedOperation,
    BranchCreateOperation,
    BranchConnectOperation,
    BranchDeleteOperation,
    BranchCommitOperation,
    BranchDiffOperation,
    BranchLogOperation,
    BranchMergeOperation,
    BranchRebaseOperation,
    BranchRevertOperation,
    BranchResetOperation,
    AddIndexOperation,
    RemoveIndexOperation,
    VacuumOperation,
    AddColumnOperation,
    RemoveColumnOperation,
    BackfillOperation,
    AddColumnWithDefaultOperation,
    TypeChangeOperation,
    RenameColumnOperation,
    RenameTableOperation,
    AddForeignKeyOperation,
    AddCheckOperation,
    SetDefaultOperation,
    SetNotNullOperation,
    ExpandContractOperation,
)

# Git-like verbs measured on a state the operation prepares itself
VERB_OPERATIONS = frozenset({
    tp.OperationType.BRANCH_COMMIT,
    tp.OperationType.BRANCH_DIFF,
    tp.OperationType.BRANCH_LOG,
    tp.OperationType.BRANCH_MERGE,
    tp.OperationType.BRANCH_REBASE,
    tp.OperationType.BRANCH_REVERT,
    tp.OperationType.BRANCH_RESET,
})

# Utility imports
from microbench.datagen import DynamicDataGenerator
from util import db_helpers as dbh
from util.import_db import load_sql_file


# ============================================================================
# Configuration
# ============================================================================


class BenchmarkConfig:
    """Loads and validates task configuration from protobuf.

    This class encapsulates all configuration logic, making it easy to
    access configuration values throughout the benchmark code.
    """

    def __init__(self, config_proto: tp.TaskConfig):
        """Initialize from parsed protobuf config.

        Args:
            config_proto: Parsed TaskConfig protobuf message
        """
        self._proto = config_proto
        self._validate()

    @classmethod
    def load_from_file(cls, config_path: str) -> 'BenchmarkConfig':
        """Load configuration from a textproto file.

        Args:
            config_path: Path to the .textproto config file

        Returns:
            BenchmarkConfig instance

        Raises:
            FileNotFoundError: If config file doesn't exist
            Exception: If config parsing fails
        """
        config = tp.TaskConfig()
        with open(config_path, 'r') as f:
            text_format.Parse(f.read(), config)
        return cls(config)

    def _validate(self) -> None:
        """Validate configuration values.

        Raises:
            ValueError: If configuration is invalid
        """
        # Basic required fields
        if not self._proto.run_id:
            raise ValueError("run_id is required")
        if not self._proto.database_setup.db_name:
            raise ValueError("database_setup.db_name is required")

        # Validate benchmark mode
        if not self._proto.HasField("operation_benchmark"):
            raise ValueError("operation_benchmark is required")

        op_bench = self._proto.operation_benchmark
        if op_bench.num_ops <= 0:
            raise ValueError("operation_benchmark.num_ops must be > 0")

        # Validate storage measurement
        if self._proto.measure_storage and self._proto.num_threads > 1:
            raise ValueError(
                "measure_storage is incompatible with num_threads > 1. "
                "Concurrent writes pollute per-thread storage deltas."
            )

        # Backend-specific validation
        if self._proto.backend == tp.Backend.NEON:
            db_setup = self._proto.database_setup
            source_type = db_setup.WhichOneof("source")
            if source_type == "existing_db":
                if not db_setup.existing_db.neon_project_id:
                    raise ValueError(
                        "neon_project_id required when reusing existing Neon database"
                    )

        # Worker processes get the backend info by pickling; the file_copy,
        # chronos and terminusdb infos hold locks or clients that do not pickle.
        if self._proto.num_processes > 1 and self._proto.backend in (
                tp.Backend.FILE_COPY, tp.Backend.CHRONOS, tp.Backend.TERMINUSDB):
            raise ValueError("num_processes > 1 is not supported on this backend")

        # A seed database to copy from needs the Dolt revision-database naming.
        if (self.source == "generated" and self.generated.seed_db
                and self._proto.backend not in (tp.Backend.DOLT, tp.Backend.DOLT_MYSQL,
                                                tp.Backend.NEON_LOCAL)):
            raise ValueError("database_setup.generated.seed_db is supported on DOLT, DOLT_MYSQL "
                             "and NEON_LOCAL only")

        # Validate operation is registered
        op_type = op_bench.operation
        if not OperationRegistry.is_registered(op_type):
            raise ValueError(
                f"Operation type {tp.OperationType.Name(op_type)} not registered"
            )

    # Configuration properties
    @property
    def run_id(self) -> str:
        return self._proto.run_id

    @property
    def backend(self) -> tp.Backend:
        return self._proto.backend

    @property
    def backend_name(self) -> str:
        return tp.Backend.Name(self._proto.backend)

    @property
    def table_name(self) -> str:
        return self._proto.table_name

    @property
    def scale_factor(self) -> int:
        return self._proto.scale_factor

    @property
    def num_threads(self) -> int:
        return max(1, self._proto.num_threads)

    @property
    def measure_storage(self) -> bool:
        return self._proto.measure_storage

    @property
    def concurrent_requests(self) -> int:
        """Number of concurrent requests per thread (default 1)."""
        return max(1, self._proto.concurrent_requests)

    @property
    def background_workers(self) -> int:
        """Background clients: one per setup branch when background_load is set."""
        return self.num_branches if self._proto.background_load else 0

    @property
    def background_operation(self) -> tp.OperationType:
        return self._proto.background_operation

    @property
    def num_processes(self) -> int:
        """Client processes the worker threads (measured and background) are
        spread over (1 = this process)."""
        return max(1, min(self._proto.num_processes, self.num_threads + self.background_workers))

    @property
    def use_async(self) -> bool:
        """Whether to run the async runner (use_async or concurrent_requests > 1)."""
        return self._proto.use_async or self.concurrent_requests > 1

    @property
    def database_setup(self) -> tp.DatabaseSetup:
        return self._proto.database_setup

    @property
    def db_name(self) -> str:
        return self._proto.database_setup.db_name

    @property
    def cleanup(self) -> bool:
        return self._proto.database_setup.cleanup

    @property
    def source(self) -> Optional[str]:
        """Which database_setup source is set: existing_db, sql_dump, generated or None."""
        return self._proto.database_setup.WhichOneof("source")

    @property
    def generated(self) -> tp.GeneratedData:
        return self._proto.database_setup.generated

    @property
    def operation_type(self) -> tp.OperationType:
        return self._proto.operation_benchmark.operation

    @property
    def operation_name(self) -> str:
        return tp.OperationType.Name(self._proto.operation_benchmark.operation)

    @property
    def num_ops(self) -> int:
        return self._proto.operation_benchmark.num_ops

    @property
    def warmup_ops(self) -> int:
        return self._proto.operation_benchmark.warmup_ops

    @property
    def setup_config(self) -> tp.SetupConfig:
        return self._proto.operation_benchmark.setup

    @property
    def num_branches(self) -> int:
        return self._proto.operation_benchmark.setup.num_branches

    @property
    def branch_shape(self) -> tp.BranchShape:
        return self._proto.operation_benchmark.setup.branch_shape

    @property
    def range_config(self) -> tp.RangeConfig:
        return self._proto.operation_benchmark.range_config

    @property
    def range_size(self) -> int:
        return self._proto.operation_benchmark.range_config.range_size or 10

    @property
    def ddl_config(self) -> tp.DdlConfig:
        return self._proto.operation_benchmark.ddl_config

    @property
    def branch_op_config(self) -> tp.BranchOpConfig:
        return self._proto.operation_benchmark.branch_op_config

    def __str__(self) -> str:
        return (
            f"BenchmarkConfig(run_id={self.run_id}, "
            f"backend={self.backend_name}, "
            f"operation={self.operation_name}, "
            f"num_ops={self.num_ops}, "
            f"num_branches={self.num_branches}, "
            f"scale_factor={self.scale_factor})"
        )


# ============================================================================
# Backend Management
# ============================================================================


@dataclass
class BackendInfo:
    """Backend-specific connection and project information."""

    default_uri: str = ""
    default_branch_id: str = ""
    # Branch workers start on (the last setup branch once setup has run)
    default_branch_name: str = ""
    # The backend's own default branch, which some backends refuse to delete
    root_branch_name: str = ""
    neon_project_id: Optional[str] = None
    xata_project_id: Optional[str] = None
    file_copy_info: Optional[FileCopyToolSuite.FileCopyInfo] = None
    chronos_info: Optional[ChronosToolSuite.ChronosInfo] = None
    setup_branches: list = None


def create_db_tools(
    backend: int,
    backend_info: BackendInfo,
    db_name: str,
    result_collector: rc.ResultCollector,
    measure_storage: bool = False,
) -> DBToolSuite:
    """Open one worker's DBToolSuite on the backend's default branch.

    Shared with the macrobench runner. ``backend`` is a Backend enum value
    (the microbench and macrobench enums use the same numbering).
    """
    default_branch = backend_info.default_branch_name
    if backend == tp.Backend.DOLT:
        return DoltToolSuite.init_for_bench(
            result_collector, db_name, default_branch, measure_storage
        )
    if backend == tp.Backend.DOLT_MYSQL:
        return DoltMySQLToolSuite.init_for_bench(
            result_collector, db_name, default_branch, measure_storage
        )
    if backend == tp.Backend.SEEKDB:
        return SeekDBToolSuite.init_for_bench(
            result_collector, db_name, default_branch, measure_storage
        )
    if backend == tp.Backend.MATRIXONE:
        return MatrixOneToolSuite.init_for_bench(
            result_collector, db_name, default_branch, measure_storage
        )
    if backend == tp.Backend.BAUPLAN:
        return BauplanToolSuite.init_for_bench(
            result_collector, db_name, default_branch, measure_storage
        )
    if backend == tp.Backend.FILE_COPY:
        return FileCopyToolSuite.init_for_bench(
            result_collector,
            db_name,
            default_branch,
            backend_info.file_copy_info.branches,
            backend_info.file_copy_info.branches_lock,
            backend_info.file_copy_info.create_db_lock,
            measure_storage,
        )
    if backend == tp.Backend.CHRONOS:
        return ChronosToolSuite.init_for_bench(
            result_collector, db_name, default_branch, measure_storage
        )
    if backend == tp.Backend.TERMINUSDB:
        return TerminusDBToolSuite.init_for_bench(
            result_collector, db_name, default_branch, measure_storage
        )
    if backend == tp.Backend.NEON_LOCAL:
        return NeonLocalToolSuite.init_for_bench(
            result_collector, db_name, default_branch, measure_storage
        )
    if backend == tp.Backend.NEON:
        return NeonToolSuite.init_for_bench(
            result_collector,
            backend_info.neon_project_id,
            backend_info.default_branch_id,
            default_branch,
            db_name,
            measure_storage,
        )
    if backend == tp.Backend.XATA:
        return XataToolSuite.init_for_bench(
            result_collector,
            backend_info.xata_project_id,
            backend_info.default_branch_id,
            default_branch,
            db_name,
            measure_storage,
        )
    raise ValueError(f"Unsupported backend: {tp.Backend.Name(backend)}")


@dataclass
class BackendSetup:
    """The parts of a config that BackendManager reads.

    Lets callers without a full BenchmarkConfig (e.g. the macrobench runner)
    set up a backend. ``database_setup`` may be the microbench or the
    macrobench DatabaseSetup message; both have the same fields.
    """

    backend: int
    database_setup: object

    @property
    def db_name(self) -> str:
        return self.database_setup.db_name

    @property
    def cleanup(self) -> bool:
        return self.database_setup.cleanup


class BackendManager:
    """Manages backend-specific setup and cleanup.

    Accepts a BenchmarkConfig or a BackendSetup; only backend,
    database_setup, db_name and cleanup are read.
    """

    def __init__(self, config, output_dir: str = "/tmp/run_stats"):
        self.config = config
        self.output_dir = output_dir
        self.backend_info: Optional[BackendInfo] = None

    def setup(self) -> BackendInfo:
        """Create backend project and database, perform setup.

        Returns:
            BackendInfo with connection details

        Raises:
            ValueError: If backend is unsupported
            Exception: If setup fails
        """
        config = self.config
        backend = config.backend
        db_name = config.db_name
        source = config.database_setup.WhichOneof("source")
        # An existing database is reused as is; a sql_dump is loaded; a
        # "generated" (or unset) source gets an empty database the caller
        # populates (the macrobench schema generators).
        require_db_setup = source != "existing_db"
        sql_dump_path = (config.database_setup.sql_dump.sql_dump_path
                         if source == "sql_dump" else None)

        info = BackendInfo()

        # Backend-specific project/connection creation
        if backend == tp.Backend.DOLT:
            info.default_uri = DoltToolSuite.get_default_connection_uri()
            info.default_branch_name = "main"
            print(f"Default Dolt connection URI: {info.default_uri}")

        elif backend == tp.Backend.DOLT_MYSQL:
            info.default_uri = DoltMySQLToolSuite.get_default_connection_uri()
            info.default_branch_name = "main"
            print(f"Default Dolt MySQL connection URI: {info.default_uri}")

        elif backend == tp.Backend.SEEKDB:
            info.default_uri = SeekDBToolSuite.get_default_connection_uri()
            info.default_branch_name = seekdb.MAIN_BRANCH
            # A SeekDB branch's ID is its database name; main is db_name.
            info.default_branch_id = db_name
            print(f"Default SeekDB connection URI: {info.default_uri}")

        elif backend == tp.Backend.MATRIXONE:
            info.default_uri = MatrixOneToolSuite.get_default_connection_uri()
            info.default_branch_name = matrixone.MAIN_BRANCH
            # A MatrixOne branch's ID is its database name; main is db_name.
            info.default_branch_id = db_name
            print(f"Default MatrixOne connection URI: {info.default_uri}")

        elif backend == tp.Backend.BAUPLAN:
            info.default_uri = BauplanToolSuite.get_default_connection_uri()
            info.default_branch_name = bauplan.MAIN_BRANCH
            # The run's root branch is <user>.<db_name>.
            info.default_branch_id = db_name

        elif backend == tp.Backend.FILE_COPY:
            info.file_copy_info = FileCopyToolSuite.FileCopyInfo(db_name)
            info.default_uri = FileCopyToolSuite.get_default_connection_uri()
            info.default_branch_name = db_name
            print(f"Default FILE_COPY connection URI: {info.default_uri}")

        elif backend == tp.Backend.CHRONOS:
            info.chronos_info = ChronosToolSuite.ChronosInfo(db_name)
            info.default_uri = ChronosToolSuite.get_default_connection_uri()
            info.default_branch_name = chronos_mod.MAIN_BRANCH
            info.root_branch_name = chronos_mod.MAIN_BRANCH
            print(f"Default CHRONOS connection URI: {info.default_uri}")

        elif backend == tp.Backend.TERMINUSDB:
            info.default_uri = TerminusDBToolSuite.get_default_connection_uri()
            info.default_branch_name = terminusdb.MAIN_BRANCH
            print(f"Default TerminusDB server: {info.default_uri}")

        elif backend == tp.Backend.NEON:
            if require_db_setup:
                # Leftovers of interrupted runs keep billing: drop them first.
                # Only this configuration's own projects: another run
                # (the macrobench's project_macro_* in particular) may be
                # live in the same org at the same time.
                project_name = f"project_{db_name}"
                stale = neon_mod.delete_stale_projects(prefix=project_name)
                if stale:
                    print(f"Deleted stale Neon projects: {stale}")
                neon_project = NeonToolSuite.create_neon_project(project_name)
                info.neon_project_id = neon_project["project"]["id"]
                info.default_uri = (
                    neon_project["connection_uris"][0]["connection_uri"]
                    if neon_project["connection_uris"]
                    else ""
                )
                info.default_branch_id = neon_project["branch"]["id"]
                info.default_branch_name = neon_project["branch"]["name"]
                print(f"Neon project ID: {info.neon_project_id}")
                print(f"Default Neon connection URI: {re.sub(r'://([^:/]+):[^@]*@', r'://\1:***@', info.default_uri)}")
            else:
                info.neon_project_id = config.database_setup.existing_db.neon_project_id
                proj_branches = NeonToolSuite.get_project_branches(info.neon_project_id)
                for branch in proj_branches["branches"]:
                    if branch["default"]:
                        info.default_branch_name = branch["name"]
                        info.default_branch_id = branch["id"]
                        break

        elif backend == tp.Backend.NEON_LOCAL:
            # The run root ("main") is a timeline branched off the seed
            # timeline, which already holds the data (see dblib/neon_local.py).
            info.default_branch_name = neon_local.MAIN_BRANCH

        elif backend == tp.Backend.XATA:
            if require_db_setup:
                (
                    xata_project_id,
                    default_branch_id,
                    default_branch_name,
                    default_uri,
                ) = XataToolSuite.create_xata_project(f"project_{db_name}")
                info.xata_project_id = xata_project_id
                info.default_uri = default_uri
                info.default_branch_id = default_branch_id
                info.default_branch_name = default_branch_name
                print(f"Xata project ID: {info.xata_project_id}")
                print(f"Default Xata connection URI: {info.default_uri}")
            else:
                raise NotImplementedError("Xata requires database setup")

        else:
            raise ValueError(f"Unsupported backend: {tp.Backend.Name(backend)}")

        # Create database and load schema if needed
        if require_db_setup and backend == tp.Backend.DOLT_MYSQL:
            # Not a Postgres server, so psql/psycopg2 can't be used to set it up.
            dolt_mysql.setup_database(db_name, sql_dump_path)
        elif require_db_setup and backend == tp.Backend.SEEKDB:
            seekdb.setup_database(db_name, sql_dump_path)
        elif require_db_setup and backend == tp.Backend.MATRIXONE:
            matrixone.setup_database(db_name, sql_dump_path)
        elif require_db_setup and backend == tp.Backend.TERMINUSDB:
            # No SQL: the dump is converted to a schema and documents.
            terminusdb.setup_database(db_name, sql_dump_path)
        elif require_db_setup and backend == tp.Backend.BAUPLAN:
            bauplan.setup_database(db_name, sql_dump_path)
        elif require_db_setup and backend == tp.Backend.NEON_LOCAL:
            neon_local.setup_run(db_name)
        elif require_db_setup:
            self._create_database(info.default_uri, db_name)

            if sql_dump_path:
                db_uri = self._get_connection_uri(info)
                load_sql_file(db_uri, sql_dump_path)
                # Commit schema changes for Dolt
                if backend == tp.Backend.DOLT:
                    commit_dolt_schema(db_uri)

        info.root_branch_name = info.default_branch_name
        self.backend_info = info
        return info

    def cleanup(self) -> None:
        """Clean up backend resources (databases, projects).

        This should be called after the benchmark completes.
        """
        if not self.config.cleanup or not self.backend_info:
            return

        info = self.backend_info
        db_name = self.config.db_name

        # Backend-specific cleanup
        if info.file_copy_info:
            FileCopyToolSuite.cleanup(info.file_copy_info)
        elif info.chronos_info:
            ChronosToolSuite.cleanup(info.chronos_info)
        elif self.config.backend == tp.Backend.DOLT_MYSQL and db_name:
            try:
                dolt_mysql.drop_database(db_name)
            except Exception as e:
                print(f"Error deleting database: {e}")
        elif self.config.backend == tp.Backend.SEEKDB and db_name:
            try:
                # Also drops every branch database forked from db_name.
                seekdb.drop_database(db_name)
            except Exception as e:
                print(f"Error deleting database: {e}")
        elif self.config.backend == tp.Backend.TERMINUSDB and db_name:
            try:
                terminusdb.drop_database(db_name)
            except Exception as e:
                print(f"Error deleting database: {e}")
        elif self.config.backend == tp.Backend.MATRIXONE and db_name:
            try:
                # Also drops every branch database and snapshot of the run.
                matrixone.drop_database(db_name)
            except Exception as e:
                print(f"Error deleting database: {e}")
        elif self.config.backend == tp.Backend.NEON_LOCAL and db_name:
            try:
                # Every compute and every timeline but the seed.
                neon_local.teardown_run(db_name)
            except Exception as e:
                print(f"Error cleaning up neon_local: {e}")
        elif self.config.backend == tp.Backend.BAUPLAN and db_name:
            try:
                # The run's branches, tags and S3 files.
                bauplan.drop_database(db_name)
            except Exception as e:
                print(f"Error deleting database: {e}")
        elif info.default_uri and db_name:
            try:
                conn = psycopg2.connect(info.default_uri)
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                cur = conn.cursor()
                cur.execute(f"DROP DATABASE IF EXISTS {db_name};")
                print(f"Database '{db_name}' deleted successfully.")
                cur.close()
                conn.close()
                if self.config.backend == tp.Backend.DOLT:
                    # Dolt keeps dropped databases on disk until purged.
                    dolt.purge_dropped_databases(info.default_uri)
            except Exception as e:
                print(f"Error deleting database: {e}")

        # Delete cloud projects
        if info.neon_project_id:
            NeonToolSuite.delete_project(info.neon_project_id)
        elif info.xata_project_id:
            XataToolSuite.delete_project(info.xata_project_id)

    def _create_database(self, uri: str, db_name: str) -> None:
        """Create the benchmark database."""
        try:
            conn = psycopg2.connect(uri)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            try:
                cur.execute(f"DROP DATABASE IF EXISTS {db_name};")
            except Exception as drop_err:
                print(f"Warning: could not drop existing database: {drop_err}")
            cur.execute(f"CREATE DATABASE {db_name};")
            print(f"Database '{db_name}' created successfully.")
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Error creating database: {e}")
            raise

    def _get_connection_uri(self, info: BackendInfo) -> str:
        """Get the database-specific connection URI."""
        backend = self.config.backend
        db_name = self.config.db_name

        if backend == tp.Backend.DOLT:
            return DoltToolSuite.get_initial_connection_uri(db_name)
        elif backend == tp.Backend.DOLT_MYSQL:
            return DoltMySQLToolSuite.get_initial_connection_uri(db_name)
        elif backend == tp.Backend.SEEKDB:
            return SeekDBToolSuite.get_initial_connection_uri(db_name)
        elif backend == tp.Backend.TERMINUSDB:
            return info.default_uri
        elif backend == tp.Backend.MATRIXONE:
            return MatrixOneToolSuite.get_initial_connection_uri(db_name)
        elif backend == tp.Backend.BAUPLAN:
            return BauplanToolSuite.get_initial_connection_uri(db_name)
        elif backend == tp.Backend.FILE_COPY:
            return FileCopyToolSuite.get_initial_connection_uri(db_name)
        elif backend == tp.Backend.CHRONOS:
            return ChronosToolSuite.get_initial_connection_uri(db_name)
        elif backend == tp.Backend.NEON:
            return NeonToolSuite._get_neon_connection_uri(
                info.neon_project_id, info.default_branch_id, db_name
            )
        elif backend == tp.Backend.XATA:
            return XataToolSuite._get_xata_connection_uri(
                info.xata_project_id, info.default_branch_id, db_name
            )
        else:
            raise ValueError(f"Unsupported backend: {tp.Backend.Name(backend)}")


# ============================================================================
# Generated data (the microbench counterpart of the macrobench "generated"
# source): CH-benCHmark seeded in the server, foreign keys skipped, committed
# as the base. BackendManager.setup() only creates the empty database, as it
# does for the macrobench runner and the verbs driver, which seed themselves;
# runner2.main() calls seed_generated_database() right after it.
# ============================================================================


def _generated_schema(gen: tp.GeneratedData, scale_factor: int = 1):
    from macrobench import task_pb2 as mtp

    return mtp.SchemaConfig(
        base=mtp.BaseSchema.CH_BENCH,
        scale_factor=gen.scale_factor or scale_factor or 1,
        skip_foreign_keys=True,
        seed_batch_rows=gen.seed_batch_rows or 500,
    )


def _seed_table_ref(backend: int, seed_db: str, table: str) -> str:
    """``table`` on the main branch of ``seed_db``, named the way the Dolt
    adapters address another database's branch (see ``_qualified_table`` in
    dblib/dolt.py and dblib/dolt_mysql.py)."""
    if backend == tp.Backend.DOLT:
        return f'"{seed_db}/main".public.{table}'
    if backend == tp.Backend.DOLT_MYSQL:
        return f"`{seed_db}/main`.`{table}`"
    raise ValueError(f"seed_db copy is not supported on {tp.Backend.Name(backend)}")


def _copy_from_seed_db(suite, backend: int, seed_db: str, ref: str, log=print) -> dict:
    """Copy every CH table from ``seed_db`` into the suite's database on
    ``ref`` with server-side INSERT ... SELECT (untimed). Returns {table: rows}."""
    from macrobench.schema import CH_TABLES

    counts = {}

    def script(db):
        for table in CH_TABLES:
            db.sql(f"INSERT INTO {table} SELECT * FROM {_seed_table_ref(backend, seed_db, table)}")
            counts[table] = db.sql(f"SELECT count(*) FROM {table}")[0][0]
            log(f"  copied {table:>12}: {counts[table]:>9,} rows")

    suite.exec(script, refs=[ref], timed=False)[0].raise_for_status()
    return counts


def _database_exists(backend: int, backend_info: BackendInfo, db_name: str) -> bool:
    """Whether the driver can open ``db_name`` on the server."""
    try:
        suite = create_db_tools(backend, backend_info, db_name, rc.ResultCollector())
    except Exception:
        return False
    suite.close_connection()
    return True


def seed_generated_database(backend: int, backend_info: BackendInfo, db_name: str,
                            gen: tp.GeneratedData, scale_factor: int = 1,
                            log=print, schema=None) -> dict:
    """Seed the root branch of the (empty) ``db_name`` with generated
    CH-benCHmark data and commit it as "base". With ``gen.seed_db`` set, the
    tables are copied server-side from that database, which is generated once
    on first use and never dropped. Returns what the summary records."""
    from macrobench import schema as mschema
    from macrobench.datagen.ch import CHScale, seed_ch

    schema = schema or _generated_schema(gen, scale_factor)
    root = backend_info.root_branch_name or backend_info.default_branch_name
    t0 = time.time()
    if backend == tp.Backend.NEON_LOCAL and gen.seed_db:
        # The run root is a branch of the seed timeline: the data is there.
        log(f"NEON_LOCAL: run root branched from the seed timeline ({gen.seed_db}); nothing to copy")
        return {"source": "generated", "scale_factor": schema.scale_factor, "rows": {},
                "seed_db": gen.seed_db, "seed_db_created": False, "seed_sec": 0.0}
    seeded_now = False
    if gen.seed_db:
        seeded_now = ensure_seed_database(backend, gen, scale_factor, log=log, schema=schema)
    suite = create_db_tools(backend, backend_info, db_name, rc.ResultCollector())
    try:
        sch = mschema.apply_schema(suite, root, schema, "microbench", log=log)
        if sch["failed"]:
            raise RuntimeError(f"schema statement failed: {sch['failed'][0]}")
        if gen.seed_db:
            log(f"Copying CH-benCHmark tables from {gen.seed_db}...")
            counts = _copy_from_seed_db(suite, backend, gen.seed_db, root, log=log)
        else:
            scale = CHScale.from_config(schema)
            log(f"Seeding CH-benCHmark at W={scale.warehouses}...")
            counts = seed_ch(suite, root, scale, seed=gen.seed or 42,
                             batch_rows=schema.seed_batch_rows or 500, log=log)
        suite.commit(root, "base", timed=False, raise_on_error=True)
    finally:
        suite.close_connection()
    stats = {
        "source": "generated",
        "scale_factor": schema.scale_factor,
        "rows": counts,
        "seed_db": gen.seed_db or None,
        "seed_db_created": seeded_now,
        "seed_sec": round(time.time() - t0, 1),
    }
    log(f"Data ready: {sum(counts.values()):,} rows in {stats['seed_sec']}s")
    return stats


def ensure_seed_database(backend: int, gen: tp.GeneratedData, scale_factor: int = 1,
                         log=print, schema=None) -> bool:
    """Create and generate ``gen.seed_db`` unless it exists. Returns True if
    it was generated now."""
    probe_info = BackendInfo(default_branch_name="main", root_branch_name="main")
    if _database_exists(backend, probe_info, gen.seed_db):
        return False
    log(f"Seed database {gen.seed_db} not found; generating it once...")
    setup = tp.DatabaseSetup(db_name=gen.seed_db, cleanup=False)
    setup.generated.CopyFrom(gen)
    setup.generated.seed_db = ""
    info = BackendManager(BackendSetup(backend, setup)).setup()
    seed_generated_database(backend, info, gen.seed_db, setup.generated, scale_factor,
                            log=log, schema=schema)
    return True


# ============================================================================
# Setup Phase (UNTIMED)
# ============================================================================


class SetupPhase:
    """Manages the untimed setup phase.

    Creates branches and performs data operations to prepare the database
    for measurement. Timing data is saved to a separate _setup.parquet file.
    """

    def __init__(
        self,
        config: BenchmarkConfig,
        backend_info: BackendInfo,
        output_dir: str = "/tmp/run_stats"
    ):
        self.config = config
        self.backend_info = backend_info
        self.output_dir = output_dir

    def execute(self) -> List[str]:
        """Run the setup phase and return list of created branches.

        Returns:
            List of branch names created during setup

        Raises:
            Exception: If setup fails
        """
        setup = self.config.setup_config
        num_branches = setup.num_branches
        shape = setup.branch_shape
        inserts_per_branch = setup.inserts_per_branch
        updates_per_branch = setup.updates_per_branch
        deletes_per_branch = setup.deletes_per_branch

        print(f"\n{'='*60}")
        print(f"Setup Phase (UNTIMED)")
        print(f"{'='*60}")
        print(f"Creating {num_branches} branches (shape: {tp.BranchShape.Name(shape)})")
        print(f"Per branch: {inserts_per_branch} inserts, {updates_per_branch} updates, {deletes_per_branch} deletes")

        # Create result collector for setup timing
        setup_result_collector = rc.ResultCollector(
            run_id=f"{self.config.run_id}_setup",
            output_dir=self.output_dir,
        )

        # Create shared branch manager
        setup_branch_manager = SharedBranchManager()

        # Create a worker context for setup operations
        with self._create_setup_context(
            setup_result_collector, setup_branch_manager
        ) as setup_ctx:
            last_branch_name, last_branch_id = self._perform_setup_operations(
                setup_ctx,
                num_branches,
                shape,
                inserts_per_branch,
                updates_per_branch,
                deletes_per_branch,
            )

            # Update backend info with last branch
            self.backend_info.default_branch_name = last_branch_name
            self.backend_info.default_branch_id = last_branch_id

        # Get all created branches
        setup_branches = setup_branch_manager.get_all_branches()
        self.backend_info.setup_branches = setup_branches

        # Write setup timing to separate parquet file
        setup_result_collector.write_to_parquet(
            filename=f"{self.config.run_id}_setup.parquet"
        )

        print(f"Setup complete: {len(setup_branches)} branches created")
        print(f"Last branch: {last_branch_name}")
        print(f"{'='*60}\n")

        return setup_branches

    @contextmanager
    def _create_setup_context(
        self,
        result_collector: rc.ResultCollector,
        branch_manager: 'SharedBranchManager'
    ):
        """Create a WorkerContext for setup operations."""
        # This is a simplified version just for setup
        # We'll define WorkerContext below
        from microbench.runner2 import WorkerContext

        ctx = WorkerContext(
            config=self.config,
            backend_info=self.backend_info,
            thread_id=0,
            seed=42,  # Fixed seed for reproducible setup
            result_collector=result_collector,
            branch_manager=branch_manager,
            shared_progress=None,
            assigned_branches=[],
        )

        try:
            ctx.__enter__()
            yield ctx
        finally:
            ctx.__exit__(None, None, None)

    def _perform_setup_operations(
        self,
        ctx: 'WorkerContext',
        num_branches: int,
        shape: tp.BranchShape,
        inserts: int,
        updates: int,
        deletes: int,
    ) -> Tuple[str, str]:
        """Perform branch creation and data operations.

        Returns:
            Tuple of (last_branch_name, last_branch_id)
        """
        # Get benchmark table
        benchmark_table = self.config.table_name
        if not benchmark_table:
            all_tables = dbh.get_all_tables(ctx.db_tools.get_current_connection())
            benchmark_table = ctx.rnd.choice(all_tables)

        # Initialize data generator
        table_schema = ctx.db_tools.get_table_schema(benchmark_table)
        if not table_schema:
            raise ValueError(f"Could not fetch DDL for table {benchmark_table}")

        ctx._table_datagen = DynamicDataGenerator(table_schema)
        ctx._pk_columns = dbh.get_pk_column_names(
            ctx.db_tools.get_current_connection(), benchmark_table
        )

        root_branch_name = ctx.current_branch
        current_parent = root_branch_name

        # Branches created so far (parents for the BUSHY shape)
        branches = [root_branch_name]

        # Perform setup on root branch
        print(f"Performing setup ops on root branch...")
        self._perform_branch_data_ops(
            ctx, benchmark_table, inserts, updates, deletes
        )

        parallelism = self.config.setup_config.setup_parallelism
        if parallelism > 1 and shape == tp.BranchShape.FAN_OUT and num_branches > 1:
            return self._parallel_fan_out(ctx, table_schema, benchmark_table, num_branches,
                                          parallelism, inserts, updates, deletes)

        # Create branches with progress bar. Branch creation is timed into
        # the setup collector; the data ops on each branch are not.
        for i in tqdm(range(num_branches), desc="Creating branches"):
            branch_name = f"setup_branch_{i + 1}"

            if shape == tp.BranchShape.SPINE:
                parent = current_parent  # Linear chain
            elif shape == tp.BranchShape.FAN_OUT:
                parent = root_branch_name  # All from root
            else:  # BUSHY
                parent = ctx.rnd.choice(branches)  # Random parent

            # The timed BRANCH row carries the branches existing before the
            # create, the CONNECT row the count after it.
            ctx.record_branch_count(i)
            ctx.db_tools.branch(
                branch_name, from_ref=parent,
                storage=self.config.measure_storage,
            ).raise_for_status()
            ctx.record_branch_count(i + 1)
            ctx.connect(branch_name)
            current_parent = branch_name
            branches.append(branch_name)

            # Perform data operations on this branch
            self._perform_branch_data_ops(
                ctx, benchmark_table, inserts, updates, deletes
            )

            # Track the branch
            ctx.add_branch(branch_name)

        last_branch_name = ctx.current_branch
        return last_branch_name, last_branch_name

    def _parallel_fan_out(self, ctx: 'WorkerContext', table_schema, table: str, num_branches: int,
                          parallelism: int, inserts: int, updates: int, deletes: int) -> Tuple[str, str]:
        """FAN_OUT setup with ``parallelism`` setup workers, each with its own
        suite: create setup_branch_i from the root, connect to it (timed, as in
        the serial path) and run its untimed data operations. Branches are
        registered in index order afterwards, so the result matches the
        serial path's."""
        root = ctx.current_branch or self.backend_info.default_branch_name
        work = queue.Queue()
        for i in range(num_branches):
            work.put(i)
        bar = tqdm(total=num_branches, desc=f"Creating branches ({parallelism} at a time)")
        bar_lock = threading.Lock()

        def worker(k: int) -> None:
            wctx = WorkerContext(
                config=self.config, backend_info=self.backend_info, thread_id=k + 1, seed=42 + k,
                result_collector=ctx.result_collector, branch_manager=SharedBranchManager(),
                shared_progress=None, assigned_branches=[],
            )
            with wctx:
                wctx._table_datagen = DynamicDataGenerator(table_schema)
                wctx._pk_columns = ctx._pk_columns
                while True:
                    try:
                        i = work.get_nowait()
                    except queue.Empty:
                        return
                    name = f"setup_branch_{i + 1}"
                    wctx.record_branch_count(i)
                    wctx.db_tools.branch(name, from_ref=root,
                                         storage=self.config.measure_storage).raise_for_status()
                    wctx.record_branch_count(i + 1)
                    wctx.connect(name)
                    self._perform_branch_data_ops(wctx, table, inserts, updates, deletes)
                    with bar_lock:
                        bar.update(1)

        with ThreadPoolExecutor(parallelism) as ex:
            for f in [ex.submit(worker, k) for k in range(min(parallelism, num_branches))]:
                f.result()
        bar.close()
        for i in range(num_branches):
            ctx.add_branch(f"setup_branch_{i + 1}")
        last = f"setup_branch_{num_branches}"
        return last, last

    def _perform_branch_data_ops(
        self,
        ctx: 'WorkerContext',
        table_name: str,
        inserts: int,
        updates: int,
        deletes: int
    ) -> None:
        """Perform inserts, updates, and deletes on current branch (untimed)."""
        # Perform inserts first
        for _ in range(inserts):
            ctx._insert_without_timing(table_name)

        # Shuffle and perform updates/deletes
        ops = ["update"] * updates + ["delete"] * deletes
        ctx.rnd.shuffle(ops)

        for op in ops:
            if op == "update":
                ctx._update_without_timing(table_name)
            else:
                ctx._delete_without_timing(table_name)


# ============================================================================
# Shared State Management
# ============================================================================


class SharedBranchManager:
    """Thread-safe manager for branch IDs and branch list."""

    def __init__(self, initial_branches: Optional[List[str]] = None):
        self._next_branch_id = 1
        self._branches: List[str] = list(initial_branches) if initial_branches else []
        self._lock = threading.Lock()

    def get_next_branch_id(self) -> int:
        """Atomically get next branch ID and increment."""
        with self._lock:
            current = self._next_branch_id
            self._next_branch_id += 1
            return current

    def add_branch(self, branch_name: str) -> None:
        """Add a branch to the shared list (once)."""
        with self._lock:
            if branch_name not in self._branches:
                self._branches.append(branch_name)

    def remove_branch(self, branch_name: str) -> None:
        """Remove a branch from the shared list."""
        with self._lock:
            if branch_name in self._branches:
                self._branches.remove(branch_name)

    def get_random_branch(self, rnd: random.Random) -> Optional[str]:
        """Get a random branch from the list."""
        with self._lock:
            if not self._branches:
                return None
            return rnd.choice(self._branches)

    def get_all_branches(self) -> List[str]:
        """Get a copy of all branches."""
        with self._lock:
            return list(self._branches)

    def __len__(self) -> int:
        with self._lock:
            return len(self._branches)


# Table metadata (key columns, all columns, schema, existing keys) looked up
# once per client process and shared by its worker threads. Each lookup is an
# information_schema query (tens of ms on the Dolt servers) or a full key
# scan; done per thread or per operation it adds server load and, in async
# mode, blocks the worker's event loop. Keyed by (backend, database, kind,
# table); a runner process serves one run, so entries never go stale.
_META_CACHE: Dict[tuple, list] = {}
_META_LOCK = threading.Lock()
_MISSING = object()


def _cached_meta(key: tuple, compute):
    """Value for ``key``, computed once per process even under concurrent callers."""
    with _META_LOCK:
        entry = _META_CACHE.get(key)
        if entry is None:
            entry = _META_CACHE[key] = [threading.Lock(), _MISSING]
    if entry[1] is _MISSING:
        with entry[0]:
            if entry[1] is _MISSING:
                entry[1] = compute()
    return entry[1]


class SharedProgress:
    """Thread-safe shared progress bar."""

    def __init__(self, total: int, desc: str = "Progress", disable: bool = False):
        self._pbar = tqdm(total=total, desc=desc, position=0, leave=True, disable=disable)
        self._lock = threading.Lock()

    def update(self, n: int = 1) -> None:
        """Thread-safe update of progress bar."""
        with self._lock:
            self._pbar.update(n)

    def close(self) -> None:
        """Close the progress bar."""
        self._pbar.close()

    def write(self, msg: str) -> None:
        """Thread-safe write message above progress bar."""
        tqdm.write(msg)


# ============================================================================
# Worker Context
# ============================================================================


class TimedPhase:
    """Start barrier and timed window shared by all worker threads.

    Each worker connects, opens its async pool (if any) and runs its warm-up,
    then calls wait(). The timed phase starts when the last worker arrives,
    so all workers start their timed ops together and per-worker setup is
    kept out of the throughput window. Each worker calls finish() when its
    timed ops are done; the window ends at the last one.
    """

    def __init__(self, num_workers: int):
        self.start_time: Optional[float] = None
        self._end_times: List[float] = []
        self._lock = threading.Lock()
        self._num_workers = num_workers
        self._barrier = threading.Barrier(num_workers, action=self._mark_start)
        # CPU seconds of the server process (when co-located) and of this
        # client process, sampled at both ends of the timed window.
        self._server_cpu = [None, None]
        self._client_cpu = [None, None]
        self.background: Optional["BackgroundControl"] = None   # stopped at the last finish

    def _mark_start(self) -> None:
        # Runs once, in the last worker to arrive, before any are released.
        self.start_time = time.time()
        self._server_cpu[0] = server_cpu_seconds()
        self._client_cpu[0] = self_cpu_seconds()

    def wait(self) -> None:
        self._barrier.wait()

    def abort(self) -> None:
        """Release workers blocked in wait() with BrokenBarrierError.

        Called when a worker fails before reaching the barrier, so the others
        fail instead of waiting forever.
        """
        self._barrier.abort()
        if self.background is not None:
            self.background.stop()

    def finish(self) -> None:
        with self._lock:
            self._end_times.append(time.time())
            if len(self._end_times) == self._num_workers:
                self._server_cpu[1] = server_cpu_seconds()
                self._client_cpu[1] = self_cpu_seconds()
                if self.background is not None:
                    self.background.stop()

    @property
    def end_time(self) -> Optional[float]:
        with self._lock:
            return max(self._end_times) if self._end_times else None

    def cpu_usage(self) -> dict:
        """CPU seconds used in the timed window by the server process (None
        unless DBFORK_SERVER_PID names a local process) and the client."""
        return {"server_cpu_seconds": _delta(self._server_cpu),
                "client_cpu_seconds": _delta(self._client_cpu)}


class RemoteCpuSampler:
    """CPU (and disk) use of remote server machines over the timed window.

    DBFORK_REMOTE_CPU="label=http://host:port/cpu?proc=a,b&disk=sdc;label2=..."
    names HTTP endpoints that return cumulative counters as JSON
    ({"proc_s", "node_busy_s", "node_iowait_s", "ncpu", "disk_busy_s"?}; see
    experiment/3-neon-branch-scaling/scripts/neon_node_agent.py). A thread
    samples every endpoint every ``period`` seconds for the whole run; the
    counters are interpolated at the window's ends, so the timed path never
    waits on the network.
    """

    FIELDS = ("proc_s", "node_busy_s", "node_iowait_s", "disk_busy_s", "disk_reads", "disk_writes",
              "disk_read_bytes", "disk_write_bytes")

    def __init__(self, period: float = 1.0):
        spec = os.environ.get("DBFORK_REMOTE_CPU", "")
        self.targets = []
        for item in filter(None, (x.strip() for x in spec.split(";"))):
            label, url = item.split("=", 1)
            self.targets.append((label.strip(), url.strip()))
        self.period = period
        self.samples: Dict[str, list] = {label: [] for label, _ in self.targets}
        self._stop = threading.Event()
        self._thread = None

    def _fetch(self, label: str, url: str) -> None:
        import urllib.request
        try:
            t0 = time.time()
            with urllib.request.urlopen(url, timeout=5) as r:
                data = json.loads(r.read())
            data["_t"] = (t0 + time.time()) / 2
            self.samples[label].append(data)
        except Exception:
            pass

    def _loop(self) -> None:
        while True:
            threads = [threading.Thread(target=self._fetch, args=t, daemon=True) for t in self.targets]
            for t in threads:
                t.start()
            for t in threads:
                t.join(6)
            if self._stop.wait(self.period):
                break

    def start(self) -> "RemoteCpuSampler":
        if self.targets:
            self._thread = threading.Thread(target=self._loop, daemon=True)
            self._thread.start()
        return self

    def stop(self) -> None:
        if self._thread:
            self._stop.set()
            self._thread.join(10)

    @staticmethod
    def _at(samples: list, field: str, t: float) -> Optional[float]:
        pts = [(x["_t"], x[field]) for x in samples if field in x]
        if len(pts) < 2 or t < pts[0][0] - 2 or t > pts[-1][0] + 2:
            return None
        for (ta, va), (tb, vb) in zip(pts, pts[1:]):
            if t <= tb or (ta, va) == pts[-2]:
                if tb == ta:
                    return vb
                return va + (vb - va) * (t - ta) / (tb - ta)
        return None

    def usage(self, start: float, end: float) -> Optional[dict]:
        """{label: {proc_cores, node_cores, node_iowait_cores, disk_util, ncpu}}
        averaged over [start, end]."""
        if not self.targets or end <= start:
            return None
        out = {}
        names = {"proc_s": "proc_cores", "node_busy_s": "node_cores",
                 "node_iowait_s": "node_iowait_cores", "disk_busy_s": "disk_util",
                 "disk_reads": "disk_reads_per_s", "disk_writes": "disk_writes_per_s",
                 "disk_read_bytes": "disk_read_bytes_per_s", "disk_write_bytes": "disk_write_bytes_per_s"}
        for label, samples in self.samples.items():
            row = {"ncpu": samples[-1].get("ncpu") if samples else None, "samples": len(samples)}
            for f in self.FIELDS:
                a, b = self._at(samples, f, start), self._at(samples, f, end)
                row[names[f]] = (b - a) / (end - start) if a is not None and b is not None else None
            out[label] = row
        return out


def server_cpu_seconds() -> Optional[float]:
    """utime + stime of the process DBFORK_SERVER_PID names (a server on
    this machine), or None."""
    pid = os.environ.get("DBFORK_SERVER_PID", "")
    if not pid.isdigit():
        return None
    try:
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().rsplit(")", 1)[1].split()
        # After the comm field: utime and stime are fields 14 and 15 overall.
        return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
    except (OSError, ValueError, IndexError):
        return None


def self_cpu_seconds() -> float:
    """CPU seconds (user + system) used by this process so far."""
    ru = resource.getrusage(resource.RUSAGE_SELF)
    return ru.ru_utime + ru.ru_stime


def _delta(pair) -> Optional[float]:
    return None if pair[0] is None or pair[1] is None else pair[1] - pair[0]


class BackgroundControl:
    """Coordinates background clients (background_load): measured workers
    wait until every background client is running, and the timed phase stops
    them when its last measured worker finishes. Built from threading
    primitives (one process) or multiprocessing ones (client processes);
    ``expected`` is the number of background clients."""

    def __init__(self, expected: int, ctx=None):
        self.expected = expected
        if ctx is None:
            self._stop = threading.Event()
            self._lock = threading.Lock()
            self._started = SimpleNamespace(value=0)
        else:
            self._stop = ctx.Event()
            self._lock = ctx.Lock()
            self._started = ctx.Value("i", 0, lock=False)

    def started(self) -> None:
        with self._lock:
            self._started.value += 1

    # Seconds the background load runs on its own before measuring starts.
    SETTLE_SEC = 1.0

    def wait_started(self, timeout: float = 600.0) -> None:
        """Block until every background client runs (or stop/timeout), then
        let the load settle for SETTLE_SEC."""
        deadline = time.time() + timeout
        while self._started.value < self.expected and not self._stop.is_set():
            if time.time() > deadline:
                raise RuntimeError(f"only {self._started.value}/{self.expected} background clients started")
            time.sleep(0.05)
        time.sleep(self.SETTLE_SEC)

    def stop(self) -> None:
        self._stop.set()

    def stopped(self) -> bool:
        return self._stop.is_set()


class ProcessTimedPhase:
    """TimedPhase shared by the worker threads of several client processes
    (num_processes > 1): one multiprocessing barrier over every thread, and
    the window start/end and server CPU kept in shared memory. Each process
    also measures its own CPU over the window."""

    def __init__(self, shared, local_workers: int, background: Optional["BackgroundControl"] = None):
        # shared = (barrier, times[start, end, server_cpu0, server_cpu1], done, lock, total_workers)
        self._barrier, self._times, self._done, self._lock, self._total = shared
        self.background = background
        self._local_workers = local_workers
        self._local_lock = threading.Lock()
        self._local_done = 0
        self._client_cpu = [None, None]

    def wait(self) -> None:
        if self._barrier.wait() == 0:
            with self._lock:
                self._times[0] = time.time()
                cpu = server_cpu_seconds()
                self._times[2] = -1.0 if cpu is None else cpu
        with self._local_lock:
            if self._client_cpu[0] is None:
                self._client_cpu[0] = self_cpu_seconds()

    def abort(self) -> None:
        self._barrier.abort()
        if self.background is not None:
            self.background.stop()

    def finish(self) -> None:
        with self._lock:
            self._done.value += 1
            if self._done.value == self._total:
                self._times[1] = time.time()
                cpu = server_cpu_seconds()
                self._times[3] = -1.0 if cpu is None else cpu
                if self.background is not None:
                    self.background.stop()
        with self._local_lock:
            self._local_done += 1
            if self._local_done == self._local_workers:
                self._client_cpu[1] = self_cpu_seconds()

    def client_cpu_seconds(self) -> Optional[float]:
        return _delta(self._client_cpu)


class WorkerContext:
    """Context object providing services to operations.

    This replaces the monolithic BenchmarkSuite with a cleaner interface
    that operations can use to access database connections, random number
    generation, data generation, etc.
    """

    def __init__(
        self,
        config: BenchmarkConfig,
        backend_info: BackendInfo,
        thread_id: int,
        seed: int,
        result_collector: rc.ResultCollector,
        branch_manager: SharedBranchManager,
        shared_progress: Optional[SharedProgress],
        assigned_branches: List[str],
    ):
        self.config = config
        self.backend_info = backend_info
        self.thread_id = thread_id
        self.seed = seed
        self.result_collector = result_collector
        self.branch_manager = branch_manager
        self.shared_progress = shared_progress
        self.assigned_branches = assigned_branches
        self.measure_storage = config.measure_storage

        # Thread-local RNG
        self.rnd = random.Random(seed)

        # Database tools (initialized in __enter__)
        self.db_tools = None

        # Data generation and caching
        self._table_datagen: Optional[DynamicDataGenerator] = None
        self._pk_columns: List[str] = []
        self._columns: Dict[str, List[str]] = {}  # table -> column names (setup helpers)
        self._existing_pks: List[Tuple] = []
        self._modified_keys: Dict[str, List[Tuple]] = {}

        # Index tracking for DDL operations
        self._created_indexes: Dict[str, List[str]] = {}  # table -> [index_names]
        self._created_columns: Dict[str, List[str]] = {}  # table -> [column_names]

    def __enter__(self) -> 'WorkerContext':
        """Initialize database connection and tools."""
        config = self.config
        backend_info = self.backend_info

        # Set thread ID for result collection
        rc.set_current_thread_id(self.thread_id)

        # Create result collector
        result_collector = self.result_collector

        # Initialize backend-specific tools
        self.db_tools = create_db_tools(
            config.backend, backend_info, config.db_name, result_collector,
            measure_storage=self.measure_storage,
        )

        # Every row of the main phase carries the number of setup branches.
        self.record_branch_count(config.num_branches)

        # Add root branch to manager (except FILE_COPY)
        if config.backend != tp.Backend.FILE_COPY:
            self.branch_manager.add_branch(backend_info.default_branch_name)
        elif config.num_threads > 1 and self.thread_id > 0 and backend_info.setup_branches:
            # CREATE DATABASE ... TEMPLATE needs no connections on the
            # template, so idle workers move off the root database.
            self.connect(backend_info.setup_branches[0], timed=False)

        # Connect to assigned branch if multi-threaded
        if self.assigned_branches:
            initial_branch = self.assigned_branches[0]
            if self.thread_id == 0:
                print(f"Thread {self.thread_id} connecting to: {initial_branch}")
            self.connect(initial_branch, timed=False)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close database connection."""
        if self.db_tools:
            self.db_tools.close_connection()

    # ========================================================================
    # Services for Operations
    # ========================================================================

    @property
    def current_ref(self) -> Ref:
        """Ref the worker's connection is on."""
        return self.db_tools.current_ref

    @property
    def current_branch(self) -> str:
        ref = self.db_tools.current_ref
        return ref.branch if ref else ""

    def connect(self, branch: str, timed: bool = True) -> None:
        """Switch to ``branch`` (an exec with an empty script, so the
        CONNECT row carries the switch cost)."""
        self.db_tools.exec([], refs=[branch], timed=timed).pop().raise_for_status()

    async def connect_async(self, branch: str, timed: bool = True) -> None:
        res = await self.db_tools.exec_async([], refs=[branch], timed=timed)
        res[0].raise_for_status()

    def run_sql(self, sql: str, params=None, timed: bool = True):
        """Run one statement on the current branch through exec() and
        return its rows. Raises if the statement failed."""
        res = self.db_tools.exec(
            [(sql, params)], refs=[self.current_ref], timed=timed,
            storage=self.measure_storage if timed else False,
        )[0]
        res.raise_for_status()
        return res.rows

    async def run_sql_async(self, sql: str, params=None, timed: bool = True):
        res = await self.db_tools.exec_async(
            [(sql, params)], refs=[self.current_ref], timed=timed,
            storage=self.measure_storage if timed else False,
        )
        res[0].raise_for_status()
        return res[0].rows

    def run_script(self, script, timed: bool = True):
        """Run a multi-statement script (or Python source) on the current
        branch as one exec(); raises if it failed."""
        res = self.db_tools.exec(
            script, refs=[self.current_ref], timed=timed,
            storage=self.measure_storage if timed else False,
        )[0]
        res.raise_for_status()
        return res

    async def run_script_async(self, script, timed: bool = True):
        res = await self.db_tools.exec_async(
            script, refs=[self.current_ref], timed=timed,
            storage=self.measure_storage if timed else False,
        )
        res[0].raise_for_status()
        return res[0]

    def _meta_key(self, kind: str, table_name: str) -> tuple:
        return (self.config.backend, self.config.db_name, kind, table_name)

    def get_pk_columns(self, table_name: str) -> List[str]:
        """Get primary key column names for a table (once per process)."""
        if not self._pk_columns:
            self._pk_columns = _cached_meta(
                self._meta_key("pk_columns", table_name),
                lambda: dbh.get_pk_column_names(self.db_tools.get_current_connection(), table_name),
            )
        return self._pk_columns

    def get_pk_values(self, table_name: str) -> list:
        """Primary keys of ``table_name`` as first read, shared by the
        process's workers (a read-only list; operations keep their own
        modified keys per branch)."""
        if not self._existing_pks:
            pk_columns = self.get_pk_columns(table_name)
            self._existing_pks = _cached_meta(
                self._meta_key("pk_values", table_name),
                lambda: dbh.get_pk_values(self.db_tools.get_current_connection(), table_name, pk_columns),
            )
        return self._existing_pks

    def initial_db_size(self) -> int:
        """Database size at the start of the run (once per process; 0 if unknown)."""
        def size():
            try:
                return dbh.get_db_size(self.db_tools.get_current_connection())
            except Exception:
                return 0
        return _cached_meta(self._meta_key("db_size", ""), size)

    def get_table_schema(self, table_name: str) -> str:
        """CREATE TABLE text of ``table_name`` (once per process)."""
        return _cached_meta(self._meta_key("schema", table_name),
                            lambda: self.db_tools.get_table_schema(table_name))

    def select_random_key(self, table_name: str) -> Optional[Tuple]:
        """Select a random primary key from the table."""
        cur_branch_id = self.current_branch

        pk_columns = self.get_pk_columns(table_name)
        existing_pks = self.get_pk_values(table_name)


        if not existing_pks:
            return None

        # 50% chance to select from modified keys if available
        modified = self._modified_keys.get(cur_branch_id, [])
        if modified and self.rnd.random() < 0.5:
            return self.rnd.choice(modified)
        else:
            return self.rnd.choice(existing_pks)

    def get_all_columns(self, table_name: str) -> List[str]:
        """Column names of ``table_name``, looked up once per process. The
        setup helpers and the write operations need them on every row, and
        an information_schema query costs tens of ms on the Dolt servers."""
        if table_name not in self._columns:
            self._columns[table_name] = _cached_meta(
                self._meta_key("columns", table_name),
                lambda: dbh.get_all_columns(self.db_tools.get_current_connection(), table_name),
            )
        return self._columns[table_name]

    def generate_row(self, table_name: str) -> dict:
        """Generate a fake data row for the table."""
        if not self._table_datagen:
            self._table_datagen = DynamicDataGenerator(self.get_table_schema(table_name))
        return self._table_datagen.generate_row()

    def track_modified_key(self, key: Tuple) -> None:
        """Track a modified primary key."""
        cur_branch_id = self.current_branch
        if key not in self._modified_keys.get(cur_branch_id, []):
            self._modified_keys.setdefault(cur_branch_id, []).append(key)

    def untrack_modified_key(self, key: Tuple) -> None:
        """Remove a key from modified tracking (e.g., after delete)."""
        cur_branch_id = self.current_branch
        if cur_branch_id in self._modified_keys:
            if key in self._modified_keys[cur_branch_id]:
                self._modified_keys[cur_branch_id].remove(key)

    def record_keys_touched(self, num_keys: int) -> None:
        """Record number of keys touched for this operation."""
        self.db_tools.result_collector.record_num_keys_touched(num_keys)

    def record_branch_count(self, n: int) -> None:
        """Set the ``branch_count`` column of the rows this worker (or the
        current asyncio task) records from now on."""
        self.db_tools.result_collector.record_branch_count(n)

    def live_branch_count(self) -> int:
        """Branches in the shared list other than the backend's root."""
        root = self.backend_info.root_branch_name
        return sum(1 for b in self.branch_manager.get_all_branches() if b != root)

    def get_next_branch_id(self) -> int:
        """Get next unique branch ID."""
        return self.branch_manager.get_next_branch_id()

    def add_branch(self, branch_name: str) -> None:
        """Add a branch to the shared branch list."""
        self.branch_manager.add_branch(branch_name)

    def remove_branch(self, branch_name: str) -> None:
        """Remove a branch from the shared branch list."""
        self.branch_manager.remove_branch(branch_name)

    def get_random_branch(self) -> Optional[str]:
        """Get a random branch from the shared list."""
        return self.branch_manager.get_random_branch(self.rnd)

    def get_all_branches(self) -> List[str]:
        """Get all branches."""
        return self.branch_manager.get_all_branches()

    @property
    def root_branch(self) -> str:
        """The backend's default branch (never a deletion target)."""
        return self.backend_info.root_branch_name

    def clear_pk_cache(self) -> None:
        """Clear cached primary keys (e.g., after branch switch)."""
        self._existing_pks = []

    def get_existing_key_count(self, table_name: str) -> int:
        """Number of existing primary keys in the table.

        Used to turn a backfill fraction into a concrete row count.
        """
        pk_columns = self.get_pk_columns(table_name)
        existing_pks = self.get_pk_values(table_name)
        return len(existing_pks) if existing_pks else 0

    def prepare_range_query(
        self, table_name: str, range_size: int, operation_name: str
    ) -> dict:
        """Prepare range query components.

        Returns:
            dict with keys_in_range, where_clause, params
        """
        pk_columns = self.get_pk_columns(table_name)

        # Get all PKs sorted
        existing_pks = self.get_pk_values(table_name)


        if not existing_pks:
            raise ValueError(f"No existing keys for {operation_name}")

        sorted_pks = sorted(existing_pks)
        total_keys = len(sorted_pks)
        range_size = min(range_size, total_keys)

        # Pick random range
        max_start_idx = max(0, total_keys - range_size)
        start_idx = self.rnd.randint(0, max_start_idx)
        end_idx = min(start_idx + range_size - 1, total_keys - 1)

        start_key = sorted_pks[start_idx]
        end_key = sorted_pks[end_idx]
        keys_in_range = sorted_pks[start_idx : end_idx + 1]

        # Build tuple comparison for composite PKs
        pk_tuple_sql = f"({', '.join(pk_columns)})"
        placeholders_start = ", ".join(
            [f"%(_start_{i})s" for i in range(len(pk_columns))]
        )
        placeholders_end = ", ".join(
            [f"%(_end_{i})s" for i in range(len(pk_columns))]
        )

        # Build params
        params = {}
        for i, val in enumerate(start_key):
            params[f"_start_{i}"] = val
        for i, val in enumerate(end_key):
            params[f"_end_{i}"] = val

        where_clause = (
            f"{pk_tuple_sql} >= ({placeholders_start}) AND "
            f"{pk_tuple_sql} <= ({placeholders_end})"
        )
        # Per-column bounds: equality on the leading key columns both ends
        # share, then a range on the next one. When that column is the last
        # key column they select exactly the range and replace the row
        # comparison; otherwise they are added to it. Postgres estimates a
        # row comparison from its first column only (stock: every row has
        # s_w_id = 1, so "all rows") and picks a sequential scan, and even
        # when forced onto the index it reads ~200 pages for 100 rows; with
        # plain per-column bounds it reads the ~15 pages the range covers.
        extra = []
        exact = True
        for i, col in enumerate(pk_columns):
            if start_key[i] == end_key[i]:
                extra.append(f"{col} = %(_start_{i})s")
                continue
            extra.append(f"{col} >= %(_start_{i})s AND {col} <= %(_end_{i})s")
            exact = i == len(pk_columns) - 1
            break
        if extra:
            extra_sql = " AND ".join(extra)
            where_clause = extra_sql if exact else f"{where_clause} AND {extra_sql}"

        return {
            "keys_in_range": keys_in_range,
            "where_clause": where_clause,
            "params": params,
        }

    def track_created_index(self, table_name: str, index_name: str) -> None:
        """Track a created index for later querying/dropping."""
        self._created_indexes.setdefault(table_name, []).append(index_name)

    def untrack_index(self, table_name: str, index_name: str) -> None:
        """Remove an index from tracking."""
        if table_name in self._created_indexes:
            if index_name in self._created_indexes[table_name]:
                self._created_indexes[table_name].remove(index_name)

    def track_created_column(self, table_name: str, column_name: str) -> None:
        """Track a benchmark-created column so it can be safely dropped later."""
        self._created_columns.setdefault(table_name, []).append(column_name)

    def untrack_column(self, table_name: str, column_name: str) -> None:
        """Remove a column from tracking."""
        if table_name in self._created_columns:
            if column_name in self._created_columns[table_name]:
                self._created_columns[table_name].remove(column_name)

    def get_random_created_column(self, table_name: str) -> Optional[str]:
        """Get a random benchmark-created column for this table.

        Only returns columns this benchmark added, never original schema
        columns, so DROP COLUMN can never corrupt the base dataset.
        """
        columns = self._created_columns.get(table_name, [])
        if columns:
            return self.rnd.choice(columns)
        return None

    def get_random_index(self, table_name: str) -> Optional[str]:
        """Get a random index name from the table."""
        indexes = self._created_indexes.get(table_name, [])
        if indexes:
            return self.rnd.choice(indexes)

        # Query database for indexes
        # This is database-specific, simplified version:
        try:
            conn = self.db_tools.get_current_connection()
            cur = conn.cursor()
            cur.execute(f"""
                SELECT indexname FROM pg_indexes
                WHERE tablename = %s
                AND indexname NOT LIKE '%%_pkey'
            """, (table_name,))
            db_indexes = [row[0] for row in cur.fetchall()]
            if db_indexes:
                return self.rnd.choice(db_indexes)
        except Exception:
            pass

        return None

    # ========================================================================
    # Untimed operations for setup phase
    # ========================================================================

    def _insert_without_timing(self, table_name: str) -> None:
        """Insert without timing (for setup)."""
        cur_branch_id = self.current_branch

        col_names = self.get_all_columns(table_name)
        pk_columns = self.get_pk_columns(table_name)

        placeholders = ", ".join([f"%({name})s" for name in col_names])
        insert_sql = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({placeholders});"

        for _ in range(5):
            row_data = self.generate_row(table_name)
            pk_tuple = tuple(row_data[pk] for pk in pk_columns)

            try:
                self.run_sql(insert_sql, row_data, timed=False)
                self._modified_keys.setdefault(cur_branch_id, []).append(pk_tuple)
                return
            except Exception:
                continue

        raise ValueError("Failed to insert row after 5 attempts")

    def _update_without_timing(self, table_name: str) -> None:
        """Update without timing (for setup)."""
        key_to_update = self.select_random_key(table_name)
        if not key_to_update:
            return  # Skip if no keys

        pk_columns = self.get_pk_columns(table_name)
        all_columns = self.get_all_columns(table_name)
        non_pk_columns = [col for col in all_columns if col not in pk_columns]

        if not non_pk_columns:
            return

        row_data = self.generate_row(table_name)
        set_clause = ", ".join([f"{col} = %({col})s" for col in non_pk_columns])
        where_clause = " AND ".join(
            [f"{pk_name} = %({pk_name})s" for pk_name in pk_columns]
        )
        update_sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};"

        for i, pk_col in enumerate(pk_columns):
            row_data[pk_col] = key_to_update[i]

        self.run_sql(update_sql, row_data, timed=False)
        self.track_modified_key(key_to_update)

    def _delete_without_timing(self, table_name: str) -> None:
        """Delete without timing (for setup)."""
        key_to_delete = self.select_random_key(table_name)
        if not key_to_delete:
            return

        pk_columns = self.get_pk_columns(table_name)
        where_clause = " AND ".join([f"{pk_name} = %s" for pk_name in pk_columns])
        delete_sql = f"DELETE FROM {table_name} WHERE {where_clause};"

        self.run_sql(delete_sql, key_to_delete, timed=False)
        self.untrack_modified_key(key_to_delete)


# ============================================================================
# Operation Execution
# ============================================================================


class OperationRunner:
    """Creates and executes operations using the registry pattern."""

    def __init__(self, config: BenchmarkConfig, context: WorkerContext,
                 op_type: Optional[int] = None):
        self.config = config
        self.context = context
        # The configured operation, or another one (a background client's)
        self.op_type = config.operation_type if op_type is None else op_type

        # Create the operation instance
        self.operation = self._create_operation()

    def run_until(self, stopped) -> Tuple[int, int, str]:
        """Run the operation back to back until stopped() is true (a
        background client). Returns (completed, failed, first error); failures
        are not recorded as the measured run's failures."""
        done = failed = 0
        first_error = ""
        while not stopped():
            try:
                self.operation.execute(self.context)
                done += 1
            except Exception as e:
                failed += 1
                first_error = first_error or f"{type(e).__name__}: {e}"[:300]
        return done, failed, first_error

    def _create_operation(self):
        """Create the operation instance based on config."""
        op_type = self.op_type
        table_name = self.config.table_name

        # Operation-specific parameters
        if op_type in [tp.OperationType.RANGE_READ, tp.OperationType.RANGE_UPDATE]:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                range_size=self.config.range_size
            )
        elif op_type == tp.OperationType.DDL_ADD_INDEX:
            column_name = self.config.ddl_config.index_column_name or None
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                column_name=column_name
            )
        elif op_type == tp.OperationType.DDL_REMOVE_INDEX:
            return OperationRegistry.create(op_type, table_name=table_name)
        elif op_type == tp.OperationType.DDL_VACUUM:
            return OperationRegistry.create(op_type, table_name=table_name)
        elif op_type == tp.OperationType.DDL_ADD_COLUMN:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                column_type=(
                    self.config.ddl_config.column_type or "INTEGER"),)
        elif op_type == tp.OperationType.DDL_REMOVE_COLUMN:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,)
        elif op_type == tp.OperationType.DDL_BACKFILL:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                backfill_fraction=(
                    self.config.ddl_config.backfill_fraction or 1.0),
                column_name=self.config.ddl_config.column_name or None,)
        elif op_type == tp.OperationType.DDL_ADD_COLUMN_WITH_DEFAULT:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                column_type=(
                    self.config.ddl_config.column_type or "INTEGER"
                ),
                default_value=self.config.ddl_config.default_value or "0",)
        elif op_type == tp.OperationType.DDL_TYPE_CHANGE:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                new_type=self.config.ddl_config.column_type or "BIGINT",
            )
        elif op_type == tp.OperationType.DDL_RENAME_COLUMN:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                new_column_name=self.config.ddl_config.new_column_name or None,
            )
        elif op_type == tp.OperationType.DDL_RENAME_TABLE:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                new_table_name=self.config.ddl_config.new_table_name or None,
            )
        elif op_type == tp.OperationType.DDL_ADD_FK:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                ref_table_name=self.config.ddl_config.ref_table_name or None,
                ref_column_name=self.config.ddl_config.ref_column_name or None,
            )
        elif op_type == tp.OperationType.DDL_ADD_CHECK:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                check_expression=self.config.ddl_config.check_expression or "1=1",
            )
        elif op_type == tp.OperationType.DDL_SET_DEFAULT:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                default_value=self.config.ddl_config.default_value or "0",
            )
        elif op_type == tp.OperationType.DDL_SET_NOT_NULL:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
            )
        elif op_type == tp.OperationType.DDL_EXPAND_CONTRACT:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_type=self.config.ddl_config.column_type or "INTEGER",
            )
        elif op_type in [
            tp.OperationType.READ,
            tp.OperationType.INSERT,
            tp.OperationType.UPDATE,
            tp.OperationType.DELETE,
            tp.OperationType.MIXED,
        ]:
            return OperationRegistry.create(op_type, table_name=table_name)
        elif op_type == tp.OperationType.BRANCH_LOG:
            cfg = self.config.branch_op_config
            return OperationRegistry.create(
                op_type, table_name=table_name,
                changes_per_op=cfg.changes_per_op or 1,
                history_commits=cfg.history_commits or 10,
                log_limit=cfg.log_limit or 10,
            )
        elif op_type in VERB_OPERATIONS:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                changes_per_op=self.config.branch_op_config.changes_per_op or 1,
            )
        else:
            # Branch operations and others without parameters
            return OperationRegistry.create(op_type)

    def execute_single(self) -> None:
        """Execute the operation once."""
        try:
            self.operation.execute(self.context)
        except Exception as e:
            if self.context.shared_progress:
                self.context.shared_progress.write(
                    f"[Thread {self.context.thread_id}] Operation failed: {e}"
                )
            raise

    def execute_multiple(
        self,
        num_ops: int,
        warmup_ops: int = 0,
        timed_phase: Optional['TimedPhase'] = None,
    ) -> None:
        """Execute the operation N times with optional warm-up.

        Args:
            num_ops: Number of operations to execute (timed)
            warmup_ops: Number of warm-up operations to execute first (not counted in results)
            timed_phase: Shared start barrier and timed window. The timed ops
                start only once every worker has finished its warm-up.
        """
        # Execute warm-up operations (if any). They are not recorded, so the
        # results only ever hold timed ops.
        if warmup_ops > 0:
            if self.context.thread_id == 0:
                print(f"Executing {warmup_ops} warm-up operations per thread...")

            self.context.result_collector.set_recording(False)
            try:
                for i in range(warmup_ops):
                    try:
                        self.operation.execute(self.context)
                    except Exception as e:
                        # Build detailed error message for warmup
                        op_name = self.context.config.operation_name
                        error_type = type(e).__name__
                        msg = f"[Thread {self.context.thread_id}] Warm-up operation {i+1}/{warmup_ops} failed: {op_name} - {error_type}: {e}"
                        if self.context.shared_progress:
                            self.context.shared_progress.write(msg)
                        else:
                            print(msg)
            finally:
                self.context.result_collector.set_recording(True)

            if self.context.thread_id == 0:
                print(f"Warm-up complete. Starting timed measurement of {num_ops} operations...")

        if timed_phase:
            timed_phase.wait()

        # Execute timed operations
        for i in range(num_ops):
            try:
                self.operation.execute(self.context)

                # Update progress
                if self.context.shared_progress:
                    self.context.shared_progress.update(1)

            except Exception as e:
                # Record the failure with details
                self.context.result_collector.record_failure(error=e, operation_number=i+1)

                # Build detailed error message
                op_name = self.context.config.operation_name
                error_type = type(e).__name__
                msg = f"[Thread {self.context.thread_id}] Operation {i+1}/{num_ops} failed: {op_name} - {error_type}: {e}"

                if self.context.shared_progress:
                    self.context.shared_progress.write(msg)
                else:
                    print(msg)

                # Update progress even for failed ops
                if self.context.shared_progress:
                    self.context.shared_progress.update(1)

        if timed_phase:
            timed_phase.finish()


class AsyncOperationRunner:
    """Executes operations asynchronously with configurable concurrency.

    This runner uses asyncio to execute multiple operations concurrently,
    controlled by a semaphore to limit the number of concurrent requests.
    Each worker opens a pool with one connection per concurrent request, so
    the requests run in parallel on the server rather than queueing on a
    single connection.

    Each operation is timed independently with accurate start/end timestamps,
    ensuring correct performance measurement even with overlapping execution.
    Latency starts after a pool connection is acquired; the wait for one is
    recorded separately as pool_wait_time.
    """

    def __init__(self, config: BenchmarkConfig, context: WorkerContext):
        self.config = config
        self.context = context
        self.operation = self._create_operation()
        self.concurrent_limit = config.concurrent_requests

    def _create_operation(self):
        """Create the operation instance based on config."""
        # Reuse the same operation creation logic as OperationRunner
        op_type = self.config.operation_type
        table_name = self.config.table_name

        # Operation-specific parameters
        if op_type in [tp.OperationType.RANGE_READ, tp.OperationType.RANGE_UPDATE]:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                range_size=self.config.range_size
            )
        elif op_type == tp.OperationType.DDL_ADD_INDEX:
            column_name = self.config.ddl_config.index_column_name or None
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                column_name=column_name
            )
        elif op_type == tp.OperationType.DDL_REMOVE_INDEX:
            return OperationRegistry.create(op_type, table_name=table_name)
        elif op_type == tp.OperationType.DDL_VACUUM:
            return OperationRegistry.create(op_type, table_name=table_name)
        elif op_type == tp.OperationType.DDL_ADD_COLUMN:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                column_type=(
                    self.config.ddl_config.column_type or "INTEGER"
                ),
            )
        elif op_type == tp.OperationType.DDL_REMOVE_COLUMN:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
            )
        elif op_type == tp.OperationType.DDL_BACKFILL:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                backfill_fraction=(
                    self.config.ddl_config.backfill_fraction or 1.0),
                column_name=self.config.ddl_config.column_name or None,
            )
        elif op_type == tp.OperationType.DDL_ADD_COLUMN_WITH_DEFAULT:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                column_type=(
                    self.config.ddl_config.column_type or "INTEGER"
                ),
                default_value=self.config.ddl_config.default_value or "0",
            )
        elif op_type == tp.OperationType.DDL_TYPE_CHANGE:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                new_type=self.config.ddl_config.column_type or "BIGINT",
            )
        elif op_type == tp.OperationType.DDL_RENAME_COLUMN:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                new_column_name=self.config.ddl_config.new_column_name or None,
            )
        elif op_type == tp.OperationType.DDL_RENAME_TABLE:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                new_table_name=self.config.ddl_config.new_table_name or None,
            )
        elif op_type == tp.OperationType.DDL_ADD_FK:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                ref_table_name=self.config.ddl_config.ref_table_name or None,
                ref_column_name=self.config.ddl_config.ref_column_name or None,
            )
        elif op_type == tp.OperationType.DDL_ADD_CHECK:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                check_expression=self.config.ddl_config.check_expression or "1=1",
            )
        elif op_type == tp.OperationType.DDL_SET_DEFAULT:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
                default_value=self.config.ddl_config.default_value or "0",
            )
        elif op_type == tp.OperationType.DDL_SET_NOT_NULL:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_name=self.config.ddl_config.column_name or None,
            )
        elif op_type == tp.OperationType.DDL_EXPAND_CONTRACT:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                column_type=self.config.ddl_config.column_type or "INTEGER",
            )
        elif op_type in [
            tp.OperationType.READ,
            tp.OperationType.INSERT,
            tp.OperationType.UPDATE,
            tp.OperationType.DELETE,
            tp.OperationType.MIXED,
        ]:
            return OperationRegistry.create(op_type, table_name=table_name)
        elif op_type == tp.OperationType.BRANCH_LOG:
            cfg = self.config.branch_op_config
            return OperationRegistry.create(
                op_type, table_name=table_name,
                changes_per_op=cfg.changes_per_op or 1,
                history_commits=cfg.history_commits or 10,
                log_limit=cfg.log_limit or 10,
            )
        elif op_type in VERB_OPERATIONS:
            return OperationRegistry.create(
                op_type, table_name=table_name,
                changes_per_op=self.config.branch_op_config.changes_per_op or 1,
            )
        else:
            # Branch operations and others without parameters
            return OperationRegistry.create(op_type)

    async def _execute_single_async(
        self, semaphore: asyncio.Semaphore, op_number: int, warmup: bool = False
    ) -> dict:
        """Execute a single operation asynchronously with semaphore control.

        Args:
            semaphore: Semaphore to limit concurrent execution
            op_number: Operation number for error reporting
            warmup: Warm-up op; not counted in the progress bar

        Returns:
            dict with status information for debugging
        """
        progress = None if warmup else self.context.shared_progress
        async with semaphore:
            try:
                # Check if operation has async version
                if hasattr(self.operation, 'execute_async'):
                    await self.operation.execute_async(self.context)
                else:
                    # Fall back to running sync operation in thread pool
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, self.operation.execute, self.context)

                # Update progress
                if progress:
                    progress.update(1)

                return {"status": "success", "op_number": op_number}

            except Exception as e:
                # Record the failure
                self.context.result_collector.record_failure(error=e, operation_number=op_number)

                # Build detailed error message
                op_name = self.context.config.operation_name
                error_type = type(e).__name__
                msg = f"[Thread {self.context.thread_id}] Async operation {op_number} failed: {op_name} - {error_type}: {e}"

                if self.context.shared_progress:
                    self.context.shared_progress.write(msg)
                else:
                    print(msg)

                # Update progress even for failed ops
                if progress:
                    progress.update(1)

                return {"status": "failed", "op_number": op_number, "error": str(e)}

    async def _ensure_async_pool(self):
        """Open the worker's async pool: one connection per concurrent
        request, so a request never waits for a connection."""
        if self.context.db_tools.async_pool:
            return
        await self.context.db_tools.open_async_pool(self.concurrent_limit)

    async def execute_multiple_async(
        self,
        num_ops: int,
        warmup_ops: int = 0,
        timed_phase: Optional['TimedPhase'] = None,
    ) -> None:
        """Execute operations asynchronously with configurable concurrency.

        Args:
            num_ops: Number of operations to execute (timed)
            warmup_ops: Number of warm-up operations to execute first (not counted in results)
            timed_phase: Shared start barrier and timed window. The timed ops
                start only once every worker has opened its pool and finished
                its warm-up.
        """
        # Ensure async connection pool is initialized
        await self._ensure_async_pool()

        # Create semaphore to limit concurrent executions
        semaphore = asyncio.Semaphore(self.concurrent_limit)

        # Execute warm-up operations (if any)
        if warmup_ops > 0:
            if self.context.thread_id == 0:
                print(f"Executing {warmup_ops} warm-up operations with concurrency={self.concurrent_limit}...")

            warmup_tasks = [
                self._execute_single_async(semaphore, i+1, warmup=True)
                for i in range(warmup_ops)
            ]
            # Warm-up ops are not recorded. The tasks run on this thread, so
            # the per-thread switch covers all of them.
            self.context.result_collector.set_recording(False)
            try:
                await asyncio.gather(*warmup_tasks, return_exceptions=True)
            finally:
                self.context.result_collector.set_recording(True)

            if self.context.thread_id == 0:
                print(f"Warm-up complete. Starting timed measurement of {num_ops} operations...")

        if timed_phase:
            # Wait off the event loop so the pool's background tasks keep running.
            await asyncio.get_running_loop().run_in_executor(None, timed_phase.wait)

        # Execute timed operations concurrently
        tasks = [
            self._execute_single_async(semaphore, i+1)
            for i in range(num_ops)
        ]

        # Wait for all operations to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        if timed_phase:
            timed_phase.finish()

        # Analyze results
        successes = [r for r in results if isinstance(r, dict) and r.get("status") == "success"]
        failures = [r for r in results if isinstance(r, dict) and r.get("status") == "failed"]
        exceptions = [r for r in results if isinstance(r, Exception)]
        nones = [r for r in results if r is None]

        # Record any uncaught exceptions as failures
        # These are exceptions that escaped the try/except in _execute_single_async
        if exceptions:
            for exc in exceptions:
                self.context.result_collector.record_failure(
                    error=exc,
                    operation_number=None
                )

        # Check for issues
        has_failures = len(failures) > 0
        has_exceptions = len(exceptions) > 0
        has_nones = len(nones) > 0
        total_returned = len(results)
        total_accounted = len(successes) + len(failures) + len(exceptions) + len(nones)
        has_accounting_error = total_accounted != total_returned
        has_missing_results = total_returned != num_ops

        # Only print detailed output if this thread has issues
        has_issues = (has_failures or has_exceptions or has_nones or
                     has_accounting_error or has_missing_results)

        if has_issues:
            print(f"[Thread {self.context.thread_id}] WARNING: Issues detected during async execution")
            print(f"  Tasks created: {num_ops}")
            print(f"  Successes: {len(successes)}/{num_ops} ({100*len(successes)/num_ops:.1f}%)")

            if has_failures:
                print(f"  Failures (caught): {len(failures)}")

            if has_exceptions:
                print(f"  Exceptions (uncaught): {len(exceptions)}")
                # Print first 5 exceptions with stack traces
                for i, exc in enumerate(exceptions[:5]):
                    print(f"    Exception {i+1}: {type(exc).__name__}: {exc}")
                    import traceback
                    traceback.print_exception(type(exc), exc, exc.__traceback__)

            if has_nones:
                print(f"  None values: {len(nones)} (unexpected!)")

            if has_accounting_error:
                print(f"  ERROR: Accounted results ({total_accounted}) != returned results ({total_returned})")

            if has_missing_results:
                print(f"  WARNING: Results returned ({total_returned}) != tasks created ({num_ops})")

        # Return stats for aggregation
        return {
            "thread_id": self.context.thread_id,
            "num_ops": num_ops,
            "successes": len(successes),
            "failures": len(failures),
            "exceptions": len(exceptions),
            "nones": len(nones),
            "has_issues": has_issues
        }

    def execute_multiple(
        self,
        num_ops: int,
        warmup_ops: int = 0,
        timed_phase: Optional['TimedPhase'] = None,
    ) -> dict:
        """Synchronous wrapper that runs async execution in event loop.

        Args:
            num_ops: Number of operations to execute (timed)
            warmup_ops: Number of warm-up operations to execute first
            timed_phase: Shared start barrier and timed window

        Returns:
            dict with execution statistics (successes, failures, etc.)
        """
        # Create and manage event loop explicitly to avoid cleanup issues in threads
        # This prevents "_ssock not found" errors in Python 3.12+ when running
        # multiple event loops in ThreadPoolExecutor workers
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(
                self.execute_multiple_async(num_ops, warmup_ops, timed_phase)
            )
        finally:
            # Close the async pool on its own loop, before that loop is
            # closed, so the server sees clean disconnects. This must come
            # before cancelling leftover tasks: the pool's background workers
            # are tasks on this loop, and close() waits for them to stop.
            try:
                loop.run_until_complete(self.context.db_tools.close_async_pool())
            except (Exception, asyncio.CancelledError) as e:
                print(f"[Thread {self.context.thread_id}] Failed to close async pool: {e!r}")
            try:
                # Cancel any remaining tasks
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                # Run loop once more to process cancellations
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            finally:
                loop.close()


# ============================================================================
# Benchmark Execution
# ============================================================================


def _merge_support(summaries: list) -> dict:
    """Combine the support summaries of several client processes."""
    counts: Dict[str, Dict[str, int]] = {}
    unsupported = set()
    for s in summaries:
        unsupported.update(s.get("unsupported_ops", []))
        for op, by_status in s.get("op_status_counts", {}).items():
            dst = counts.setdefault(op, {})
            for status, n in by_status.items():
                dst[status] = dst.get(status, 0) + n
    return {"workflow_supported": not unsupported, "unsupported_ops": sorted(unsupported),
            "op_status_counts": counts}


def _client_process_main(proc_idx, config_bytes, backend_info, setup_branches, thread_ids,
                         assignments, seed, output_dir, shared, results_q, background=None) -> None:
    """Entry point of one client process (num_processes > 1): run this
    process's worker threads behind the shared start barrier, write their
    rows to <run_id>.part<i>.parquet, and report back on results_q."""
    out = {"proc": proc_idx, "part": None, "error": None, "failed": [],
           "support": None, "client_cpu": None}
    try:
        register_all_operations()
        proto = tp.TaskConfig()
        proto.ParseFromString(config_bytes)
        config = BenchmarkConfig(proto)
        collector = rc.ResultCollector(run_id=config.run_id, output_dir=output_dir)
        executor = BenchmarkExecutor(config, backend_info, setup_branches, collector)
        measured_here = sum(1 for t in thread_ids if t < config.num_threads)
        timed = ProcessTimedPhase(shared, measured_here, background)
        executor._timed_phase = timed
        executor._bg = background
        progress = SharedProgress(total=len(thread_ids) * config.num_ops, disable=True)
        executor._execute_multi_threaded(seed, assignments, progress, thread_ids=thread_ids)
        part = f"{config.run_id}.part{proc_idx}.parquet"
        if collector.total_rows:
            collector.write_to_parquet(filename=part, append=False)
            out["part"] = os.path.join(output_dir, part)
        out.update(failed=list(collector.failed_operations), support=collector.support_summary(),
                   client_cpu=timed.client_cpu_seconds(),
                   background_failed=executor._background_failed,
                   background_error=executor._background_error)
    except BaseException as e:
        out["error"] = f"{type(e).__name__}: {e}"
        try:
            shared[0].abort()   # release the other processes' workers
            if background is not None:
                background.stop()
        except Exception:
            pass
    finally:
        results_q.put(out)


def _latency_quantiles(rows: list) -> dict:
    """n, mean and p50/p95/p99/max of the rows' latencies (seconds)."""
    if not rows:
        return {"n": 0}
    lat = np.array([r.latency for r in rows], dtype=float)
    return {
        "n": int(lat.size),
        "mean": float(lat.mean()),
        "p50": float(np.percentile(lat, 50)),
        "p95": float(np.percentile(lat, 95)),
        "p99": float(np.percentile(lat, 99)),
        "max": float(lat.max()),
    }


class BenchmarkExecutor:
    """Orchestrates benchmark execution with thread management."""

    def __init__(
        self,
        config: BenchmarkConfig,
        backend_info: BackendInfo,
        setup_branches: List[str],
        result_collector: rc.ResultCollector,
    ):
        self.config = config
        self.backend_info = backend_info
        self.setup_branches = setup_branches
        self.result_collector = result_collector

        # Shared state
        self.branch_manager = SharedBranchManager(initial_branches=setup_branches)
        # Background clients (background_load): control and failure count
        self._bg: Optional[BackgroundControl] = None
        self._background_failed = 0
        self._background_error = ""
        self._bg_lock = threading.Lock()

    def execute(self) -> dict:
        """Execute the benchmark and return metrics.

        Returns:
            dict with timing and throughput metrics
        """
        num_threads = self.config.num_threads
        num_ops = self.config.num_ops
        total_ops = num_ops * num_threads

        print(f"\n{'='*60}")
        print(f"Benchmark Execution (TIMED)")
        print(f"{'='*60}")
        print(f"Operation: {self.config.operation_name}")
        print(f"Threads: {num_threads}")
        print(f"Operations per thread: {num_ops}")
        print(f"Total operations: {total_ops}")

        # Create shared progress bar
        shared_progress = SharedProgress(
            total=total_ops,
            desc=f"Benchmark ({num_threads} threads)",
            disable=False,
        )

        # Partition branches among threads
        thread_branch_assignments = self._assign_branches_to_threads()

        # Fixed seed for reproducibility
        fixed_seed = int(time.time())
        print(f"Seed: {fixed_seed}")
        print(f"{'='*60}\n")

        wall_start = time.time()
        merged = None
        remote = RemoteCpuSampler().start()
        if self.config.num_processes > 1:
            # Threads spread over client processes; rows merged afterwards.
            shared_progress.close()
            merged = self._execute_multi_process(fixed_seed, thread_branch_assignments)
        else:
            # All workers start their timed ops together; see TimedPhase.
            self._timed_phase = TimedPhase(num_threads)
            if self.config.background_workers:
                self._bg = BackgroundControl(self.config.background_workers)
                self._timed_phase.background = self._bg
            try:
                if num_threads == 1 and not self._bg:
                    # Single-threaded execution
                    self._execute_single_threaded(
                        fixed_seed, thread_branch_assignments.get(0, []), shared_progress
                    )
                else:
                    # Multi-threaded execution
                    self._execute_multi_threaded(
                        fixed_seed, thread_branch_assignments, shared_progress
                    )
            finally:
                shared_progress.close()

        # Throughput window: from the start barrier (every worker connected
        # and warmed up) to the last worker finishing its timed ops. Thread
        # start-up, connection/pool setup and teardown are excluded.
        start_time = self._timed_phase.start_time
        end_time = self._timed_phase.end_time
        time.sleep(remote.period * 1.5 if remote.targets else 0)   # a sample after the window
        remote.stop()
        if start_time is None or end_time is None:
            raise RuntimeError(
                "No worker completed the timed phase; see the worker errors above."
            )
        elapsed_time = end_time - start_time
        wall_time = time.time() - wall_start

        all_rows = self.result_collector.results if merged is None else merged["rows"]
        measured, background = self._split_rows(all_rows)
        if merged is None:
            metrics = self._calculate_metrics(total_ops, elapsed_time, rows=measured)
            cpu = self._timed_phase.cpu_usage()
        else:
            metrics = self._calculate_metrics(
                total_ops, elapsed_time, rows=measured,
                failed_ops=merged["failed"], support=merged["support"],
            )
            metrics["rows_merged"] = True   # already in <run_id>.parquet
            cpu = merged["cpu"]
            self._background_failed = merged.get("background_failed", 0)
            self._background_error = merged.get("background_error", "")
        if self.config.background_workers:
            metrics["background"] = self._background_metrics(background, start_time, end_time)
        metrics["wall_time"] = wall_time
        metrics["num_processes"] = self.config.num_processes
        metrics["remote_cpu"] = remote.usage(start_time, end_time)
        # Average busy cores over the timed window: the server process (when
        # co-located, DBFORK_SERVER_PID) and all client processes together.
        for who in ("server", "client"):
            secs = cpu.get(f"{who}_cpu_seconds")
            metrics[f"{who}_cpu_cores"] = (secs / elapsed_time
                                           if secs is not None and elapsed_time > 0 else None)
        self._print_metrics(metrics)

        return metrics

    def _split_rows(self, rows) -> Tuple[list, list]:
        """(measured rows, background rows): background clients have
        thread ids from num_threads up."""
        t = self.config.num_threads
        measured = [r for r in rows if r.thread_id < t]
        return measured, [r for r in rows if r.thread_id >= t]

    def _background_metrics(self, rows, start: float, end: float) -> dict:
        """What the background clients did inside the timed window."""
        inside = [r for r in rows if r.status == rslt.OpStatus.OK and start <= r.start_time <= end]
        ops = sum(1 for r in inside if r.op_type in rc.VERB_OP_TYPES)
        statements = [r for r in inside if r.op_type in rc.STATEMENT_OP_TYPES]
        elapsed = end - start
        return {
            "operation": tp.OperationType.Name(self.config.background_operation),
            "clients": self.config.background_workers,
            "ops_in_window": ops,
            "throughput": ops / elapsed if elapsed > 0 else 0.0,
            "failed": self._background_failed,
            "first_error": self._background_error or None,
            "latency": {"statements": _latency_quantiles(statements)},
        }

    def _background_branches(self) -> List[str]:
        root = self.backend_info.root_branch_name
        return [b for b in self.setup_branches if b != root]

    def _execute_multi_process(self, fixed_seed: int, assignments: Dict[int, List[str]]) -> dict:
        """Run the worker threads in num_processes client processes (spawned,
        each running its share of thread ids with _execute_multi_threaded),
        then merge their rows into <run_id>.parquet. Returns the rows and
        counters the metrics need."""
        num_threads = self.config.num_threads
        nproc = self.config.num_processes
        ctx = mp.get_context("spawn")
        times = ctx.Array("d", [0.0, 0.0, -1.0, -1.0], lock=False)
        done = ctx.Value("i", 0, lock=False)
        shared = (ctx.Barrier(num_threads), times, done, ctx.Lock(), num_threads)
        results_q = ctx.Queue()
        n_bg = self.config.background_workers
        background = BackgroundControl(n_bg, ctx) if n_bg else None
        groups = [list(range(num_threads + n_bg))[i::nproc] for i in range(nproc)]
        out_dir = self.result_collector.output_dir
        print(f"Client processes: {nproc} ({min(map(len, groups))}-{max(map(len, groups))} threads each)")

        procs = []
        for i, tids in enumerate(groups):
            p = ctx.Process(
                target=_client_process_main, name=f"client-{i}",
                args=(i, self.config._proto.SerializeToString(), self.backend_info,
                      self.setup_branches, tids, {t: assignments.get(t, []) for t in tids},
                      fixed_seed, out_dir, shared, results_q, background),
            )
            p.start()
            procs.append(p)
        results = []
        while len(results) < len(procs):
            try:
                results.append(results_q.get(timeout=5))
            except queue.Empty:
                if not any(p.is_alive() for p in procs):
                    break   # a process died without reporting
        for p in procs:
            p.join()
        for r in results:
            if r["error"]:
                print(f"Client process {r['proc']} failed: {r['error']}")
        if len(results) < len(procs):
            print(f"{len(procs) - len(results)} client process(es) exited without reporting")

        # Merge the per-process row files into the run's parquet file.
        parts = sorted(r["part"] for r in results if r["part"])
        rows = []
        if parts:
            table = pa.concat_tables([pq.read_table(p) for p in parts], promote_options="default")
            main_path = os.path.join(out_dir, f"{self.config.run_id}.parquet")
            if os.path.exists(main_path):
                full = pa.concat_tables([pq.read_table(main_path), table], promote_options="default")
            else:
                full = table
            pq.write_table(full, main_path)
            for p in parts:
                os.remove(p)
            print(f"Merged {len(table)} rows from {len(parts)} client processes into {main_path}")
            cols = table.select(["op_type", "status", "latency", "thread_id", "start_time"]).to_pydict()
            rows = [SimpleNamespace(op_type=o, status=rslt.OpStatus.Value(s), latency=l,
                                    thread_id=t, start_time=st)
                    for o, s, l, t, st in zip(cols["op_type"], cols["status"], cols["latency"],
                                              cols["thread_id"], cols["start_time"])]

        failed = [f for r in results for f in r["failed"]]
        self.result_collector.failed_operations.extend(failed)
        support = _merge_support([r["support"] for r in results if r["support"]])
        start, end = times[0] or None, times[1] or None
        self._timed_phase = SimpleNamespace(start_time=start, end_time=end)
        server_cpu = times[3] - times[2] if times[2] >= 0 and times[3] >= 0 else None
        client = [r["client_cpu"] for r in results]
        client_cpu = sum(client) if client and None not in client else None
        return {"rows": rows, "failed": len(failed), "support": support,
                "background_failed": sum(r.get("background_failed", 0) for r in results),
                "background_error": next((r["background_error"] for r in results if r.get("background_error")), ""),
                "cpu": {"server_cpu_seconds": server_cpu, "client_cpu_seconds": client_cpu}}

    def _assign_branches_to_threads(self) -> Dict[int, List[str]]:
        """Assign branches to threads for multi-threaded execution."""
        num_threads = self.config.num_threads
        if not self.setup_branches or num_threads == 1:
            return {}

        # The setup worker lists the backend's root branch too; load goes to
        # the setup branches only, so N branches means N loaded branches.
        root = self.backend_info.root_branch_name
        branches = [b for b in self.setup_branches if b != root]
        if not branches:
            return {}

        assignments = {}
        num_branches = len(branches)

        if num_threads <= num_branches:
            # Round-robin assignment
            for tid in range(num_threads):
                assignments[tid] = [
                    branches[idx]
                    for idx in range(tid, num_branches, num_threads)
                ]
        else:
            # More threads than branches: cyclic assignment
            for tid in range(num_threads):
                assigned_branch = branches[tid % num_branches]
                assignments[tid] = [assigned_branch]

        return assignments

    def loaded_branches(self) -> int:
        """Branches that get load: one per thread up to the setup branches
        (each worker connects to its first assigned branch), or 1 without setup."""
        assignments = self._assign_branches_to_threads()
        if not assignments:
            return 1
        return len({v[0] for v in assignments.values() if v})

    def _execute_single_threaded(
        self, seed: int, assigned_branches: List[str], progress: SharedProgress
    ) -> None:
        """Execute benchmark in single thread."""
        rc.set_current_thread_id(0)

        with WorkerContext(
            config=self.config,
            backend_info=self.backend_info,
            thread_id=0,
            seed=seed,
            result_collector=self.result_collector,
            branch_manager=self.branch_manager,
            shared_progress=progress,
            assigned_branches=assigned_branches,
        ) as ctx:
            # Initialize table name if needed
            if not self.config.table_name:
                all_tables = dbh.get_all_tables(ctx.db_tools.get_current_connection())
                self.config._proto.table_name = ctx.rnd.choice(all_tables)

            # Set context for result collection
            table_name = self.config.table_name
            table_schema = ctx.get_table_schema(table_name)
            initial_db_size = ctx.initial_db_size()

            ctx.db_tools.result_collector.set_context(
                table_name=table_name,
                table_schema=table_schema,
                initial_db_size=initial_db_size,
                seed=seed,
            )

            # Create and execute operations
            # Use async runner if use_async or concurrent_requests > 1
            if self.config.use_async:
                runner = AsyncOperationRunner(self.config, ctx)
                stats = runner.execute_multiple(
                    self.config.num_ops, self.config.warmup_ops, self._timed_phase
                )
                # Print aggregate summary for async execution
                self._print_aggregate_summary([stats])
            else:
                runner = OperationRunner(self.config, ctx)
                runner.execute_multiple(
                    self.config.num_ops, self.config.warmup_ops, self._timed_phase
                )

    def _execute_multi_threaded(
        self,
        fixed_seed: int,
        branch_assignments: Dict[int, List[str]],
        progress: SharedProgress,
        thread_ids: Optional[List[int]] = None,
    ) -> None:
        """Execute benchmark with multiple threads (``thread_ids``: this
        process's share when the threads are spread over client processes)."""

        def worker_fn(thread_id: int, assigned_branches: List[str]):
            """Worker function for each thread.

            Returns:
                dict with stats if using async runner, None otherwise
            """
            try:
                return run_worker(thread_id, assigned_branches)
            except BaseException:
                # If this worker never reaches the start barrier, release the
                # others instead of leaving them waiting forever. No-op once
                # the timed phase has started.
                self._timed_phase.abort()
                raise

        def run_background(thread_id: int, branch: str):
            """A background client: keep ``branch`` busy until stopped."""
            rc.set_current_thread_id(thread_id)
            seed = fixed_seed + thread_id
            with WorkerContext(
                config=self.config, backend_info=self.backend_info, thread_id=thread_id,
                seed=seed, result_collector=self.result_collector,
                branch_manager=self.branch_manager, shared_progress=progress,
                assigned_branches=[branch],
            ) as ctx:
                table_name = self.config.table_name
                ctx.db_tools.result_collector.set_context(
                    table_name=table_name, table_schema=ctx.get_table_schema(table_name),
                    initial_db_size=ctx.initial_db_size(), seed=seed,
                )
                runner = OperationRunner(self.config, ctx, op_type=self.config.background_operation)
                # One untimed operation first (connection, key cache, row
                # generator), so the client is really working once it
                # reports started.
                ctx.db_tools.result_collector.set_recording(False)
                try:
                    runner.operation.execute(ctx)
                except Exception:
                    pass
                finally:
                    ctx.db_tools.result_collector.set_recording(True)
                self._bg.started()
                _, failed, error = runner.run_until(self._bg.stopped)
                with self._bg_lock:
                    self._background_failed += failed
                    self._background_error = self._background_error or error
            return None

        def run_worker(thread_id: int, assigned_branches: List[str]):
            if thread_id >= self.config.num_threads:
                return run_background(thread_id, assigned_branches[0])
            rc.set_current_thread_id(thread_id)
            worker_seed = fixed_seed + thread_id

            with WorkerContext(
                config=self.config,
                backend_info=self.backend_info,
                thread_id=thread_id,
                seed=worker_seed,
                result_collector=self.result_collector,
                branch_manager=self.branch_manager,
                shared_progress=progress,
                assigned_branches=assigned_branches,
            ) as ctx:
                # Initialize table name if needed
                if not self.config.table_name:
                    all_tables = dbh.get_all_tables(ctx.db_tools.get_current_connection())
                    self.config._proto.table_name = ctx.rnd.choice(all_tables)

                # Set context
                table_name = self.config.table_name
                table_schema = ctx.get_table_schema(table_name)
                initial_db_size = ctx.initial_db_size()

                ctx.db_tools.result_collector.set_context(
                    table_name=table_name,
                    table_schema=table_schema,
                    initial_db_size=initial_db_size,
                    seed=worker_seed,
                )

                # With background load, start measuring only once every
                # background client is running.
                if self._bg is not None:
                    self._bg.wait_started()

                # Create and execute operations
                # Use async runner if use_async or concurrent_requests > 1
                if self.config.use_async:
                    runner = AsyncOperationRunner(self.config, ctx)
                    return runner.execute_multiple(
                        self.config.num_ops, self.config.warmup_ops, self._timed_phase
                    )
                else:
                    runner = OperationRunner(self.config, ctx)
                    runner.execute_multiple(
                        self.config.num_ops, self.config.warmup_ops, self._timed_phase
                    )
                    return None

        # Execute with thread pool; background clients (thread ids from
        # num_threads up) each keep one setup branch busy.
        if thread_ids is None:
            thread_ids = list(range(self.config.num_threads + self.config.background_workers))
        bg_branches = self._background_branches()
        def assigned(tid: int) -> List[str]:
            if tid >= self.config.num_threads:
                return [bg_branches[tid - self.config.num_threads]]
            return branch_assignments.get(tid, [])
        with ThreadPoolExecutor(max_workers=len(thread_ids)) as executor:
            futures = [
                executor.submit(worker_fn, thread_id=tid, assigned_branches=assigned(tid))
                for tid in thread_ids
            ]

            # Wait for completion and collect stats
            thread_stats = []
            for future in as_completed(futures):
                try:
                    stats = future.result()
                    if stats is not None:  # Async runner returns stats
                        thread_stats.append(stats)
                except Exception as e:
                    print(f"Worker thread failed: {type(e).__name__}: {e}")

            # Print aggregate summary for async execution
            if thread_stats:
                self._print_aggregate_summary(thread_stats)

    def _calculate_metrics(self, total_ops: int, elapsed_time: float, rows=None,
                           failed_ops: Optional[int] = None, support: Optional[dict] = None) -> dict:
        """Calculate throughput and latency metrics.

        Note: total_ops is the intended count (num_ops * num_threads).
        ``rows`` (op_type, status, latency) default to this process's
        collector; the multi-process path passes the merged rows.
        """
        # One verb row (BRANCH, DELETE, ...) or one EXEC row per operation;
        # the statement rows inside an exec are its breakdown.
        if rows is None:
            rows = self.result_collector.results
        ok_rows = [r for r in rows if r.status == rslt.OpStatus.OK]
        actual_ops = sum(1 for r in ok_rows if r.op_type in rc.VERB_OP_TYPES)
        if failed_ops is None:
            failed_ops = len(self.result_collector.failed_operations)
        throughput = actual_ops / elapsed_time if elapsed_time > 0 else 0

        # Data operation throughput: the SQL statements themselves
        data_ops = [r for r in ok_rows if r.op_type in rc.STATEMENT_OP_TYPES]
        data_ops_count = len(data_ops)
        data_ops_time = sum(r.latency for r in data_ops) if data_ops else 0
        data_throughput = data_ops_count / data_ops_time if data_ops_time > 0 else 0

        return {
            "intended_ops": total_ops,  # Intended number of operations
            "total_ops": actual_ops,  # Successful verb/exec operations
            "failed_ops": failed_ops,  # Operations that raised in the runner
            "elapsed_time": elapsed_time,
            "throughput": throughput,  # Based on actual successful ops
            "data_ops_count": data_ops_count,
            "data_ops_time": data_ops_time,
            "data_throughput": data_throughput,
            "loaded_branches": self.loaded_branches(),
            "latency": {
                "ops": _latency_quantiles([r for r in ok_rows if r.op_type in rc.VERB_OP_TYPES]),
                "statements": _latency_quantiles(data_ops),
            },
            "support": support if support is not None else self.result_collector.support_summary(),
        }

    def _print_aggregate_summary(self, thread_stats: list) -> None:
        """Print aggregate summary of async execution across all threads.

        Args:
            thread_stats: List of dicts with per-thread statistics
        """
        if not thread_stats:
            return

        # Aggregate stats across all threads
        total_ops = sum(s['num_ops'] for s in thread_stats)
        total_successes = sum(s['successes'] for s in thread_stats)
        total_failures = sum(s['failures'] for s in thread_stats)
        total_exceptions = sum(s['exceptions'] for s in thread_stats)
        total_nones = sum(s['nones'] for s in thread_stats)
        threads_with_issues = sum(1 for s in thread_stats if s['has_issues'])

        print(f"\n{'='*80}")
        print(f"Async Execution Summary ({len(thread_stats)} thread(s)):")
        print(f"  Total operations: {total_ops:,}")
        print(f"  Successes: {total_successes:,} ({100*total_successes/total_ops:.2f}%)" if total_ops > 0 else "  Successes: 0")

        if total_failures > 0:
            print(f"  Failures (caught): {total_failures:,} ({100*total_failures/total_ops:.2f}%)")

        if total_exceptions > 0:
            print(f"  Exceptions (uncaught): {total_exceptions:,} ({100*total_exceptions/total_ops:.2f}%)")

        if total_nones > 0:
            print(f"  None values: {total_nones:,}")

        if threads_with_issues > 0:
            print(f"  Threads with issues: {threads_with_issues}/{len(thread_stats)}")

        print(f"{'='*80}\n")

    def _print_metrics(self, metrics: dict) -> None:
        """Print benchmark metrics."""
        print(f"\n{'='*60}")
        print(f"Benchmark Results:")
        print(f"  Intended operations: {metrics['intended_ops']}")
        print(f"  Successful operations: {metrics['total_ops']}")
        print(f"  Failed operations: {metrics['failed_ops']}", end="")
        if metrics['failed_ops'] > 0 and metrics['intended_ops'] > 0:
            print(f" ({100*metrics['failed_ops']/metrics['intended_ops']:.1f}%)")
        else:
            print()
        print(f"  Timed window: {metrics['elapsed_time']:.2f}s (all workers started together)")
        print(f"  Wall time incl. worker setup: {metrics['wall_time']:.2f}s")
        print(f"  Throughput: {metrics['throughput']:.2f} ops/sec (successful ops only)")
        print(f"\n  SQL statements only (excluding branch ops):")
        print(f"    Count: {metrics['data_ops_count']}")
        print(f"    Time: {metrics['data_ops_time']:.2f}s")
        print(f"    Throughput: {metrics['data_throughput']:.2f} ops/sec")
        support = metrics.get("support", {})
        if support and not support.get("workflow_supported", True):
            print(f"  UNSUPPORTED on this backend: {support['unsupported_ops']}")
        print(f"{'='*60}\n")


# ============================================================================
# Result Management
# ============================================================================


class ResultManager:
    """Manages result writing to parquet and JSON."""

    def __init__(
        self,
        config: BenchmarkConfig,
        result_collector: rc.ResultCollector,
        metrics: dict,
        output_dir: str,
        data_source: Optional[dict] = None,
    ):
        self.config = config
        self.result_collector = result_collector
        self.metrics = metrics
        self.output_dir = output_dir
        self.data_source = data_source or {"source": config.source or "none"}

    def write_results(self) -> None:
        """Write all results to files."""
        # Write main parquet file (the multi-process path has already merged
        # its client processes' rows into it)
        if not self.metrics.get("rows_merged"):
            self.result_collector.write_to_parquet()

        # Write summary JSON
        self._write_summary_json()

        # Write failure details if any failures occurred
        if self.result_collector.failed_operations:
            self._write_failures_json()

    def _write_summary_json(self) -> None:
        """Write summary JSON with throughput metrics."""
        summary = {
            "run_id": self.config.run_id,
            "backend": self.config.backend_name,
            "operation": self.config.operation_name,
            "num_ops": self.config.num_ops,
            "num_threads": self.config.num_threads,
            "num_branches": self.config.num_branches,
            "execution_mode": "async" if self.config.use_async else "sync",
            "concurrent_requests": self.config.concurrent_requests,
            "scale_factor": self.config.scale_factor,
            "intended_ops": self.metrics["intended_ops"],  # num_ops * num_threads
            "total_ops": self.metrics["total_ops"],  # Actual successful operations
            "failed_ops": self.metrics["failed_ops"],  # Failed operations
            "elapsed_time": self.metrics["elapsed_time"],  # Timed window (start barrier to last worker done)
            "wall_time": self.metrics["wall_time"],  # Incl. worker connect/warm-up/teardown
            "throughput": self.metrics["throughput"],  # Based on successful ops only
            "data_ops_count": self.metrics["data_ops_count"],
            "data_ops_time": self.metrics["data_ops_time"],
            "data_throughput": self.metrics["data_throughput"],
            "loaded_branches": self.metrics.get("loaded_branches"),
            "num_processes": self.metrics.get("num_processes", 1),
            # Average busy cores in the timed window (server: only when co-located)
            "server_cpu_cores": self.metrics.get("server_cpu_cores"),
            "client_cpu_cores": self.metrics.get("client_cpu_cores"),
            # Remote machines (DBFORK_REMOTE_CPU): process and node CPU cores, disk utilisation
            "remote_cpu": self.metrics.get("remote_cpu"),
            # background_load: what the clients keeping every setup branch busy did
            "background": self.metrics.get("background"),
            "latency": self.metrics.get("latency", {}),  # seconds; ops = verbs/EXEC, statements = SQL inside exec
            "data_source": self.data_source,
            "support": self.metrics.get("support", {}),
        }
        # Server behaviour the backend worked around (retries and the like)
        suite_class = {
            tp.Backend.CHRONOS: ChronosToolSuite,
            tp.Backend.TERMINUSDB: TerminusDBToolSuite,
        }.get(self.config.backend)
        observed = getattr(suite_class, "OBSERVED", None)
        if observed:
            summary["backend_observations"] = dict(observed)

        # Build filename
        filename_parts = [
            self.config.run_id,
            self.config.operation_name,
            f"threads{self.config.num_threads}",
        ]
        summary_filename = "_".join(filename_parts) + "_summary.json"
        summary_path = os.path.join(self.output_dir, summary_filename)

        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)

        print(f"Summary written to: {summary_path}")

    def _write_failures_json(self) -> None:
        """Write detailed failure information to a separate JSON file."""
        failures_data = {
            "run_id": self.config.run_id,
            "backend": self.config.backend_name,
            "operation": self.config.operation_name,
            "total_failures": len(self.result_collector.failed_operations),
            "failures": self.result_collector.failed_operations,
        }

        # Build filename
        filename_parts = [
            self.config.run_id,
            self.config.operation_name,
            f"threads{self.config.num_threads}",
        ]
        failures_filename = "_".join(filename_parts) + "_failures.json"
        failures_path = os.path.join(self.output_dir, failures_filename)

        with open(failures_path, "w") as f:
            json.dump(failures_data, f, indent=2)

        print(f"Failure details written to: {failures_path}")


# ============================================================================
# Operation Registration
# ============================================================================


def register_all_operations() -> None:
    """Register all available operations with the registry."""
    # CRUD operations
    OperationRegistry.register(tp.OperationType.READ, ReadOperation)
    OperationRegistry.register(tp.OperationType.INSERT, InsertOperation)
    OperationRegistry.register(tp.OperationType.UPDATE, UpdateOperation)
    OperationRegistry.register(tp.OperationType.DELETE, DeleteOperation)
    OperationRegistry.register(tp.OperationType.RANGE_READ, RangeReadOperation)
    OperationRegistry.register(tp.OperationType.RANGE_UPDATE, RangeUpdateOperation)
    OperationRegistry.register(tp.OperationType.MIXED, MixedOperation)

    # Branch operations
    OperationRegistry.register(tp.OperationType.BRANCH_CREATE, BranchCreateOperation)
    OperationRegistry.register(tp.OperationType.BRANCH_CONNECT, BranchConnectOperation)
    OperationRegistry.register(tp.OperationType.BRANCH_DELETE, BranchDeleteOperation)
    OperationRegistry.register(tp.OperationType.BRANCH_COMMIT, BranchCommitOperation)
    OperationRegistry.register(tp.OperationType.BRANCH_DIFF, BranchDiffOperation)
    OperationRegistry.register(tp.OperationType.BRANCH_LOG, BranchLogOperation)
    OperationRegistry.register(tp.OperationType.BRANCH_MERGE, BranchMergeOperation)
    OperationRegistry.register(tp.OperationType.BRANCH_REBASE, BranchRebaseOperation)
    OperationRegistry.register(tp.OperationType.BRANCH_REVERT, BranchRevertOperation)
    OperationRegistry.register(tp.OperationType.BRANCH_RESET, BranchResetOperation)

    # DDL operations
    OperationRegistry.register(tp.OperationType.DDL_ADD_INDEX, AddIndexOperation)
    OperationRegistry.register(tp.OperationType.DDL_REMOVE_INDEX, RemoveIndexOperation)
    OperationRegistry.register(tp.OperationType.DDL_VACUUM, VacuumOperation)
    OperationRegistry.register(tp.OperationType.DDL_ADD_COLUMN, AddColumnOperation)
    OperationRegistry.register(tp.OperationType.DDL_REMOVE_COLUMN, RemoveColumnOperation)
    OperationRegistry.register(tp.OperationType.DDL_BACKFILL, BackfillOperation)
    OperationRegistry.register(tp.OperationType.DDL_ADD_COLUMN_WITH_DEFAULT,AddColumnWithDefaultOperation,)
    OperationRegistry.register(tp.OperationType.DDL_TYPE_CHANGE, TypeChangeOperation)
    OperationRegistry.register(tp.OperationType.DDL_RENAME_COLUMN, RenameColumnOperation)
    OperationRegistry.register(tp.OperationType.DDL_RENAME_TABLE, RenameTableOperation)
    OperationRegistry.register(tp.OperationType.DDL_ADD_FK, AddForeignKeyOperation)
    OperationRegistry.register(tp.OperationType.DDL_ADD_CHECK, AddCheckOperation)
    OperationRegistry.register(tp.OperationType.DDL_SET_DEFAULT, SetDefaultOperation)
    OperationRegistry.register(tp.OperationType.DDL_SET_NOT_NULL, SetNotNullOperation)
    OperationRegistry.register(tp.OperationType.DDL_EXPAND_CONTRACT, ExpandContractOperation)


# ============================================================================
# Main Orchestration
# ============================================================================


def main():
    """Main entry point for runner2."""
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Run database microbenchmarks from task2 config."
    )
    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to task2 configuration file (.textproto)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/tmp/run_stats",
        help="Directory for output files (default: /tmp/run_stats)",
    )
    args = parser.parse_args()

    # Register all operations
    register_all_operations()

    # Load configuration
    try:
        config = BenchmarkConfig.load_from_file(args.config)
        print(f"Loaded configuration: {config}")
    except FileNotFoundError:
        print(f"Error: Config file not found: {args.config}")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading config: {e}")
        sys.exit(1)

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    # Create shared result collector
    result_collector = rc.ResultCollector(
        run_id=config.run_id, output_dir=args.output_dir
    )

    try:
        # 1. Setup backend
        backend_mgr = BackendManager(config, args.output_dir)
        backend_info = backend_mgr.setup()
        data_source = None
        if config.source == "generated":
            data_source = seed_generated_database(
                config.backend, backend_info, config.db_name, config.generated,
                config.scale_factor,
            )

        # 2. Setup phase (UNTIMED)
        if config.num_branches > 0:
            setup_phase = SetupPhase(config, backend_info, args.output_dir)
            setup_branches = setup_phase.execute()
        else:
            setup_branches = []
            backend_info.setup_branches = []

        # 3. Execute benchmark (TIMED)
        executor = BenchmarkExecutor(
            config, backend_info, setup_branches, result_collector
        )
        metrics = executor.execute()

        # 4. Write results
        result_mgr = ResultManager(config, result_collector, metrics, args.output_dir,
                                   data_source=data_source)
        result_mgr.write_results()

    finally:
        # 5. Cleanup
        if backend_mgr:
            backend_mgr.cleanup()

    print("\nBenchmark complete!")


if __name__ == "__main__":
    main()
