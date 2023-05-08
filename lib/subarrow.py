"""
Utility functions for pyarrow run in subprocess
"""
import sys
import subprocess
import time

import pyarrow.parquet as pq
import pyarrow as pa

from pathlib import Path

_FILE_IN = 'tmp_agg_in.parquet'
_FILE_OUT = 'tmp_agg_out.parquet'


class _ArrowAggregateArguments:

    def __init__(self, by: list[str], agg: list[tuple], tempdir: Path):
        self.by = by
        self.agg = agg
        self.tempdir = Path(tempdir)

    def to_args(self) -> list[str]:
        return [",".join(self.by), ",".join([f"{a[0]}|{a[1]}" for a in self.agg]), str(self.tempdir.absolute())]

    @staticmethod
    def from_args(args: list[str]):
        _by = args[0].split(',')
        _agg_tup = args[1].split(",")
        _agg = [tuple(_aggt.split("|")) for _aggt in _agg_tup]
        _td = args[2]

        return _ArrowAggregateArguments(_by, _agg, Path(_td))


class ArrowAggregate:

    def __init__(self, table: pa.Table, by: list[str], agg: list[tuple], tempdir='.', venv=r"..\venv\Scripts\python.exe"):
        self._table = table
        self._args = _ArrowAggregateArguments(by, agg, Path(tempdir))
        self._venv = venv

    def _tempfile_in(self) -> Path:
        return self._args.tempdir / _FILE_IN

    def _tempfile_out(self) -> Path:
        return self._args.tempdir / _FILE_OUT

    def _store(self):
        pq.write_table(self._table, self._tempfile_in())

    def _restore(self) -> pa.Table:
        _result = pq.read_table(self._tempfile_out())
        while True:
            try:
                self._tempfile_out().unlink()
                break
            except PermissionError:
                time.sleep(0.001)
                continue
        return _result

    def _exec(self):
        res = subprocess.run([self._venv, __file__] + self._args.to_args())
        if res.returncode != 0:
            raise ValueError('The process failed')

    def aggregate(self) -> pa.Table:
        self._store()
        self._exec()
        self._tempfile_in().unlink()
        return self._restore()


if __name__ == '__main__':

    if len(sys.argv) < 4:
        print('Incorrect number of parameters')
        exit(1)

    _arguments = _ArrowAggregateArguments.from_args(sys.argv[1:])

    _table = pq.read_table(_arguments.tempdir / _FILE_IN)
    _aggregated = _table.group_by(_arguments.by).aggregate(_arguments.agg)
    pq.write_table(_aggregated, _arguments.tempdir / _FILE_OUT)

    exit(0)
