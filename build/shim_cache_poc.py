# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Isolated measurement of cross-run artifact reuse; not a production cache."""

import argparse
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import tempfile
import time
from urllib.parse import urlencode
import zipfile

import check_shim_compatibility as checker


ROOT = Path(__file__).resolve().parents[1]
FILES = ('build/check_shim_compatibility.py', 'build/get_buildvers.py',
         'build/shim_cache_poc.py', '.github/workflows/shim-cache-poc.yml')
CASES = [
    dict(issue='16404', base='5621e7dc8f0c752fdfaacc8df9181c0594b85a68',
         a='1715951d449dcad86e9fad1f3f2cd0508a3b9459',
         b='e880e2f92933975d38c32a587ffa9e994157d639',
         profile='410db183', symbol='ParquetMissingStructShims'),
    dict(issue='15390', base='4383de9cf1e20b37f27f909b5ce1b3a3b95eb9d8',
         a='a278c97965ba93333ae6aa3a2a0d39638babc6f4',
         b='2a33816b2ccf96725b7504743bb8e4c8b9d337b1',
         profile='359', symbol='CreateNamedStructShims'),
    dict(issue='14371', base='8b6e24c76275b9c8d8b92146e2a7bcaca6b159c9',
         a='943f931e9d93ab18772978d240fe72b18c96f005',
         b='7fca9197621dd482fcfc56455082664b815605eb',
         profile='358', symbol='GpuParquetUtilsShims'),
]
OPERATIONS = []


class CacheMiss(Exception):
    pass


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def code_digest(revision=None):
    files = {}
    for name in FILES:
        data = (subprocess.check_output(['git', 'show', revision + ':' + name], cwd=ROOT)
                if revision else (ROOT / name).read_bytes())
        files[name] = hashlib.sha256(data).hexdigest()
    return digest(files)


def context(case):
    return dict(schema=1, repository='NVIDIA/cudf-spark', base=case['base'],
                checker=code_digest(), git=checker.git(ROOT, '--version'),
                python=platform.python_version(), scope='sql-plugin/all-supported',
                merge_order='ascending-head-sha-v1')


def key(case, ctx):
    return digest(dict(context=ctx, heads=sorted((case['a'], case['b']))))


def artifact_name(producer, ctx):
    return 'shim-cache-poc-v1-' + producer + '-' + digest(ctx)


def prepare_objects():
    for case in CASES:
        for sha in (case['base'], case['a'], case['b']):
            checker.fetch_peer(ROOT, 'NVIDIA/cudf-spark', sha)


def compute(case, current=None):
    low, high = sorted((case['a'], case['b']))
    current = current or low
    peer = high if current == low else low
    with tempfile.TemporaryDirectory(prefix='shim-cache-poc-') as directory:
        temporary, cache = Path(directory), {}
        start = time.monotonic()
        tree = checker.merge_snapshot(ROOT, case['base'], current)
        commit = checker.git(ROOT, '-c', 'commit.gpgsign=false', '-c', 'user.name=Shim cache PoC',
                             '-c', 'user.email=shim-cache-poc@example.invalid', 'commit-tree', tree,
                             '-p', case['base'], '-p', current, '-m', 'Temporary cache measurement')
        coverage = checker.tree_coverage(ROOT, tree, temporary, cache)
        setup = time.monotonic() - start
        start = time.monotonic()
        result = checker.peer_combination(ROOT, 'NVIDIA/cudf-spark', case['base'], commit,
                                          coverage, {'head': peer}, temporary, cache)
        elapsed = time.monotonic() - start
    assert result['status'] == 'NEW_INTERACTION_FINDINGS', result
    assert any(f['buildver'] == case['profile'] and case['symbol'] in f.get('symbol', '')
               for f in result['new_findings']), result
    record = dict(status=result['status'], joint_tree=result['joint_tree'],
                  buildvers=result['buildvers'], new_findings=result['new_findings'],
                  by_head={current: result['current_findings'], peer: result['peer_findings']})
    return record, dict(setup_seconds=setup, peer_seconds=elapsed, full_seconds=setup + elapsed)


def select(package, case, ctx, source_run, source_head):
    if (package.get('source_run') != source_run or package.get('source_head') != source_head
            or package.get('context') != ctx or package.get('heads') != sorted((case['a'], case['b']))
            or package.get('pair_key') != key(case, ctx)):
        raise CacheMiss('Source or immutable inputs differ')
    record = package.get('result', {})
    if record.get('status') not in ('NEW_INTERACTION_FINDINGS', 'NO_NEW_INTERACTION_FINDINGS'):
        raise CacheMiss('Incomplete result')
    if set(record.get('by_head', {})) != {case['a'], case['b']}:
        raise CacheMiss('Missing individual baselines')
    return record


def verify_run(run, repo, producer):
    if (run.get('repository', {}).get('full_name') != repo or run.get('head_sha') != producer
            or run.get('path') != '.github/workflows/shim-cache-poc.yml'
            or run.get('event') != 'push' or run.get('status') != 'completed'
            or run.get('conclusion') != 'success'):
        raise CacheMiss('Unexpected producer workflow/run')


def api(path, binary=False):
    start = time.monotonic()
    result = subprocess.run(['gh', 'api', '-H', 'Cache-Control: no-cache', path], cwd=ROOT,
                            capture_output=True, timeout=20)
    OPERATIONS.append(dict(path=path, elapsed_seconds=time.monotonic() - start,
                           ok=result.returncode == 0, bytes=len(result.stdout)))
    if result.returncode:
        # Do not print authentication diagnostics or redirect URLs.
        raise CacheMiss('GitHub API request failed')
    return result.stdout if binary else json.loads(result.stdout)


def download(case, ctx, repo, producer):
    name = artifact_name(producer, ctx)
    listing = api('repos/' + repo + '/actions/artifacts?' + urlencode(dict(name=name, per_page=3)))
    candidates = [a for a in listing['artifacts'] if not a['expired']
                  and a['workflow_run']['head_sha'] == producer]
    if not candidates:
        raise CacheMiss('No exact artifact')
    artifact = candidates[0]
    run_id = artifact['workflow_run']['id']
    verify_run(api(f'repos/{repo}/actions/runs/{run_id}'), repo, producer)
    if code_digest(producer) != code_digest():
        raise CacheMiss('Producer executed a different checker/workflow')
    data = api(f"repos/{repo}/actions/artifacts/{artifact['id']}/zip", binary=True)
    expected = artifact.get('digest', '')
    if expected != 'sha256:' + hashlib.sha256(data).hexdigest():
        raise CacheMiss('Artifact digest mismatch')
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        entry = archive.getinfo('pair-cache.json')
        if entry.file_size > 1024 * 1024:
            raise CacheMiss('Cache payload exceeds 1 MiB')
        package = json.loads(archive.read(entry))
    record = select(package, case, ctx, run_id, producer)
    return record, dict(artifact_id=artifact['id'], size_in_bytes=artifact['size_in_bytes'],
                        source_run=run_id, source_head=producer)


def invalidation_checks(package, case, ctx, source_run, source_head):
    rejected = []
    mutations = ('base', 'checker', 'git', 'python', 'scope', 'schema', 'head',
                 'source_run', 'source_head', 'incomplete', 'missing_baseline')
    for field in mutations:
        data = copy.deepcopy(package)
        if field in ('base', 'checker', 'git', 'python', 'scope', 'schema'):
            data['context'][field] = 'different'
        elif field == 'head':
            data['heads'][0] = '0' * 40
        elif field in ('source_run', 'source_head'):
            data[field] = 'different'
        elif field == 'incomplete':
            data['result']['status'] = 'INCOMPLETE'
        else:
            data['result']['by_head'].pop(case['a'])
        try:
            select(data, case, ctx, source_run, source_head)
        except CacheMiss:
            rejected.append(field)
        else:
            raise AssertionError('Unsafe cache accepted: ' + field)
    return rejected


def run(mode, output, repetitions):
    output.mkdir(parents=True, exist_ok=True)
    config = json.loads((ROOT / 'build/shim_cache_poc_run.json').read_text())
    source_head = os.environ.get('GITHUB_SHA') or checker.git(ROOT, 'rev-parse', 'HEAD')
    source_run = int(os.environ.get('GITHUB_RUN_ID', '0'))
    source_repo = os.environ.get('GITHUB_REPOSITORY', 'wjxiz1992/spark-rapids')
    report = dict(mode=mode, source_head=source_head, source_run=source_run,
                  checker=code_digest(), git=checker.git(ROOT, '--version'),
                  python=platform.python_version(), cases=[], operations=OPERATIONS,
                  boundary='Controlled fork push workflows; no upstream PR event or untrusted producer tested')
    prepare_objects()
    for case in CASES:
        ctx = context(case)
        baseline = []
        fresh = None
        for _ in range(repetitions):
            result, timing = compute(case)
            assert fresh is None or result == fresh
            fresh = result
            baseline.append(timing)
        package = dict(context=ctx, heads=sorted((case['a'], case['b'])), pair_key=key(case, ctx),
                       source_run=source_run, source_head=source_head, result=fresh)
        case_report = dict(issue=case['issue'], context=ctx, baseline=baseline,
                           invalidations=invalidation_checks(package, case, ctx, source_run, source_head))
        reverse, reverse_timing = compute(case, max(case['a'], case['b']))
        assert fresh == reverse, (case['issue'], fresh, reverse)
        case_report['reverse_equal'] = True
        case_report['reverse_timing'] = reverse_timing
        if mode in ('producer', 'local'):
            destination = output / 'cache' / case['issue']
            destination.mkdir(parents=True, exist_ok=True)
            (destination / 'pair-cache.json').write_text(json.dumps(package, sort_keys=True) + '\n')
            case_report['json_bytes'] = (destination / 'pair-cache.json').stat().st_size
            if os.environ.get('GITHUB_OUTPUT'):
                with open(os.environ['GITHUB_OUTPUT'], 'a') as stream:
                    stream.write('artifact_' + case['issue'] + '=' + artifact_name(source_head, ctx) + '\n')
        if mode == 'consumer':
            producer, repo = config['producer_head'], config['producer_repo']
            hits = []
            for _ in range(repetitions):
                start = time.monotonic()
                cached, identity = download(case, ctx, repo, producer)
                elapsed = time.monotonic() - start
                assert cached == fresh
                hits.append(dict(seconds=elapsed, **identity))
            case_report['cold_hits'] = hits
            # An absent namespace models a first run: one lookup, then real recomputation.
            start = time.monotonic()
            missing_name = artifact_name(producer, ctx) + '-deliberate-miss'
            absent = api('repos/' + repo + '/actions/artifacts?' + urlencode(dict(name=missing_name, per_page=3)))
            assert not absent['artifacts']
            lookup_seconds = time.monotonic() - start
            fallback, timing = compute(case)
            assert fallback == fresh
            case_report['miss'] = dict(lookup_seconds=lookup_seconds, compute=timing,
                                       total_peer_seconds=lookup_seconds + timing['peer_seconds'],
                                       total_full_seconds=lookup_seconds + timing['full_seconds'])
        report['cases'].append(case_report)
        (output / 'measurement.json').write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(dict(issue=case['issue'], direct_peer_median=statistics.median(
            t['peer_seconds'] for t in baseline), direct_full_median=statistics.median(
            t['full_seconds'] for t in baseline), reverse_equal=True,
            cache_hit_median=statistics.median(h['seconds'] for h in case_report['cold_hits'])
            if 'cold_hits' in case_report else None)), flush=True)
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('local', 'producer', 'consumer'), required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--repetitions', type=int, default=3)
    args = parser.parse_args()
    run(args.mode, args.output_dir, args.repetitions)
