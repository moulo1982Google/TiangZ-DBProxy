"""按配置记录目录实际分配空间；仅诊断，不调整验收门槛或清理业务数据。"""

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    raw = Path(args.config).read_bytes()
    config = json.loads(raw)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with Path(str(output) + '.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        started = datetime.now(timezone.utc).isoformat()
        groups = {}
        for name, paths in config['groups'].items():
            rows = []
            for path in paths:
                if not Path(path).is_absolute():
                    raise ValueError('Sampling paths must be absolute')
                result = subprocess.run(
                    ['/usr/bin/du', '-x', '-s', '-B1', '--', path],
                    capture_output=True, text=True, timeout=60, check=False,
                )
                if result.returncode:
                    rows.append({'path': path, 'error': result.stderr.strip()[:300]})
                else:
                    rows.append({'path': path, 'allocatedBytes': int(result.stdout.split()[0])})
            groups[name] = rows
        stat = os.statvfs(config.get('filesystem', '/'))
        record = {
            'schemaVersion': 1, 'startedAt': started,
            'finishedAt': datetime.now(timezone.utc).isoformat(),
            'configSha256': hashlib.sha256(raw).hexdigest(),
            'context': config.get('context', ''), 'groups': groups,
            'filesystem': {'totalBytes': stat.f_blocks * stat.f_frsize,
                           'usedBytes': (stat.f_blocks - stat.f_bfree) * stat.f_frsize,
                           'availableBytes': stat.f_bavail * stat.f_frsize},
        }
        # 最多保留当前文件与两份轮转记录；锁覆盖采样和轮转。
        if output.exists() and output.stat().st_size >= 8 * 1024**2:
            previous = Path(str(output) + '.1')
            if previous.exists():
                os.replace(previous, str(output) + '.2')
            os.replace(output, previous)
        with output.open('a', encoding='utf-8') as stream:
            stream.write(json.dumps(record, ensure_ascii=False) + '\n')
        errors = sum('error' in row for rows in groups.values() for row in rows)
        print(json.dumps({'output': str(output), 'groups': len(groups), 'errors': errors}))
        if errors:
            raise SystemExit(1)


if __name__ == '__main__':
    main()
