"""Actual local verification. This does NOT emulate Trino or Docker services."""
import argparse
import csv
import hashlib
import json
import platform
import random
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
TABLES = ['customer', 'orders', 'lineitem']


def run(sf=1, repeats=5):
    out = ROOT / 'results' / 'local'
    data = ROOT / 'data' / 'local'
    out.mkdir(parents=True, exist_ok=True)
    data.mkdir(parents=True, exist_ok=True)
    c = duckdb.connect()
    c.execute("SET threads=4; SET memory_limit='2GB'")
    c.execute('INSTALL tpch; LOAD tpch')
    c.execute(f'CALL dbgen(sf={sf})')
    for name in TABLES:
        c.execute(f'CREATE VIEW raw_{name} AS SELECT ' + ', '.join(
            f'{col[0]} AS {col[0][2:]}' for col in c.execute(f'DESCRIBE {name}').fetchall()
        ) + f' FROM {name}')
    # Reuse the assignment SQL unchanged except for the namespace.
    q = (ROOT / 'sql/q18.sql').read_text(encoding='utf-8')
    for name in TABLES:
        q = q.replace('{schema}.' + name, 'raw_' + name)
    expected = c.execute(q).fetchall()
    assert 0 < len(expected) <= 100, 'Q18 returns up to 100 qualifying orders'
    # Independent one-pass aggregate, unlike the nested reference query.
    independent = c.execute('''SELECT c.name,c.custkey,o.orderkey,o.orderdate,
        o.totalprice,x.total_quantity FROM
        (SELECT orderkey,sum(quantity) total_quantity FROM raw_lineitem
         GROUP BY orderkey HAVING sum(quantity)>300) x
        JOIN raw_orders o USING(orderkey) JOIN raw_customer c USING(custkey)
        ORDER BY totalprice DESC, orderdate, orderkey LIMIT 100''').fetchall()
    assert expected == independent
    metrics, trials = [], []
    configs = [('PARQUET','UNCOMPRESSED'),('PARQUET','SNAPPY'),
               ('PARQUET','GZIP'),('PARQUET','ZSTD'),('CSV','NONE')]
    random.Random(18).shuffle(configs)
    for fmt, codec in configs:
        folder = data / f'{fmt.lower()}_{codec.lower()}'
        folder.mkdir(exist_ok=True)
        writes = []
        # Every write trial exports the same complete raw rows, not the projection.
        for trial in range(repeats):
            start = time.perf_counter()
            for name in TABLES:
                path = (folder / f'{name}.{fmt.lower()}').as_posix()
                opts = (f"FORMAT PARQUET, COMPRESSION '{codec}', ROW_GROUP_SIZE 122880"
                        if fmt == 'PARQUET' else 'FORMAT CSV, HEADER true')
                c.execute(f"COPY raw_{name} TO '{path}' ({opts})")
            elapsed = time.perf_counter() - start
            writes.append(elapsed)
            trials.append(dict(format=fmt,codec=codec,phase='write',trial=trial+1,seconds=elapsed))
        for name in TABLES:
            path = (folder / f'{name}.{fmt.lower()}').as_posix()
            if fmt == 'PARQUET':
                reader = f"read_parquet('{path}')"
            else:
                cols = c.execute(f'DESCRIBE raw_{name}').fetchall()
                colspec = ','.join(f"'{r[0]}':'{r[1]}'" for r in cols)
                reader = f"read_csv('{path}', header=true, columns={{{colspec}}})"
            c.execute(f'CREATE OR REPLACE VIEW bench_{name} AS SELECT * FROM {reader}')
        bq = q.replace('raw_', 'bench_')
        assert c.execute(bq).fetchall() == expected  # warmup and correctness
        reads = []
        for trial in range(repeats):
            start = time.perf_counter()
            assert c.execute(bq).fetchall() == expected
            elapsed = time.perf_counter() - start
            reads.append(elapsed)
            trials.append(dict(format=fmt,codec=codec,phase='query',trial=trial+1,seconds=elapsed))
        m = dict(format=fmt, codec=codec, bytes=sum(p.stat().st_size for p in folder.iterdir()),
                 write_median_s=statistics.median(writes),query_median_s=statistics.median(reads),
                 query_min_s=min(reads),query_max_s=max(reads),write_trials_s=writes,query_trials_s=reads)
        metrics.append(m)
        print(fmt,codec,m,flush=True)
    c.execute('CREATE SCHEMA bronze; CREATE SCHEMA silver; CREATE SCHEMA gold')
    layer_stats = []
    def save(layer, name):
        folder = data / layer
        folder.mkdir(exist_ok=True)
        path = (folder / f'{name}.parquet').as_posix()
        c.execute(f"COPY {layer}.{name} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        count = c.execute(f'SELECT count(*) FROM {layer}.{name}').fetchone()[0]
        layer_stats.append(dict(layer=layer,table=name,rows=count,bytes=Path(path).stat().st_size,
                                columns=[r[0] for r in c.execute(f'DESCRIBE {layer}.{name}').fetchall()]))
    for name in TABLES:
        c.execute(f'CREATE TABLE bronze.{name} AS SELECT * FROM raw_{name}')
        save('bronze',name)
    sql = (ROOT / 'sql/silver.sql').read_text(encoding='utf-8')
    sql = sql.replace('lake.', '').replace(" WITH (format='PARQUET', compression_codec='ZSTD')", '')
    c.execute(sql)
    gsql = (ROOT / 'sql/gold.sql').read_text(encoding='utf-8')
    c.execute(gsql.replace('lake.', '').replace(" WITH (format='PARQUET', compression_codec='ZSTD')", ''))
    for name in ['customer','orders','lineitem','large_orders','customer_orders']:
        save('silver',name)
    save('gold','q18_top100')
    actual = c.execute('SELECT * FROM gold.q18_top100 ORDER BY totalprice DESC,orderdate,orderkey').fetchall()
    assert actual == expected
    # Verify uniqueness, no orphan keys, no invalid quantities, and no lost rows.
    checks = {}
    for name, keys in [('customer','custkey'),('orders','orderkey'),('lineitem','orderkey,linenumber')]:
        checks[f'{name}_duplicate_keys'] = c.execute(f'SELECT count(*) FROM (SELECT {keys} FROM bronze.{name} GROUP BY {keys} HAVING count(*)>1)').fetchone()[0]
        checks[f'{name}_rejected_rows'] = c.execute(f'SELECT (SELECT count(*) FROM bronze.{name})-(SELECT count(*) FROM silver.{name})').fetchone()[0]
    checks['orphan_orders'] = c.execute('SELECT count(*) FROM silver.orders o LEFT JOIN silver.customer c USING(custkey) WHERE c.custkey IS NULL').fetchone()[0]
    checks['orphan_lineitems'] = c.execute('SELECT count(*) FROM silver.lineitem l LEFT JOIN silver.orders o USING(orderkey) WHERE o.orderkey IS NULL').fetchone()[0]
    assert all(x == 0 for x in checks.values()), checks
    with (out / 'q18_top100.csv').open('w',newline='',encoding='utf-8-sig') as f:
        w=csv.writer(f); w.writerow(['name','custkey','orderkey','orderdate','totalprice','total_quantity']); w.writerows(actual)
    with (out / 'trials.csv').open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=list(trials[0]));w.writeheader();w.writerows(trials)
    result = dict(engine='DuckDB',version=duckdb.__version__,platform=platform.platform(),
                  python=platform.python_version(),threads=4,memory_limit='2GB',sf=sf,
                  timestamp_utc=datetime.now(timezone.utc).isoformat(),repeats=repeats,
                  method='one warmup then warm filesystem cache; median of full fetch; no cache flush',
                  metrics=metrics,layers=layer_stats,checks=checks,
                  reference_equal=True,independent_query_equal=True,
                  result_sha256=hashlib.sha256((out/'q18_top100.csv').read_bytes()).hexdigest())
    (out/'measurements.json').write_text(json.dumps(result,indent=2,ensure_ascii=False),encoding='utf-8')
    print('PASS: reference SQL = independent SQL = Gold; quality checks passed',flush=True)


if __name__ == '__main__':
    p=argparse.ArgumentParser();p.add_argument('--sf',type=float,default=1);p.add_argument('--repeats',type=int,default=5)
    a=p.parse_args();run(a.sf,a.repeats)
