"""Run the actual Iceberg pipeline and benchmark through Trino's HTTP protocol.

Uses only the Python standard library. SQL, query IDs and measured statistics are
saved incrementally. This script never substitutes DuckDB data for Trino results.
"""
import argparse
import csv
import hashlib
import json
import os
import random
import statistics
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'results/trino'
TABLES=['customer','orders','lineitem']
SILVER=['customer','orders','lineitem','large_orders','customer_orders']


class Trino:
    def __init__(self):
        self.base=os.environ.get('TRINO_URL','http://localhost:8080')
        self.headers={'X-Trino-User':'student','X-Trino-Source':'practice1-q18',
                      'Content-Type':'text/plain; charset=utf-8'}
        OUT.mkdir(parents=True,exist_ok=True)

    def query(self,sql):
        start=time.perf_counter()
        request=urllib.request.Request(self.base+'/v1/statement',data=sql.encode(),headers=self.headers)
        rows,columns=[],[]
        deadline=time.monotonic()+1800
        try:
            while True:
                with urllib.request.urlopen(request,timeout=180) as response:
                    result=json.load(response)
                if 'error' in result:
                    raise RuntimeError(json.dumps(result['error'],ensure_ascii=False))
                rows.extend(result.get('data',[]))
                columns=result.get('columns',columns)
                if not result.get('nextUri'):
                    break
                if time.monotonic()>deadline:
                    raise TimeoutError('Trino query exceeded 30 minutes')
                request=urllib.request.Request(result['nextUri'],headers=self.headers)
        except Exception:
            # Cancel an in-flight query rather than leaving work running after failure.
            if 'result' in locals() and result.get('nextUri'):
                try:
                    urllib.request.urlopen(urllib.request.Request(result['nextUri'],method='DELETE',headers=self.headers),timeout=10).close()
                except Exception:
                    pass
            raise
        metadata=dict(query_id=result['id'],client_seconds=time.perf_counter()-start,
                      stats=result.get('stats',{}),rows=len(rows),sql=sql)
        with (OUT/'queries.jsonl').open('a',encoding='utf-8') as f:
            f.write(json.dumps(metadata,ensure_ascii=False)+'\n')
        return rows,metadata,[x['name'] for x in columns]

    def rows(self,sql):
        return self.query(sql)[0]

    def execute_file(self,name):
        for stmt in (ROOT/'sql'/name).read_text(encoding='utf-8').split(';'):
            if stmt.strip():
                self.query(stmt.strip())


def dump(name,value):
    (OUT/name).write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8')


def write_csv(name,columns,rows):
    with (OUT/name).open('w',newline='',encoding='utf-8-sig') as f:
        writer=csv.writer(f);writer.writerow(columns);writer.writerows(rows)


def main(sf,repeats):
    t=Trino()
    (OUT/'SUCCESS').unlink(missing_ok=True)
    (OUT/'queries.jsonl').write_text('',encoding='utf-8')
    source=f'tpch.sf{sf}'
    reference=(ROOT/'sql/q18.sql').read_text(encoding='utf-8')
    expected,_,columns=t.query(reference.format(schema=source))
    assert expected, 'No qualifying Q18 orders'
    dump('environment.json',dict(sf=sf,repeats=repeats,engine='Trino',
         version=t.rows('SELECT version()')[0][0],timestamp_utc=datetime.now(timezone.utc).isoformat(),
         run_context=os.environ.get('RUN_CONTEXT','local Docker host'),
         nodes=t.rows('SELECT * FROM system.runtime.nodes')))
    for schema in ['bronze','silver','gold','evolution_demo']:
        t.query(f'CREATE SCHEMA IF NOT EXISTS lake.{schema}')
    # Rebuild only this exercise's named tables. Volumes and other schemas persist.
    for schema,names in [('gold',['q18_top100']),('silver',list(reversed(SILVER))),('bronze',TABLES),('evolution_demo',['orders_demo'])]:
        for name in names:
            t.query(f'DROP TABLE IF EXISTS lake.{schema}.{name}')
    for name in TABLES:
        t.query(f"CREATE TABLE lake.bronze.{name} WITH (format='PARQUET',compression_codec='ZSTD') AS SELECT * FROM {source}.{name}")
    checks={}
    for name,keys in [('customer','custkey'),('orders','orderkey'),('lineitem','orderkey,linenumber')]:
        checks[f'{name}_duplicate_keys']=t.rows(f'SELECT count(*) FROM (SELECT {keys} FROM lake.bronze.{name} GROUP BY {keys} HAVING count(*)>1)')[0][0]
    assert all(v==0 for v in checks.values()),checks
    t.execute_file('silver.sql');t.execute_file('gold.sql')
    for name in TABLES:
        checks[f'{name}_rejected_rows']=t.rows(f'SELECT (SELECT count(*) FROM lake.bronze.{name})-(SELECT count(*) FROM lake.silver.{name})')[0][0]
    checks['orphan_orders']=t.rows('SELECT count(*) FROM lake.silver.orders o LEFT JOIN lake.silver.customer c ON o.custkey=c.custkey WHERE c.custkey IS NULL')[0][0]
    checks['orphan_lineitems']=t.rows('SELECT count(*) FROM lake.silver.lineitem l LEFT JOIN lake.silver.orders o ON l.orderkey=o.orderkey WHERE o.orderkey IS NULL')[0][0]
    assert all(v==0 for v in checks.values()),checks
    gold=t.rows('SELECT * FROM lake.gold.q18_top100 ORDER BY totalprice DESC,orderdate,orderkey')
    bronze=t.rows(reference.format(schema='lake.bronze'))
    assert gold==expected==bronze,'Gold differs from reference TPC-H or Bronze'
    checks['reference_equals_bronze_equals_gold']=True
    dump('checks.json',checks);write_csv('q18_top100.csv',columns,gold)
    layers=[]
    for schema,names in [('bronze',TABLES),('silver',SILVER),('gold',['q18_top100'])]:
        for name in names:
            rows=t.rows(f'SELECT count(*) FROM lake.{schema}.{name}')[0][0]
            # Physical current data files. SHOW STATS data_size is not object size.
            files=t.rows(f'SELECT file_path,file_size_in_bytes FROM lake.{schema}."{name}$files" WHERE content=0')
            cols=[r[0] for r in t.rows(f'DESCRIBE lake.{schema}.{name}')]
            layers.append(dict(layer=schema,table=name,rows=rows,bytes=sum(r[1] for r in files),files=files,columns=cols))
    dump('layers.json',layers)
    configs=[('PARQUET','NONE'),('PARQUET','SNAPPY'),('PARQUET','GZIP'),('PARQUET','ZSTD'),('ORC','GZIP'),('ORC','ZSTD')]
    random.Random(18).shuffle(configs)
    benchmark=[]
    for fmt,codec in configs:
        schema=f'bench_{fmt.lower()}_{codec.lower()}'
        t.query(f'CREATE SCHEMA IF NOT EXISTS lake.{schema}')
        writes=[]
        for iteration in range(repeats):
            times=[]
            for name in TABLES:
                t.query(f'DROP TABLE IF EXISTS lake.{schema}.{name}')
                _,m,_=t.query(f"CREATE TABLE lake.{schema}.{name} WITH (format='{fmt}',compression_codec='{codec}') AS SELECT * FROM {source}.{name}")
                times.append(m)
            writes.append(dict(trial=iteration+1,seconds=sum(x['client_seconds'] for x in times),queries=times))
        query=reference.format(schema=f'lake.{schema}')
        assert t.rows(query)==expected  # warmup excluded
        reads=[]
        for iteration in range(repeats):
            result,m,_=t.query(query)
            assert result==expected
            reads.append(dict(trial=iteration+1,seconds=m['client_seconds'],query_id=m['query_id'],stats=m['stats']))
        size=0;file_count=0
        for name in TABLES:
            files=t.rows(f'SELECT file_size_in_bytes FROM lake.{schema}."{name}$files" WHERE content=0')
            size+=sum(r[0] for r in files);file_count+=len(files)
        benchmark.append(dict(format=fmt,codec=codec,bytes=size,file_count=file_count,
             write_median_s=statistics.median(r['seconds'] for r in writes),
             query_median_s=statistics.median(r['seconds'] for r in reads),
             query_min_s=min(r['seconds'] for r in reads),query_max_s=max(r['seconds'] for r in reads),
             writes=writes,reads=reads))
        dump('benchmark.json',benchmark)
        print('Measured',fmt,codec,flush=True)
    # Isolated table for a real schema evolution and rollback demonstration.
    table='lake.evolution_demo.orders_demo'
    t.query(f"CREATE TABLE {table} WITH (format='PARQUET',compression_codec='ZSTD') AS SELECT * FROM lake.silver.customer_orders")
    before_files=t.rows('SELECT file_path FROM lake.evolution_demo."orders_demo$files" ORDER BY file_path')
    snapshot=t.rows('SELECT snapshot_id FROM lake.evolution_demo."orders_demo$snapshots" ORDER BY committed_at DESC LIMIT 1')[0][0]
    before=t.rows(f'SELECT name,custkey,orderkey,orderdate,totalprice,total_quantity FROM {table} ORDER BY orderkey')
    t.query(f'ALTER TABLE {table} ADD COLUMN audit_note varchar')
    t.query(f'ALTER TABLE {table} RENAME COLUMN audit_note TO processing_note')
    after_files=t.rows('SELECT file_path FROM lake.evolution_demo."orders_demo$files" ORDER BY file_path')
    assert before_files==after_files,'Schema-only edit unexpectedly rewrote files'
    nulls=t.rows(f'SELECT count(*) FROM {table} WHERE processing_note IS NULL')[0][0]
    assert nulls==len(before)
    key=before[0][2]
    t.query(f'DELETE FROM {table} WHERE orderkey={int(key)}')
    current_count=t.rows(f'SELECT count(*) FROM {table}')[0][0]
    assert current_count==len(before)-1
    old_count=t.rows(f'SELECT count(*) FROM {table} FOR VERSION AS OF {int(snapshot)}')[0][0]
    assert old_count==len(before)
    t.query(f'ALTER TABLE {table} EXECUTE rollback_to_snapshot({int(snapshot)})')
    restored=t.rows(f'SELECT name,custkey,orderkey,orderdate,totalprice,total_quantity FROM {table} ORDER BY orderkey')
    assert restored==before
    dump('evolution.json',dict(snapshot_id=snapshot,rows_before=len(before),rows_after_delete=current_count,
          time_travel_count=old_count,rows_after_rollback=len(restored),data_files_unchanged_on_schema_edit=True,
          added_column_existing_rows_null=True,restored_equal=True,columns=t.rows(f'DESCRIBE {table}')))
    (OUT/'SUCCESS').write_text('All actual Trino checks passed\n',encoding='utf-8')
    print('PASS: pipeline, benchmark, schema evolution, time travel, rollback',flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--sf',type=int,default=1);p.add_argument('--repeats',type=int,default=5)
    a=p.parse_args()
    if a.sf<1 or a.repeats<1:
        p.error('sf and repeats must be positive integers')
    main(a.sf,a.repeats)
