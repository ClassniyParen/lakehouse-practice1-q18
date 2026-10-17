"""Idempotent initialization; errors other than verified existing objects stop startup."""
import json
import urllib.error
import urllib.request

BASE = 'http://lakekeeper:8181/management/v1'


def request(path, body=None):
    req=urllib.request.Request(BASE+path, data=None if body is None else json.dumps(body).encode(),
                               headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=60) as r:
        content=r.read()
        return json.loads(content) if content else None


try:
    request('/bootstrap', {'accept-terms-of-use':True})
except urllib.error.HTTPError as e:
    if e.code not in (400,409):
        raise
    # Only continue if management API is actually usable.
    request('/warehouse?project-id=00000000-0000-0000-0000-000000000000')

warehouses=request('/warehouse?project-id=00000000-0000-0000-0000-000000000000')
if not any(w['warehouse-name']=='practice1' for w in warehouses.get('warehouses',[])):
    request('/warehouse',{
        'warehouse-name':'practice1','project-id':'00000000-0000-0000-0000-000000000000',
        'storage-profile':{'type':'s3','bucket':'lakehouse','key-prefix':'practice1',
                           'endpoint':'http://minio:9000','region':'local-01',
                           'path-style-access':True,'flavor':'minio','sts-enabled':False},
        'storage-credential':{'type':'s3','credential-type':'access-key',
                              'aws-access-key-id':'minio-root-user',
                              'aws-secret-access-key':'minio-root-password'}})
print('Warehouse practice1 is available')
