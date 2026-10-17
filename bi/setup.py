"""Register Bronze and Gold datasets and record a real Bronze query from BI container."""
import json
from pathlib import Path
from sqlalchemy import create_engine, text
from superset.app import create_app

app=create_app()
with app.app_context():
    from superset import db, security_manager
    from superset.models.core import Database
    from superset.connectors.sqla.models import SqlaTable
    if security_manager.find_user(username='student') is None:
        security_manager.add_user('student','Gleb','Tarasov','student@example.invalid',
                                  security_manager.find_role('Admin'),password='Practice1-local!')
    database=db.session.query(Database).filter_by(database_name='Lakehouse').one_or_none()
    if database is None:
        database=Database(database_name='Lakehouse',sqlalchemy_uri='trino://student@trino:8080/lake/bronze',expose_in_sqllab=True)
        db.session.add(database);db.session.commit()
    for schema,table in [('bronze','customer'),('bronze','orders'),('bronze','lineitem'),('gold','q18_top100')]:
        dataset=db.session.query(SqlaTable).filter_by(database_id=database.id,schema=schema,table_name=table).one_or_none()
        if dataset is None:
            dataset=SqlaTable(database=database,schema=schema,table_name=table)
            db.session.add(dataset);db.session.commit()
        dataset.fetch_metadata();db.session.commit()
    engine=create_engine('trino://student@trino:8080/lake/bronze')
    with engine.connect() as conn:
        rows=conn.execute(text('SELECT orderkey, sum(quantity) AS qty FROM lake.bronze.lineitem GROUP BY orderkey HAVING sum(quantity)>300 ORDER BY qty DESC,orderkey LIMIT 10')).fetchall()
    Path('/evidence/bi-bronze.json').write_text(json.dumps([list(r) for r in rows],default=str,indent=2))
