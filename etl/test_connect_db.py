qimport sqlalchemy

from sqlalchemy import create_engine

db_port = 5432
db_user = 'forecaster'
db_password = ''
db_host = 'genius-forecaster-20210622125859343300000001.ccjvnxzgjtv6.eu-west-1.rds.amazonaws.com'

# db_uri = f'postgresql://{}(db_user)s:%(db_password)s@%(db_host)s:%(db_port)s/%%s'
db_uri = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/test_db'

engine = create_engine(db_uri, echo=True)

from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey

metadata = MetaData()
users = Table('users', metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String),
              Column('fullname', String), )

addresses = Table('addresses', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('user_id', None, ForeignKey('users.id')),
                  Column('email_address', String, nullable=False)
                  )

metadata.create_all(engine)

ins = users.insert().values(name='jack', fullname='Jack Jones')

conn = engine.connect()

result = conn.execute(ins)