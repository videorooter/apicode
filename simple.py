#! /usr/bin/python3
from bottle import route, run, template, default_app, get
from bottle.ext import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, Sequence, String
from sqlalchemy.dialects.mysql import DATETIME, TIMESTAMP, TEXT

from sqlalchemy.ext.declarative import declarative_base

app = default_app()

app.config.load_config('api.conf')
app.config.setdefault('api.db', 'sqlite:///:memory:')

Base = declarative_base()
engine = create_engine(app.config['api.db'], echo=True)

plugin = sqlalchemy.Plugin(
    engine,
    keyword='db',
    commit=True,
    use_kwargs=False
)

app.install(plugin)

class Work(Base):
    __tablename__ = 'article_images'
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(150))
    userid = Column(String(100))
    img_url = Column(TEXT)
    sha1 = Column(String(150))
    artist = Column(String(250))
    object_name = Column(String(250))
    credit = Column(String(250))
    usage_terms = Column(String(250))
    license_url = Column(String(150))
    license_shortname = Column(String(250))
    imagedescription = Column(String(250))
    copyrighted = Column(String(50))
    timestamp = Column(String(150))
    continue_code = Column(String(250))
    is_copied = Column(Integer)
    black_hash = Column(String(150)) # To remove
    gen_filename = Column(String(225))
    block_hash_code = Column(String(250))
    is_video = Column(Integer)
    inserted_date = Column(TIMESTAMP)
    updated_date = Column(DATETIME)
    api_from = Column(Integer)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "<Work('%d', '%s')>" % (self.id, self.name)

@route('/hello/<name>')
def index(name):
    return template('<b>Hello {{name}}</b>!', name=name)

@get('/work/<id>')
def get_work(id, db):
    entity = db.query(Work).filter_by(id=id).first()
    if entity:
        return {'id': entity.id, 'title': entity.title}
    return HTTPError(404, 'Entity not found.')


run(host='0.0.0.0', port=8080)

