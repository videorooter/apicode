#! /usr/bin/python3
from bottle import route, run, template, default_app, get, request, abort, response
from bottle.ext import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, Sequence, String, func
from sqlalchemy.dialects.mysql import DATETIME, TIMESTAMP, TEXT
from sqlalchemy.ext.declarative import declarative_base
from json import dumps

app = default_app()

app.config.load_config('api.conf')
app.config.setdefault('api.db', 'sqlite:///:memory:')
app.config.setdefault('api.base', 'http://localhost:8080')

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

#
# This is a compatibility function for Elog.io clients. Clients
# would also send src, context, page and per_page parameters. Tha
# latter two for compatibility with RFC 5005 Link header. We ignore
# everything other than hash at the moment and limit the search to
# images specifically.
#
@get('/lookup/blockhash')
def lookup_blockhash(db):
    hash = request.query.hash
    if not hash:
        abort(400, 'hash is a required parameter')
    if len(hash) != 64:
        abort(400, 'hash must be a 256-bit hexadecimal encoded value')

    entity = db.query(Work.id, func.hammingdistance(hash, Work.block_hash_code).label('distance')).filter(func.hammingdistance(hash, Work.block_hash_code) < 10, Work.is_video == 0).limit(1000).all()
    if not entity:
        abort(404, 'no works found')
    d = []
    for row in entity:
       d.append({'href': "%s/works/%s" % (app.config['api.base'], row.id),
                 'distance': row.distance})

    response.content_type = 'application/json'
    return dumps(d)

@get('/lookup/hash')
def lookup_blockhash(db):
    hash = request.query.hash
    if not hash:
        abort(400, 'hash is a required parameter')
    if len(hash) != 64:
        abort(400, 'hash must be a 256-bit hexadecimal encoded value')

    method = request.query.method or 'blockhash'
    distance = request.query.distance or 10
    #
    # We should implement a flexible limit / pager here, for now a hard
    # limit of 1000 should be enough.
    #
    entity = db.query(Work).filter(func.hammingdistance(hash, Work.block_hash_code) < distance).limit(1000).all()
    if not entity:
        abort(404, 'no works found')
    d = []
    for row in entity:
       d.append({'id': row.id, 'title': row.title})
    response.content_type = 'application/json'
    return dumps(d)

@get('/works/<id>')
def get_works(id, db):
    entity = db.query(Work).filter_by(id=id).first()
    if entity:
        d = {}
        for column in entity.__table__.columns:
            d[column.name] = str(getattr(entity, column.name))

        return d
# {'id': entity.id, 'title': entity.title}
    return HTTPError(404, 'Entity not found.')


run(host='0.0.0.0', port=8080)

