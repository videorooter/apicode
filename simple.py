#! /usr/bin/python3
from bottle import route, run, template, default_app, get, request, abort, response, redirect
from bottle.ext import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, Sequence, String, func
from sqlalchemy.dialects.mysql import DATETIME, TIMESTAMP, TEXT
from sqlalchemy.ext.declarative import declarative_base
from json import dumps
import string
from bs4 import BeautifulSoup

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
# would also send other parameters, which we ignore all of them,
# including any compatibility with RFC 5005 Link. Also limit
# the search to images only.
#
@get('/lookup/blockhash')
def lookup_blockhash(db):
    hash = request.query.hash
    if not hash:
        abort(400, 'hash is a required parameter')
    if len(hash) != 64:
        abort(400, 'hash must be a 256-bit hexadecimal encoded value')

    entity = db.query(Work.id, func.hammingdistance(hash, Work.block_hash_code).label('distance')).filter(func.hammingdistance(hash, Work.block_hash_code) < 10, Work.is_video == 0).limit(1000).all()
    d = []
    for row in entity:
       d.append({'href': "%s/works/%s" % (app.config['api.base'], row.id),
                 'distance': row.distance})

    response.content_type = 'application/json'
    return dumps(d)

# This includes video lookup
@get('/lookup/video')
def lookup_blockhash(db):
    hash = request.query.hash
    if not hash:
        abort(400, 'hash is a required parameter')
    if len(hash) != 64:
        abort(400, 'hash must be a 256-bit hexadecimal encoded value')

    entity = db.query(Work.id, func.hammingdistance(hash, Work.block_hash_code).label('distance')).filter(func.hammingdistance(hash, Work.block_hash_code) < 10, Work.is_video == 1).limit(1000).all()
    d = []
    for row in entity:
       d.append({'href': "%s/works/%s" % (app.config['api.base'], row.id),
                 'distance': row.distance})

    response.content_type = 'application/json'
    return dumps(d)

#
# Elog.io
#
#[
#  {
#    "href": "https://catalog.elog.io/works/5396e592d7d163613d7321ee",
#    "uri": "http://some.where/foo.jpg",
#    "property": "locator",
#    "score": 100
#  }
#]
#
@get('/lookup/uri')
def lookup_uri(db):
    uri = request.query.uri

    response.content_type = 'application/json'
    return dumps([])

# Elog.io
@get('/works/<id>/media')
def get_works_media(id, db):
    entity = db.query(Work).filter_by(id=id).first()
    if entity:
       d = { 'id': entity.id,
             'href': "%s/works/%s/media" % (app.config['api.base'], entity.id),
             'annotations' : [
                {
                   "property": {
                      "propertyName" : "locator",
                      "locatorLink" : entity.img_url
                   }
                }
             ]
           }
       return d 
    about(404, 'media not found')

# Elog.io
# Clients could select which information to return, we ignore
# this and return everything we know.
@get('/works/<id>')
def get_works(id, db):
    entity = db.query(Work).filter_by(id=id).first()
    if entity:
        d = { 'id': entity.id,
              'href': "%s/works/%s" % (app.config['api.base'], entity.id),
              'public': 'true',
              'added_at': '2015-02-21T11:11:12.685Z',
              'description': entity.imagedescription,
              'owner': { 'org': { 'id': 1, 'href': 'http://example.com'}}
            }

        d['media'] = []
        d['media'].append({ 'id': entity.id,
                            'href': "%s/works/%s/media" % (app.config['api.base'], entity.id) })
        d['annotations'] = []
        d['annotations'].append({
            'propertyName': 'title',
            'language': 'en',
            'titleLabel': entity.object_name })

        # Identify that this is a wikimedia commons work
        if entity.img_url.find('https://upload.wikimedia.org/wikipedia/commons') == 0:
            identifier = "https://commons.wikimedia.org/wiki/%s" % entity.title
            collection = "http://commons.wikimedia.org"
        else:
            abort(500, 'unknown collection in table')

        d['annotations'].append({
            'propertyName': 'identifier',
            'identifierLink': identifier })
        d['annotations'].append({
            'propertyName': 'locator',
            'locatorLink': entity.img_url })
        d['annotations'].append({
            'propertName': 'policy',
            'statementLabel': entity.license_shortname,
            'statementLink': entity.license_url,
            'typeLabel': 'license',
            'typeLink': 'http://www.w3.org/1999/xhtml/vocab#license' })
        d['annotations'].append({
            'propertyName': 'collection',
            'collectionLink': collection })

        # Process artist through Soup, since it often contain HTML code
        soup = BeautifulSoup(entity.artist)

        d['annotations'].append({
            'propertyName': 'creator',
            'creatorLabel': soup.get_text() })
        d['annotations'].append({
            'propertyName': 'copyright',
            'holderLabel': soup.get_text() })
        
        return d
    abort(404, 'id not found')


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

@get('/random')
def random(db):
    type = request.query.type
    entity = db.query(Work.id).order_by(func.rand())
    if type == "video":
        entity = entity.filter(Work.is_video == 1)
    if type == "image":
        entity = entity.filter(Work.is_video == 0)
    entity = entity.first()
    if not entity:
        abort(404, 'no works found -- not a one!')
    redirect('/works/%s' % entity.id)

run(host='0.0.0.0', port=8080)

