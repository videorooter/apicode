#! /usr/bin/python3
from bottle import route, run, template, default_app, get, request, abort, response, redirect
from bottle.ext import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, Sequence, String, func, ForeignKey
from sqlalchemy.dialects.mysql import DATETIME, TIMESTAMP, TEXT, INTEGER
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from sqlalchemy.orm import relationship, sessionmaker

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

class Expression(Base):
    __tablename__ = 'expression'
    id = Column(INTEGER(unsigned=True, zerofill=True),
                Sequence('expression_id_seq', start=1, increment=1),
                primary_key = True)
    title = Column(String(500))
    description = Column(String(2048))
    # 
    # Allowed:  Any CC URI + defined by rightsstatements.org
    #
    rights_statement = Column(String(128))
    credit = Column(String(500))
    credit_url = Column(String(1024))
    #
    # http://wikimedia.org/
    #
    collection_url = Column(String(128))
    source_id = Column(String(256))
    updated_date = Column(TIMESTAMP, default=datetime.utcnow,
                          nullable=False, onupdate=datetime.utcnow)
    manifestation = relationship('Manifestation', backref="expression")

class Manifestation(Base):
    __tablename__ = 'manifestation'
    id = Column(INTEGER(unsigned=True, zerofill=True),
                Sequence('manifestation_id_seq', start=1, increment=1),
                primary_key = True)
    url = Column(String(500))
    # 
    # media_type:  image/jpeg image/gif  image/png video/mpeg video/mp4
    #              video/ogg  video/webm audio/ogg
    #
    media_type = Column(String(64))
    expression_id = Column(INTEGER(unsigned=True, zerofill=True), 
                     ForeignKey('expression.id'))
    fingerprint = relationship('Fingerprint', backref="manifestation")

class Fingerprint(Base):
    __tablename__ = 'fingerprint'
    id = Column(INTEGER(unsigned=True, zerofill=True),
                Sequence('fingerprint_id_seq', start=1, increment=1),
                primary_key = True)
    #
    #
    type = Column(String(64))
    hash = Column(String(256))
    updated_date = Column(TIMESTAMP, default=datetime.utcnow,
                          nullable=False, onupdate=datetime.utcnow)
    manifestation_id = Column(INTEGER(unsigned=True, zerofill=True), 
                     ForeignKey('manifestation.id'))
 

hashers = {  
             'http://videorooter.org/ns/blockhash': {
                'types': ['image/png', 'image/jpg'],
              },
             'http://videorooter.org/ns/x-blockhash-video-cv': {
                'types': ['video/mp4', 'video/mpeg', 'video/webm'],
              },
          }

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

    distance = 10   # Maximum value
    if (request.query.distance and int(request.query.distance) <= distance):
        distance = int(request.query.distance)

    entity = db.query(Expression, Manifestation, Fingerprint, func.hammingdistance(hash, Fingerprint.hash).label('distance')).filter(func.hammingdistance(hash, Fingerprint.hash) < distance, Manifestation.media_type.in_(hashers['http://videorooter.org/ns/blockhash']['types']), Manifestation.id == Fingerprint.manifestation_id, Expression.id == Manifestation.expression_id ).limit(1000).all()
    d = []
    for row in entity:
       d.append({'href': "%s/works/%s" % (app.config['api.base'], row.Expression.id),
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

    distance = 40   # Maximum value
    if (request.query.distance and int(request.query.distance) <= distance):
        distance = int(request.query.distance)

    entity = db.query(Expression, Manifestation, Fingerprint, func.hammingdistance(hash, Fingerprint.hash).label('distance')).filter(func.hammingdistance(hash, Fingerprint.hash) < distance, Manifestation.media_type.in_(hashers['http://videorooter.org/ns/x-blockhash-video-cv']['types']), Manifestation.id == Fingerprint.manifestation_id, Expression.id == Manifestation.expression_id ).limit(1000).all()
    d = []
    for row in entity:
       d.append({'href': "%s/works/%s" % (app.config['api.base'], row.Expression.id),
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
    entity = db.query(Expression,Manifestation).filter_by(Expression.id==id,Manifestation.expression_id == id).first()
    if entity:
       d = { 'id': entity.id,
             'href': "%s/works/%s/media" % (app.config['api.base'], entity.Expression.id),
             'annotations' : [
                {
                   "property": {
                      "propertyName" : "locator",
                      "locatorLink" : entity.Manifestation.url
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
    entity = db.query(Expression, Manifestation).filter(Expression.id==id, Manifestation.expression_id==id).first()
    if entity:
        d = { 'id': entity.Expression.id,
              'href': "%s/works/%s" % (app.config['api.base'], entity.Expression.id),
              'public': 'true',
              'added_at': '2015-02-21T11:11:12.685Z',
              'description': entity.Expression.description,
              'owner': { 'org': { 'id': 1, 'href': 'http://example.com'}}
            }

        d['media'] = []
        d['media'].append({ 'id': entity.Expression.id,
                            'href': "%s/works/%s/media" % (app.config['api.base'], entity.Expression.id) })
        d['annotations'] = []
        d['annotations'].append({
            'propertyName': 'title',
            'language': 'en',
            'titleLabel': entity.Expression.title })

        d['annotations'].append({
            'propertyName': 'identifier',
            'identifierLink': entity.Expression.source_id })
        d['annotations'].append({
            'propertyName': 'locator',
            'locatorLink': entity.Manifestation.url })
        d['annotations'].append({
            'propertName': 'policy',
            'statementLink': entity.Expression.rights_statement,
            'typeLabel': 'license',
            'typeLink': 'http://www.w3.org/1999/xhtml/vocab#license' })
        d['annotations'].append({
            'propertyName': 'collection',
            'collectionLink': entity.Expression.collection_url })

        # Process artist through Soup, since it often contain HTML code
        soup = BeautifulSoup(entity.Expression.credit)

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
    entity = db.query(Expression,Fingerprint,Manifestation).filter(func.hammingdistance(hash, Fingerprint.hash) < distance, Manifestation.id == Fingerprint.manifestation_id, Expression.id == Manifestation.expression_id).limit(1000).all()
    if not entity:
        abort(404, 'no works found')
    d = []
    for row in entity:
       d.append({'id': row.Expression.id, 'title': row.Expression.title})
    response.content_type = 'application/json'
    return dumps(d)

@get('/random')
def random(db):
    type = request.query.type
    entity = db.query(Expression,Manifestation,Fingerprint).order_by(func.rand()).filter(Expression.id==Manifestation.expression_id,Manifestation.id==Fingerprint.manifestation_id)
    if type == "video":
        entity = entity.filter(Manifestation.media_type.in_(hashers['http://videorooter.org/ns/x-blockhash-video-cv']['types']))
    if type == "image":
        entity = entity.filter(Manifestation.media_type.in_(hashers['http://videorooter.org/ns/blockhash']['types']))
    entity = entity.first()
    if not entity:
        abort(404, 'no works found -- not a one!')
    redirect('/works/%s' % entity.Expression.id)

run(host='0.0.0.0', port=8080)

