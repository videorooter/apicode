#! /usr/bin/python3
from bottle import route, run, template, default_app, get, request, abort, response, redirect, post
from bottle.ext import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, Sequence, String, func, ForeignKey, LargeBinary
from sqlalchemy.dialects.mysql import DATETIME, TIMESTAMP, TEXT, INTEGER
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy_fulltext import FullText, FullTextSearch
import random

from json import dumps
import string
from bs4 import BeautifulSoup

app = default_app()

app.config.load_config('api.conf')
app.config.setdefault('api.db', 'sqlite:///:memory:')
app.config.setdefault('api.base', 'http://localhost:8080')
app.config.setdefault('api.queuedir', '/tmp')

Base = declarative_base()
engine = create_engine(app.config['api.db'], echo=False)

plugin = sqlalchemy.Plugin(
    engine,
    keyword='db',
    commit=True,
    use_kwargs=False
)

app.install(plugin)

class Expression(FullText, Base):
    __tablename__ = 'expression'
    __fulltext_columns__ = ('description', 'title', 'credit')
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

class Queue(Base):
    __tablename__ = 'queue'
    id = Column(INTEGER(unsigned=True, zerofill=True),
               Sequence('queue_id_seq', start=1, increment=1),
               primary_key = True)
    queryhash = Column(String(256))
    requested_date = Column(TIMESTAMP, default=datetime.utcnow, nullable=False)
    completed_date = Column(TIMESTAMP)
    status = Column(INTEGER, default=0) # 0 = unprocessed, 1 = working, 2 = done
    email = Column(String(256))
    results = relationship('QueueResults', backref="queue")

class QueueResults(Base):
    __tablename__ = 'queue_results'
    id = Column(INTEGER(unsigned=True, zerofill=True),
               Sequence('queue_results_id_seq', start=1, increment=1),
               primary_key = True)
    qid = Column(INTEGER(unsigned=True, zerofill=True),
                ForeignKey('queue.id'))
    distance = Column(INTEGER)
    expression_id = Column(INTEGER(unsigned=True, zerofill=True))

Base.metadata.create_all(engine)

hashers = {  
             'http://videorooter.org/ns/blockhash': {
                'command': '/home/api/algorithms/commonsmachinery-blockhash/build/blockhash',   
                'types': ['image/png', 'image/jpg'],
              },
             'http://videorooter.org/ns/x-blockhash-video-cv': {
                'command': '/home/api/algorithms/jonasob-blockhash-master/build/blockhash_video',
                'types': ['video/mp4', 'video/mpeg', 'video/ogg', 'video/webm'],
              },
          }

#
# These are videorooter specific API calls following
#
@post('/videorooter/video')
def videorooter_video(db):
    # Takes two arguments: email - (option) email of the user requesting
    #                      file - binary data with file
    email = request.forms.get('email')
    file = request.files.get('file') 
    hash = "%032x" % random.getrandbits(128)
    if (not file):
      abort(400, 'file is a required parameter')

    obj = Queue(queryhash = hash, email = email)
    db.add(obj)
    db.commit()

    file.save("%s/%s" % (app.config['api.queuedir'], hash))
    response.content_type = 'application/json'
    s = { "process_id": hash }
    return dumps(s)

@get('/videorooter/results/<id>')
def videorooter_results(id, db):
    entity = db.query(Queue).filter_by(queryhash=id).first()
    if not entity:
       abort(404, 'Sorry. Invalid process number')
    if entity.status < 2:
       abort(202, 'Your search is being processed. Check back later for the results.')
    d = []
    for row in entity.results:
       d.append({'href': "%s/works/%s" % (app.config['api.base'], row.expression_id),
                 'distance': row.distance})
    response.content_type = 'application/json'
    return dumps(d)
 

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

#
# Does a lookup of text across the fields title, description and credit
# 
# NB: This requires a full text index:
# create fulltext index fulltext_idx on expression (title, description, credit);
#
@get('/lookup/text')
def lookup_text(db):
    q = request.query.q
    if not q:
       abort(400, 'q is a required parameter')
    entity = db.query(Expression).filter(FullTextSearch(q, Expression)).limit(1000).all()
    d = []
    for row in entity:
       d.append({'href': "%s/works/%s" % (app.config['api.base'], row.id) })

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
def randomwork(db):
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

