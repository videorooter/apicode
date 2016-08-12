#! /usr/bin/python3
from bottle import default_app
from bottle.ext import sqlalchemy

from sqlalchemy import create_engine, Column, Integer, Sequence, String, func, ForeignKey, LargeBinary
from sqlalchemy.dialects.mysql import DATETIME, TIMESTAMP, TEXT, INTEGER
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from sqlalchemy.orm import relationship, sessionmaker
from datetime import datetime

import time
import random
import subprocess
import os
import fasteners
import mimetypes
import magic
import logging

from json import dumps
import string

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

def lock_me():
  a_lock = fasteners.InterProcessLock('/tmp/backend-queue.lock')
  for i in range(10):
    gotten = a_lock.acquire(blocking=False)
    if gotten:
       time.sleep(0.2)
    else:
       time.sleep(10)

lock_me()

dbsession = sessionmaker(bind=engine)
db = dbsession()

m = magic.Magic(flags=magic.MAGIC_MIME_TYPE)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)8s  %(message)s')
log = logging.getLogger('queuerun')

entity = db.query(Queue).filter_by(status=0).limit(10).all()
for row in entity:
   log.debug("%d: Starting identification" % row.id)
   db.query(Queue).filter(Queue.id==row.id).update({ Queue.status: 1 })
   db.commit()
   filename = "%s/%s" % (app.config['api.queuedir'], row.queryhash)
   mime_type = m.id_filename(filename)
   log.debug("%d: Identified as %s" % (row.id, mime_type))
   for k, v in hashers.items():
       if (not mime_type in v['types']):
            continue
       cmd = subprocess.check_output([v['command'], filename], stderr=subprocess.DEVNULL)
       if cmd.split()[0]:
          type = mime_type
          hash = cmd.split()[0]
          log.debug("%d: Hash=%s, type=%s" % (row.id, hash, type))
          distance = 10   # Maximum value

          query = db.query(Expression, Manifestation, Fingerprint, func.hammingdistance(hash, Fingerprint.hash).label('distance')).filter(func.hammingdistance(hash, Fingerprint.hash) < distance, Manifestation.media_type.in_(hashers[k]['types']), Manifestation.id == Fingerprint.manifestation_id, Expression.id == Manifestation.expression_id ).limit(1000).all()
          if not query:
             log.debug("%d: No matching works found" % row.id)
          for result in query:
             log.debug("%d: Identified %d as match" % (row.id, result.Expression.id))
             d = QueueResults(qid = row.id,
                              distance = result.distance,
                              expression_id = result.Expression.id)
             db.add(d)
             db.commit()
   db.query(Queue).filter(Queue.id==row.id).update({ Queue.status: 2,
                          Queue.completed_date: datetime.utcnow() })
   db.commit()
   log.debug("%d: Completed work" % row.id)
