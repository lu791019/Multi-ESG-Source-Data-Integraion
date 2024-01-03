from sqlalchemy import Column, Integer, Boolean, DateTime, MetaData
from sqlalchemy.ext.declarative import declarative_base

class EtlStatus(declarative_base(metadata=MetaData(schema='app'))):
    __tablename__ = 'etl_status'

    id = Column(Integer, primary_key=True)
    done = Column(Boolean)
    executed_at = Column(DateTime(timezone=False))
    done_at = Column(DateTime(timezone=False))
    updated_at = Column(DateTime(timezone=False))
     
    def __init__(self, done, executed_at, done_at, updated_at):
        self.done = done
        self.executed_at = executed_at
        self.done_at = done_at
        self.updated_at = updated_at