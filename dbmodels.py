from sqlalchemy import Column, String, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import create_engine
from uuid import uuid4
from sqlalchemy.orm.session import sessionmaker

Base = declarative_base()


class Oclass(Base):
    __tablename__ = 'oclass'
    uuid = Column(UUID(as_uuid=True), default=uuid4, primary_key=True)
    name_class = Column(String(64))
    tech_name_class = Column(String(64))
    type_class = Column(String(64))


class Olink(Base):
    __tablename__ = 'olink'
    uuid = Column(UUID(as_uuid=True), default=uuid4, primary_key=True)
    src_class_id = Column(UUID(as_uuid=True), ForeignKey('oclass.uuid', ondelete='CASCADE'))
    dst_class_id = Column(UUID(as_uuid=True), ForeignKey('oclass.uuid', ondelete='CASCADE'))
    dst_attribute_id = Column(UUID(as_uuid=True), ForeignKey('oattribute.uuid', ondelete='CASCADE'))
    linktype_id = Column(UUID(as_uuid=True), ForeignKey('olinktype.uuid', ondelete='CASCADE'))
    value = Column(Text)


class Olinktype(Base):
    __tablename__ = 'olinktype'
    uuid = Column(UUID(as_uuid=True), default=uuid4, primary_key=True)
    tech_name_linktype = Column(String(64))
    name_linktype = Column(String(64))


class Oattribute(Base):
    __tablename__ = 'oattribute'
    uuid = Column(UUID(as_uuid=True), default=uuid4, primary_key=True)
    name_attr = Column(String(64))
    tech_name_attr = Column(String(64))


if __name__ == '__main__':
    engine = create_engine('postgresql://postgres:newpassword@localhost/ontology')
    Base.metadata.create_all(engine)
    Session = sessionmaker(autoflush=True)
    Session.configure(bind=engine)
    sess = Session()
    c = Oclass(type_class='visualint', name_class='test', tech_name_class='test')
    lt = Olinktype(tech_name_linktype='Owner', name_linktype='Владелец')
    a = Oattribute(name_attr='Имя', tech_name_attr='Name')
    a1 = Oattribute(name_attr='Тип', tech_name_attr='Type')
    a2 = Oattribute(name_attr='Шаблон', tech_name_attr='Template')
    sess.add_all([c, lt, a, a1, a2])
    sess.flush()
    for i in [[a, 'Test'], [a1, 'Testing'], [a2, 'TESTTESTTEST']]:
        sess.add(Olink(src_class_id=c.uuid, linktype_id=lt.uuid, dst_attribute_id=i[0].uuid, value=i[1]))
    sess.flush()
    sess.commit()

