from sqlalchemy import create_engine
workengine = create_engine('postgresql://postgres:newpassword@localhost/working')
workconnect = workengine.connect()


ontoengine = create_engine('postgresql://postgres:newpassword@localhost/ontology')
ontoconnect = ontoengine.connect()

dbs = {'ontology': ontoconnect, 'working': workconnect}
