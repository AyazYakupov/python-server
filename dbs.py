from sqlalchemy import create_engine
workengine = create_engine('postgresql://dbuser:dbuser@192.168.88.88/working')
workconnect = workengine.connect()


ontoengine = create_engine('postgresql://dbuser:dbuser@192.168.88.88/ontology')
ontoconnect = ontoengine.connect()

dbs = {'ontology': ontoconnect, 'working': workconnect}
