from sqlalchemy import create_engine
workengine = create_engine('postgresql://postgres:newpassword@localhost/postgres')
workconnect = workengine.connect()


ontoengine = create_engine('postgresql://postgres:newpassword@localhost/ontology')
ontoconnect = ontoengine.connect()
