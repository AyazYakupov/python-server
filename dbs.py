from sqlalchemy import create_engine
<<<<<<< HEAD
workengine = create_engine('postgresql://dbuser:dbuser@192.168.88.89/working')
workconnect = workengine.connect()


ontoengine = create_engine('postgresql://dbuser:dbuser@192.168.88.89/ontology')
=======
workengine = create_engine('postgresql://postgres:newpassword@localhost/working')
workconnect = workengine.connect()


ontoengine = create_engine('postgresql://postgres:newpassword@localhost/ontology')
>>>>>>> 4adddfcde955d703e9733e679f5eec29daf97be8
ontoconnect = ontoengine.connect()

dbs = {'ontology': ontoconnect, 'working': workconnect}
