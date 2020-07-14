from dynamoer.entity import Entity

def test_entity():
    data = {
        "name": "Test"
    }
    entity = Entity(data=data)
