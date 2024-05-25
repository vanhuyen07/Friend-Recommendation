from flask import Flask, jsonify
from flask_cors import CORS
from py2neo import Graph

app = Flask(__name__)
CORS(app)

# Set up connecting
PORT = 7687
USER = "neo4j"
PASS = "12345678"
graph = Graph("bolt://localhost:7687", auth=(USER, PASS))

@app.route("/")
def index():
    return 'Hello, World!'

#check connection to neo4j database
@app.route("/check_connection")
def check_connection():
    try:
        result = graph.run("RETURN 1 AS result").data()
        if result and result[0]['result'] == 1:
            return jsonify({"status": "success", "message": "Connected to Neo4j"}), 200
        else:
            return jsonify({"status": "error", "message": "Failed to connect to Neo4j"}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# Get all users
@app.route('/api/users', methods=['GET'])
def get_all_users():
    query = """
    MATCH (u:User) RETURN u.id AS UserID, u.name AS Name, u.gender AS Gender, u.dob AS DateOfBirth
    """
    result = graph.run(query)
    users = result.data()
    return jsonify(users)

#get user title
@app.route('/api/users/<user_id>', methods=['GET'])
def get_user(user_id):
    query = """
    MATCH (u:User{id: $user_id})
    OPTIONAL MATCH (u)-[:HAS_INTEREST]->(i:Interest)
    OPTIONAL MATCH (u)-[:LIVES_IN_CITY]->(city:City)
    OPTIONAL MATCH (city)-[:LOCATED_IN_COUNTRY]->(country:Country)
    RETURN u.id AS UserID,
           u.name AS UserName,
           u.gender AS Gender,
           u.dob AS DateOfBirth,
           COLLECT(DISTINCT i.name) AS Interests,
           city.name AS City,
           country.name AS Country
    """
    result = graph.run(query, user_id=user_id)
    user = result.data()
    if user:
        return jsonify(user[0])
    else:
        return jsonify({"message": "User not found"}), 404


# Get list of interests of a user
@app.route('/api/users/<user_id>/interests', methods=['GET'])
def get_user_interests(user_id):
    query = """
    MATCH (u:User {id: $user_id})-[:HAS_INTEREST]->(i:Interest) 
    RETURN i.name AS Interest
    """
    result = graph.run(query, user_id=user_id)
    interests = result.data()
    return jsonify(interests)

#get list user live in city with user_id
@app.route('/api/users/<user_id>/city', methods=['GET'])
def get_users_in_same_city(user_id):
    query = """
    MATCH (u:User {id: $user_id})-[:LIVES_IN_CITY]->(c:City)<-[:LIVES_IN_CITY]-(other:User) 
    WHERE other.id <> $user_id 
    RETURN other.id AS UserID, other.name AS Name, c.name AS City
    """
    result = graph.run(query, user_id=user_id)
    users = result.data()
    return jsonify(users)

#get list user live in country with user_id
@app.route('/api/users/<user_id>/country', methods=['GET'])
def get_users_in_same_country(user_id):
    query = """
    MATCH (u:User {id: $user_id})-[:LIVES_IN_COUNTRY]->(c:Country)<-[:LIVES_IN_COUNTRY]-(other:User) 
    WHERE other.id <> $user_id 
    RETURN other.id AS UserID, other.name AS Name, c.name AS Country
    """
    result = graph.run(query, user_id=user_id)
    users = result.data()
    return jsonify(users)


#KNN recommentdations
@app.route('/api/users/<user_id>/knn_recommendations', methods=['GET'])
def recommend_friends_knn(user_id):
    query = """
    MATCH (u:User{id:$user_id})-[s:SIMILAR_TO]-(similar:User)
    WITH similar, COLLECT(s.score) AS scores
    RETURN similar.id AS SimilarUserID, similar.name AS SimilarUserName, apoc.coll.max(scores) AS maxScore
    ORDER BY maxScore DESC
    LIMIT 10;
    """
    result = graph.run(query, user_id=user_id)
    recommendations = result.data()
    return jsonify(recommendations)

#PAGERANK recommendations
@app.route('/api/users/<user_id>/pagerank_recommendations', methods=['GET'])
def recommend_friends_pagerank(user_id):
    query = """
    CALL gds.pageRank.stream(
        'knnGraph',
        {
            maxIterations: 20,
            dampingFactor: 0.85
        }
    )
    YIELD nodeId, score
    WITH gds.util.asNode(nodeId) AS userNode, score
    WHERE userNode.id = $user_id
    MATCH (userNode)-[:SIMILAR_TO]-(similarNode:User)
    WHERE similarNode.id <> $user_id
    RETURN DISTINCT similarNode.id AS suggestedUserId, similarNode.name AS suggestedUserName, score
    ORDER BY score DESC
    LIMIT 10
    """
    result = graph.run(query, user_id=user_id)
    recommendations = result.data()
    return jsonify(recommendations)


if __name__ == '__main__':
    app.run(debug=True)
