import csv
from py2neo import Graph, Node, Relationship

N_Node = 100000

def main():
    # Set up connecting
    PORT = 7687
    USER = "neo4j"
    PASS = "12345678"
    graph = Graph("bolt://" + ":7687", auth=(USER, PASS))

    csv_file = 'data/SocialMediaUsersDataset.csv'

    #Import dataset CSV
    print(" import and create user node")
    import_users(graph, csv_file)

    print(" import and create city node and relationship")
    import_city(graph, csv_file)

    print("import and create country node and relationship")
    import_country(graph, csv_file)

    print(" import and create Interests node")
    import_interests(graph, csv_file)

def import_users(graph, csv_file):
    with open(csv_file, encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)

        for i, row in enumerate(reader):
            if (i % 100 == 0):
                print(f"{i}/{N_Node} Create user node ")
            if i >= N_Node:
                break

            user_id = row['UserID']
            name = row['Name']
            gender = row['Gender']
            dob = row['DOB']
            user_node = Node("User", id=user_id, name=name, gender=gender, dob=dob)
            graph.create(user_node)

def import_interests(graph, csv_file):
    with open(csv_file, encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        existing_interests = set()  # Set để lưu trữ các sở thích đã tồn tại trong đồ thị
        for i, row in enumerate(reader):
            if i % 100 == 0:
                print(f"{i}/{N_Node} Create interests node anh relationship ")
            if i >= N_Node:
                break

            user_id = row['UserID']
            interests = row['Interests']

            if interests:
                interest_list = interests.strip("'").split("', '")

                user_node = graph.nodes.match("User", id=user_id).first()
                if user_node is not None:
                    for interest in interest_list:
                        # Tạo khóa duy nhất cho sở thích (chuyển về viết thường)
                        interest_key = interest.lower()
                        # Thêm khóa sở thích vào tập hợp đã tồn tại
                        existing_interests.add(interest_key)
                        interest_node = graph.nodes.match("Interest", name=interest).first()
                        if interest_node is None:
                            interest_node = Node("Interest", name=interest)
                            graph.create(interest_node)
                        Has_interest_rel = Relationship(user_node, "HAS_INTEREST", interest_node)
                        graph.create(Has_interest_rel)

def import_city(graph, csv_file):
    with open(csv_file, encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        existing_cities = set()  # Danh sách thành phố đã tồn tại trong đồ thị

        for i, row in enumerate(reader):
            if i % 100 == 0:
                print(f"{i}/{N_Node} Create city node and relationship")
            if i >= N_Node:
                break

            city = row['City']

            # Tạo nút "City" hoặc lấy nút đã tồn tại từ danh sách city_nodes
            city_node = graph.nodes.match("City", name=city).first()
            if city_node is None:
                city_node = Node("City", name=city)
                graph.create(city_node)
                city_node.add_label("City")  # Thêm nhãn "City"
                existing_cities.add(city)  # Thêm thành phố vào danh sách thành phố đã tồn tại

            # Tạo mối quan hệ "LIVES_IN_CITY" giữa người dùng và thành phố
            user_node = graph.nodes.match("User", id=row['UserID']).first()
            if user_node is not None:
                city_in_relationship = Relationship(user_node, "LIVES_IN_CITY", city_node)
                graph.create(city_in_relationship)

def import_country(graph, csv_file):
    with open(csv_file, encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        existing_countries = set()  # Danh sách nước đã tồn tại trong đồ thị

        for i, row in enumerate(reader):
            if i % 100 == 0:
                print(f"{i}/{N_Node} Create country node and relationship")
            if i >= N_Node:
                break
            city = row['City']
            country = row['Country']

            # Tạo nút "Country" hoặc lấy nút đã tồn tại từ danh sách existing_countries
            country_node = graph.nodes.match("Country", name=country).first()
            if country_node is None:
                country_node = Node("Country", name=country)
                graph.create(country_node)
                country_node.add_label("Country")  # Thêm nhãn "Country"
                existing_countries.add(country)  # Thêm nước vào danh sách nước đã tồn tại

            # Tạo mối quan hệ "LIVES_IN_COUNTRY" giữa người dùng và nước
            user_node = graph.nodes.match("User", id=row['UserID']).first()
            if user_node is not None:
                country_in_relationship = Relationship(user_node, "LIVES_IN_COUNTRY", country_node)
                graph.create(country_in_relationship)

            # Tạo mối quan hệ "LOCATED_IN_COUNTRY" giữa thành phố và quốc gia
            city_node = graph.nodes.match("City", name=row['City']).first()
            if city_node is not None:
                located_in_relationship = Relationship(city_node, "LOCATED_IN_COUNTRY", country_node)
                graph.create(located_in_relationship)


if __name__ == '__main__':
    main()