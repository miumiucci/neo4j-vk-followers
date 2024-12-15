import requests
import os
import argparse
from neo4j import GraphDatabase
import logging
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VK_Neo4j_Integration")

# Параметры VK AP
BASE_URL = "https://api.vk.com/method/"
API_VERSION = "5.131"
ACCESS_TOKEN = os.getenv("VK_TOKEN")

# Параметры подключения к Neo4j
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "Zg2003")

# Инициализация драйвера Neo4j
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def get_user_id(user_name):
    """Получение ID пользователя по screen_name."""
    url = f"{BASE_URL}users.get"
    params = {
        "user_ids": user_name,
        "access_token": ACCESS_TOKEN,
        "v": API_VERSION
    }
    response = requests.get(url, params=params).json()
    if response.get("response"):
        return response["response"][0]["id"]
    logger.error(f"Ошибка получения ID пользователя: {response}")
    return None

def fetch_user_info(user_id):
    """Получение информации о пользователе."""
    url = f"{BASE_URL}users.get"
    params = {
        "user_ids": user_id,
        "fields": "screen_name,sex,home_town,city",
        "access_token": ACCESS_TOKEN,
        "v": API_VERSION
    }
    try:
        response = requests.get(url, params=params).json()
        if "response" in response:
            return response["response"][0]
        logger.warning(f"Не удалось получить информацию о пользователе {user_id}: {response}")
    except requests.RequestException as e:
        logger.error(f"Ошибка сети при запросе пользователя {user_id}: {e}")
    return None

def save_user(user_info):
    """Сохранение информации о пользователе в Neo4j."""
    with driver.session() as session:
        session.run(
            """
            MERGE (u:User {id: $id})
            SET u.name = $name,
                u.screen_name = $screen_name,
                u.sex = $sex,
                u.home_town = $home_town,
                u.city = $city
            """,
            id=user_info["id"],
            name=f"{user_info['first_name']} {user_info['last_name']}",
            screen_name=user_info.get("screen_name"),
            sex=user_info.get("sex"),
            home_town=user_info.get("home_town"),
            city=user_info.get("city", {}).get("title")
        )

def fetch_followers(user_id):
    """Получение подписчиков пользователя."""
    url = f"{BASE_URL}users.getFollowers"
    params = {
        "user_id": user_id,
        "access_token": ACCESS_TOKEN,
        "v": API_VERSION
    }
    try:
        response = requests.get(url, params=params).json()
        if "response" in response:
            return response["response"]["items"]
        logger.warning(f"Ошибка получения подписчиков {user_id}: {response}")
    except requests.RequestException as e:
        logger.error(f"Ошибка сети при запросе подписчиков {user_id}: {e}")
    return []

def save_relationship(user_id, follower_id):
    """Сохранение отношения подписки в Neo4j."""
    with driver.session() as session:
        session.run(
            """
            MATCH (u:User {id: $user_id})
            MERGE (f:User {id: $follower_id})
            MERGE (f)-[:FOLLOWS]->(u)
            """,
            user_id=user_id,
            follower_id=follower_id
        )

def process_user(user_id, depth=0):
    """Обработка пользователя и его подписчиков."""
    if depth > 1:
        return
    user_info = fetch_user_info(user_id)
    if user_info:
        save_user(user_info)
        followers = fetch_followers(user_id)
        for follower_id in followers:
            process_user(follower_id, depth + 1)
            save_relationship(user_id, follower_id)

def query_neo4j(query):
    """Выполнение запросов к Neo4j."""
    with driver.session() as session:
        return list(session.run(query))

def main():
    parser = argparse.ArgumentParser(description="Сохранение данных VK в Neo4j")
    parser.add_argument("--user", type=str, required=True, help="ID или screen_name пользователя VK")
    args = parser.parse_args()

    user_id = get_user_id(args.user)
    if not user_id:
        logger.error("Не удалось получить ID пользователя. Завершение работы.")
        return

    logger.info(f"Начало обработки пользователя {user_id}")
    process_user(user_id)

    # Примеры запросов
    queries = {
        "Всего пользователей": "MATCH (u:User) RETURN count(u) AS total_users",
        "Всего групп": "MATCH (g:Group) RETURN count(g) AS total_groups",
        "Топ 5 пользователей по количеству подписчиков": """
            MATCH (u:User)<-[:FOLLOWS]-(f)
            RETURN u.id AS user_id, u.name AS name, count(f) AS follower_count
            ORDER BY follower_count DESC LIMIT 5
        """,
        "Топ 5 самых популярных групп": """
            MATCH (g:Group)<-[:SUBSCRIBED_TO]-(u)
            RETURN g.id AS group_id, g.name AS name, count(u) AS subscriber_count
            ORDER BY subscriber_count DESC LIMIT 5
        """,
        "Пользователи, подписанные друг на друга": """
            MATCH (u1:User)-[:FOLLOWS]->(u2:User)
            WHERE (u2)-[:FOLLOWS]->(u1)
            RETURN u1.name AS name1, u2.name AS name2
        """
    }

    for description, query in queries.items():
        results = query_neo4j(query)
        logger.info(f"{description}:")
        for record in results:
            logger.info(dict(record))

if __name__ == "__main__":
    main()
