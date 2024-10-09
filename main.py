import requests
import psycopg2
import random
import time
import logging
import pandas as pd
from matplotlib import pyplot as plt

conn = psycopg2.connect(dbname="test_db", user="postgres", password="Rezdbzq16401", host="127.0.0.1")

# Конфигурация базы данных
db_config = {
    'dbname': 'test_db',
    'user': 'postgres',
    'password': 'Rezdbzq16401',
    'host': "127.0.0.1",
}

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Функция для создания таблицы vacancies
def create_table(conn):
    cursor = conn.cursor()

    create_table_query = """
        CREATE TABLE IF NOT EXISTS vacancies (
            id SERIAL PRIMARY KEY,
            city VARCHAR(50),
            company VARCHAR(200),
            industry VARCHAR(200),
            title VARCHAR(200),
            keywords TEXT,
            skills TEXT,
            experience VARCHAR(50),
            salary VARCHAR(50),
            url VARCHAR(200)
        )
    """
    cursor.execute(create_table_query)

    conn.commit()
    cursor.close()
    logging.info("Таблица 'vacancies' успешно создана.")


# Функция для удаления таблицы vacancies
def drop_table(conn):
    cursor = conn.cursor()

    drop_table_query = "DROP TABLE IF EXISTS vacancies"
    cursor.execute(drop_table_query)

    conn.commit()
    cursor.close()
    logging.info("Таблица 'vacancies' успешно удалена.")


# Функция для получения вакансий
def get_vacancies(city, vacancy, page):
    url = 'https://api.hh.ru/vacancies'
    params = {
        'text': f"{vacancy} {city}",
        'area': city,
        'specialization': 1,
        'per_page': 100,
        'page': page
    }

    response = requests.get(url, params=params, )
    response.raise_for_status()
    return response.json()


# Функция для получения навыков вакансии
def get_vacancy_skills(vacancy_id):
    url = f'https://api.hh.ru/vacancies/{vacancy_id}'
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    skills = [skill['name'] for skill in data.get('key_skills', [])]
    return ', '.join(skills)


# Функция для получения отрасли компании
def get_industry(company_id):
    # Получение отрасли компании по ее идентификатору
    if company_id is None:
        return 'Unknown'

    url = f'https://api.hh.ru/employers/{company_id}'
    response = requests.get(url)
    if response.status_code == 404:
        return 'Unknown'
    response.raise_for_status()
    data = response.json()

    if 'industries' in data and len(data['industries']) > 0:
        return data['industries'][0].get('name')
    return 'Unknown'


# Функция для парсинга вакансий
def parse_vacancies():
    cities = {
        'Москва': 1,
    }

    vacancies = [
        'Data Analyst',
        'Data Science',
        'Data Engineer',
    ]

    with psycopg2.connect(**db_config) as conn:
        drop_table(conn)
        create_table(conn)

        for city, city_id in cities.items():
            for vacancy in vacancies:
                page = 0
                while True:
                    try:
                        data = get_vacancies(city_id, vacancy, page)

                        if not data['items']:
                            break

                        with conn.cursor() as cursor:
                            for item in data['items']:
                                if vacancy.lower() not in item['name'].lower():
                                    continue  # Пропустить, если название вакансии не совпадает

                                title = f"{item['name']} ({city})"
                                keywords = item['snippet'].get('requirement', '')
                                skills = get_vacancy_skills(item['id'])
                                company = item['employer']['name']
                                industry = get_industry(item['employer'].get('id'))
                                experience = item['experience'].get('name', '')
                                salary = item['salary']
                                if salary is None:
                                    salary = "з/п не указана"
                                else:
                                    salary = salary.get('from', '')
                                url = item['alternate_url']

                                insert_query = """
                                    INSERT INTO vacancies 
                                    (city, company, industry, title, url, skills, experience, salary) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                """
                                cursor.execute(insert_query,
                                               (city, company, industry, title, url, skills, experience, salary))

                            if page >= data['pages'] - 1:
                                break

                            page += 1

                            # Задержка между запросами в пределах 1-3 секунд
                            time.sleep(random.uniform(3, 6))

                    except requests.HTTPError as e:
                        logging.error(f"Ошибка при обработке города {city}: {e}")
                        continue  # Перейти к следующему городу, если произошла ошибка

        conn.commit()

    logging.info("Парсинг завершен. Данные сохранены в базе данных PostgreSQL.")


# Функция для удаления дубликотов на основе столбца «url»
def remove_duplicates():
    with psycopg2.connect(**db_config) as conn:
        cursor = conn.cursor()

        # Удалить дубликаты на основе столбца «url»
        delete_duplicates_query = """
            DELETE FROM vacancies
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM vacancies
                GROUP BY url
            )
        """
        cursor.execute(delete_duplicates_query)

        conn.commit()
        cursor.close()

    logging.info("Дубликаты в таблице 'vacancies' удалены.")


def run_parsing_job():
    logging.info("Запуск парсинга...")

    try:
        parse_vacancies()
        remove_duplicates()
    except Exception as e:
        logging.error(f"Ошибка при выполнении парсинга: {e}")

# вызывать данную функцию только в случае нового сбора вакансий, старая таблица будет удалена!!!!
# run_parsing_job()


def postgresql_to_dataframe(conn, select_query, column_names):
    """
    Tranform a SELECT query into a pandas dataframe
    """
    cursor = conn.cursor()
    try:
        cursor.execute(select_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1

    # Naturally we get a list of tupples
    tupples = cursor.fetchall()
    cursor.close()

    # We just need to turn it into a pandas dataframe
    df = pd.DataFrame(tupples, columns=column_names)
    return df


column_names = ['title', 'experience',]

# для замены запроса по профессиям требуется заменить название в строке запроса ilike, к примеру, на %data science%
query = """
SELECT
     "experience", COUNT("experience")
FROM
     "vacancies"
WHERE title ilike '%data engineer%'     
GROUP BY
     "experience"
HAVING
     (COUNT("experience") > 1)
"""

df = postgresql_to_dataframe(conn, query, column_names)
print(df)

fig, ax = plt.subplots(figsize=(6, 3), subplot_kw=dict(aspect="equal"))
plt.pie(df['experience'], labels=df['title'])

# Adding legend
plt.title('Data Engineer', fontdict=None, loc=None)
# show plot
plt.show()