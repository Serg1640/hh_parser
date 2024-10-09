# hh_parser
Parser for research info of vacancies of 'Data Analyst', 'Data Science',  'Data Engineer'
Парсер вакансий ('Data Analyst', 'Data Science',  'Data Engineer') работает через API hh.ru. Путем запроса (json формат) получает информацию о вакансиях в городе (городах), с последующим сохранением в базе данных Postgres. Ссылка на документацию к API -https://api.hh.ru/openapi/redoc#section/Obshaya-informaciya
Ссылка на прототип парсера через API hh.ru - https://github.com/shakhbanov/HeadHunter

1. Клонирование репозитория 

```git clone https://github.com/Serg1640/hh_parser.git```

2. Переход в директорию Oxygen

```cd hh_parser```

3. Создание виртуального окружения

```python3 -m venv venv```

4. Активация виртуального окружения

```source venv/bin/activate```

5. Установка зависимостей

```pip install -r requirements.txt``
