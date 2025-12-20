import json
import sys
import time
import psycopg2
from psycopg2 import sql, errors
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Конфигурация
DB_CONFIG = {
    'host': '',
    'database': '',
    'user': '',
    'password': '',
    'port': ''
}

KAFKA_BROKER = ''
TOPIC = ''


def print_header():
    print("ETL CONSUMER - Получение данных из Kafka в PostgreSQL")


def connect_postgres():
    """Подключение к PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        return conn
    except psycopg2.OperationalError as e:
        print(f"Ошибка подключения к PostgreSQL: {e}")
        return None


def connect_kafka():
    """Подключение к Kafka"""
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='etl-console-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=5000  # Таймаут для проверки сообщений
        )
        return consumer
    except NoBrokersAvailable:
        print(f"Ошибка: Не удалось подключиться к Kafka на {KAFKA_BROKER}")
        return None


def create_table_if_not_exists(conn, table_name, columns):
    """Создание таблицы если её нет"""
    cursor = conn.cursor()

    try:
        # Проверяем существование таблицы
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
        """, (table_name.lower(),))

        table_exists = cursor.fetchone()[0]

        if not table_exists:
            # Создаем таблицу
            columns_with_types = []
            for col in columns:
                columns_with_types.append(sql.SQL("{} TEXT").format(sql.Identifier(col)))

            create_query = sql.SQL("""
                CREATE TABLE {} (
                    id SERIAL PRIMARY KEY,
                    {},
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """).format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(columns_with_types)
            )

            cursor.execute(create_query)
            print(f"✓ Таблица '{table_name}' создана")
        else:
            print(f"✓ Таблица '{table_name}' уже существует")

            # Проверяем структуру таблицы
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s 
                AND column_name NOT IN ('id', 'created_at')
                ORDER BY ordinal_position;
            """, (table_name.lower(),))

            existing_columns = [row[0] for row in cursor.fetchall()]

            # Проверяем соответствие столбцов
            missing_columns = set(columns) - set(existing_columns)
            extra_columns = set(existing_columns) - set(columns)

            if missing_columns:
                print(f"  Предупреждение: Отсутствуют столбцы: {missing_columns}")
            if extra_columns:
                print(f"  Предупреждение: Лишние столбцы: {extra_columns}")

        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"✗ Ошибка создания таблицы '{table_name}': {e}")
        raise
    finally:
        cursor.close()


def insert_data(conn, table_name, columns, rows):
    """Вставка данных в таблицу"""
    cursor = conn.cursor()
    inserted_count = 0

    try:
        # Подготавливаем запрос
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name),
            sql.SQL(', ').map(sql.Identifier, columns),
            sql.SQL(', ').join([sql.Placeholder()] * len(columns))
        )

        # Вставляем данные
        for row in rows:
            try:
                # Проверяем и преобразуем значения
                row_values = []
                for val in row:
                    if val is None:
                        row_values.append(None)
                    elif isinstance(val, (int, float)):
                        row_values.append(str(val))
                    else:
                        # Обрезаем слишком длинные строки
                        row_values.append(str(val)[:255])

                cursor.execute(insert_query, row_values)
                inserted_count += 1

            except Exception as row_error:
                print(f"  Ошибка в строке {row}: {row_error}")
                conn.rollback()
                continue

        conn.commit()
        return inserted_count

    except Exception as e:
        conn.rollback()
        print(f"✗ Ошибка вставки данных: {e}")
        raise
    finally:
        cursor.close()


def process_message(conn, message):
    """Обработка сообщения из Kafka"""
    try:
        data = message.value
        table_name = data['table_name']
        columns = data['columns']
        rows = data['data']

        print(f"\n" + "-" * 50)
        print(f"ОБРАБОТКА СООБЩЕНИЯ")
        print(f"Таблица: {table_name}")
        print(f"Столбцы: {columns}")
        print(f"Записей: {len(rows)}")
        print("-" * 50)

        # Создаем таблицу
        create_table_if_not_exists(conn, table_name, columns)

        # Вставляем данные
        inserted = insert_data(conn, table_name, columns, rows)

        print(f"✓ Успешно вставлено: {inserted} из {len(rows)} записей")
        print(f"✓ Смещение: {message.offset}, Партиция: {message.partition}")

        # Показываем статистику по таблице
        cursor = conn.cursor()
        cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name)))
        total_rows = cursor.fetchone()[0]
        cursor.close()

        print(f"✓ Всего записей в таблице '{table_name}': {total_rows}")

        return True

    except Exception as e:
        print(f"✗ Ошибка обработки сообщения: {e}")
        return False


def check_connections():
    """Проверка всех подключений"""
    print_header()

    # Проверка PostgreSQL
    print("\n[1] Проверка PostgreSQL...")
    pg_conn = connect_postgres()
    if pg_conn:
        print("✓ PostgreSQL подключен")

        cursor = pg_conn.cursor()
        try:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            print(f"  Версия: {version}")

            cursor.execute("SELECT current_database();")
            db_name = cursor.fetchone()[0]
            print(f"  База данных: {db_name}")

        except Exception as e:
            print(f"✗ Ошибка: {e}")
        finally:
            cursor.close()
            pg_conn.close()
    else:
        print("✗ PostgreSQL не доступен")

    # Проверка Kafka
    print("\n[2] Проверка Kafka...")
    consumer = connect_kafka()
    if consumer:
        print(f"✓ Kafka подключен")
        print(f"  Брокер: {KAFKA_BROKER}")

        # Проверяем топики
        topics = consumer.topics()
        print(f"  Доступно топиков: {len(topics)}")

        if TOPIC in topics:
            partitions = consumer.partitions_for_topic(TOPIC)
            print(f"  Топик '{TOPIC}': есть ({len(partitions)} партиций)")
        else:
            print(f"  Топик '{TOPIC}': отсутствует")

        consumer.close()
    else:
        print("✗ Kafka не доступен")

    input("\nНажмите Enter для продолжения...")


def view_tables():
    """Просмотр существующих таблиц"""
    conn = connect_postgres()
    if not conn:
        return

    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name;
        """)

        tables = cursor.fetchall()

        print("\n" + "-" * 50)
        print("СУЩЕСТВУЮЩИЕ ТАБЛИЦЫ")
        print("-" * 50)

        if not tables:
            print("Таблиц нет")
        else:
            for i, (table_name,) in enumerate(tables, 1):
                # Получаем количество записей
                cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name)))
                count = cursor.fetchone()[0]

                # Получаем столбцы
                cursor.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = %s 
                    ORDER BY ordinal_position;
                """, (table_name,))

                columns = cursor.fetchall()
                column_names = [col[0] for col in columns if col[0] not in ['id', 'created_at']]

                print(f"\n{i}. {table_name}")
                print(f"   Записей: {count}")
                print(f"   Столбцы: {', '.join(column_names)}")

    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        cursor.close()
        conn.close()

    input("\nНажмите Enter для продолжения...")


def start_consuming():
    """Запуск потребителя"""
    print("\n" + "-" * 50)
    print("ЗАПУСК ПОТРЕБИТЕЛЯ")
    print("Нажмите Ctrl+C для остановки")
    print("-" * 50)

    # Подключаемся к БД
    conn = connect_postgres()
    if not conn:
        return

    # Подключаемся к Kafka
    consumer = connect_kafka()
    if not consumer:
        conn.close()
        return

    processed_count = 0
    error_count = 0

    try:
        while True:
            print("\nОжидание сообщений... (Ctrl+C для остановки)")

            try:
                # Получаем сообщения
                messages = consumer.poll(timeout_ms=5000)

                if not messages:
                    print("Нет новых сообщений...")
                    continue

                for topic_partition, msg_list in messages.items():
                    for message in msg_list:
                        print(f"\nПолучено сообщение из {topic_partition}")

                        success = process_message(conn, message)

                        if success:
                            processed_count += 1
                        else:
                            error_count += 1

                        print(f"\nСтатистика: обработано {processed_count}, ошибок {error_count}")

            except KeyboardInterrupt:
                print("\n\nОстановка по запросу пользователя")
                break
            except Exception as e:
                print(f"Ошибка при получении сообщений: {e}")
                error_count += 1
                time.sleep(2)

    except KeyboardInterrupt:
        print("\nПотребитель остановлен")
    finally:
        print(f"\nИтог: обработано {processed_count} сообщений, ошибок {error_count}")
        consumer.close()
        conn.close()


def main_menu():
    """Главное меню"""
    while True:
        print_header()
        print("\n1. Проверить подключения")
        print("2. Просмотреть таблицы в БД")
        print("3. Запустить потребитель (режим ожидания)")
        print("4. Обработать одно сообщение")
        print("0. Выход")

        try:
            choice = input("\nВыберите действие: ").strip()

            if choice == '1':
                check_connections()
            elif choice == '2':
                view_tables()
            elif choice == '3':
                start_consuming()
            elif choice == '4':
                process_single_message()
            elif choice == '0':
                print("\nВыход из программы")
                break
            else:
                print("Неверный выбор. Попробуйте снова.")

        except KeyboardInterrupt:
            print("\n\nВозврат в меню")
        except Exception as e:
            print(f"Ошибка: {e}")


def process_single_message():
    """Обработать одно сообщение"""
    print("\nОбработка одного сообщения...")

    conn = connect_postgres()
    if not conn:
        return

    consumer = connect_kafka()
    if not consumer:
        conn.close()
        return

    try:
        # Получаем одно сообщение
        message = next(consumer)
        process_message(conn, message)

    except StopIteration:
        print("Нет сообщений для обработки")
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        consumer.close()
        conn.close()

    input("\nНажмите Enter для продолжения...")


if __name__ == '__main__':
    try:
        main_menu()
    except KeyboardInterrupt:
        print("\n\nПрограмма завершена")
    except Exception as e:
        print(f"\nКритическая ошибка: {e}")
        sys.exit(1)