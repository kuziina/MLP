import json
import csv
import os
import sys
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Конфигурация
KAFKA_BROKER = ''
TOPIC = ''


def print_header():
    print("ETL PRODUCER - Отправка данных в Kafka")


def connect_kafka():
    """Подключение к Kafka"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3
        )
        return producer
    except NoBrokersAvailable:
        print(f"Ошибка: Не удалось подключиться к Kafka на {KAFKA_BROKER}")
        return None


def validate_table_name(name):
    """Валидация имени таблицы"""
    if not name:
        return False, "Имя таблицы не может быть пустым"
    if len(name) > 63:
        return False, "Имя таблицы слишком длинное (макс 63 символа)"
    if not name.replace('_', '').isalnum():
        return False, "Имя таблицы может содержать только буквы, цифры и _"
    return True, "OK"


def validate_columns(columns):
    """Валидация столбцов"""
    if not columns:
        return False, "Столбцы не могут быть пустыми"
    columns_list = [col.strip() for col in columns.split(',')]

    for col in columns_list:
        if not col:
            return False, "Имя столбца не может быть пустым"
        if len(col) > 63:
            return False, f"Столбец '{col}' слишком длинный"
        if not col.replace('_', '').isalnum():
            return False, f"Столбец '{col}' может содержать только буквы, цифры и _"

    return True, columns_list


def input_manual_data(columns_list):
    """Ввод данных вручную"""
    print(f"\nВведите данные для столбцов: {', '.join(columns_list)}")
    print("По одной строке на запись, значения разделяйте запятыми")
    print("Пустая строка для завершения ввода")
    print("Пример: 1,John,25")

    rows = []
    row_num = 1

    while True:
        try:
            row_input = input(f"Строка {row_num}: ").strip()
            if not row_input:
                break

            values = [v.strip() for v in row_input.split(',')]

            if len(values) != len(columns_list):
                print(f"Ошибка: Ожидается {len(columns_list)} значений, получено {len(values)}")
                continue

            rows.append(values)
            row_num += 1

        except KeyboardInterrupt:
            print("\nВвод прерван")
            break
        except Exception as e:
            print(f"Ошибка: {e}")

    return rows


def read_csv_file(filepath):
    """Чтение CSV файла"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            data = list(reader)

        if not data:
            return None, "Файл пустой"

        return data, None

    except FileNotFoundError:
        return None, "Файл не найден"
    except Exception as e:
        return None, f"Ошибка чтения файла: {e}"


def read_json_file(filepath):
    """Чтение JSON файла"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if not isinstance(data, list):
            return None, "JSON должен быть массивом объектов"

        return data, None

    except json.JSONDecodeError:
        return None, "Неверный формат JSON"
    except FileNotFoundError:
        return None, "Файл не найден"
    except Exception as e:
        return None, f"Ошибка чтения файла: {e}"


def process_csv_data(data, columns_list):
    """Обработка CSV данных"""
    if len(data[0]) != len(columns_list):
        return None, f"Заголовок CSV имеет {len(data[0])} столбцов, ожидается {len(columns_list)}"

    rows = []
    for i, row in enumerate(data[1:], start=2):
        if len(row) != len(columns_list):
            print(f"Предупреждение: Строка {i} имеет {len(row)} значений, пропущена")
            continue
        rows.append(row)

    return rows, None


def process_json_data(data, columns_list):
    """Обработка JSON данных"""
    rows = []
    for i, item in enumerate(data, start=1):
        if not isinstance(item, dict):
            print(f"Предупреждение: Элемент {i} не является объектом, пропущен")
            continue

        row = []
        for col in columns_list:
            value = item.get(col, '')
            row.append(str(value) if value is not None else '')

        rows.append(row)

    return rows, None


def send_to_kafka(producer, table_name, columns_list, rows):
    """Отправка данных в Kafka"""
    try:
        message = {
            'table_name': table_name,
            'columns': columns_list,
            'data_type': 'manual',
            'data': rows
        }

        future = producer.send(TOPIC, value=message)
        result = future.get(timeout=10)

        return True, f"Данные отправлены! Топик: {result.topic}, Смещение: {result.offset}"

    except Exception as e:
        return False, f"Ошибка отправки: {e}"


def main_menu():
    """Главное меню"""
    producer = connect_kafka()
    if not producer:
        return

    while True:
        print_header()
        print("\n1. Ввести данные вручную")
        print("2. Загрузить из CSV файла")
        print("3. Загрузить из JSON файла")
        print("4. Проверить подключение к Kafka")
        print("0. Выход")

        try:
            choice = input("\nВыберите действие: ").strip()

            if choice == '1':
                input_data_manual(producer)
            elif choice == '2':
                input_data_file(producer, 'csv')
            elif choice == '3':
                input_data_file(producer, 'json')
            elif choice == '4':
                check_kafka_connection(producer)
            elif choice == '0':
                print("\nВыход из программы")
                break
            else:
                print("Неверный выбор. Попробуйте снова.")

        except KeyboardInterrupt:
            print("\n\nПрограмма прервана")
            break
        except Exception as e:
            print(f"Ошибка: {e}")

    producer.close()


def input_data_manual(producer):
    """Ввод данных вручную"""
    print("\n" + "-" * 40)
    print("РУЧНОЙ ВВОД ДАННЫХ")
    print("-" * 40)

    # Ввод имени таблицы
    while True:
        table_name = input("Введите имя таблицы: ").strip()
        is_valid, msg = validate_table_name(table_name)
        if is_valid:
            break
        print(f"Ошибка: {msg}")

    # Ввод столбцов
    while True:
        columns = input("Введите имена столбцов через запятую: ").strip()
        is_valid, result = validate_columns(columns)
        if is_valid:
            columns_list = result
            break
        print(f"Ошибка: {result}")

    # Ввод данных
    rows = input_manual_data(columns_list)

    if not rows:
        print("Нет данных для отправки")
        return

    # Подтверждение
    print(f"\nБудет создана таблица: {table_name}")
    print(f"Столбцы: {', '.join(columns_list)}")
    print(f"Количество записей: {len(rows)}")
    print("\nПервые 3 записи:")
    for i, row in enumerate(rows[:3], 1):
        print(f"  {i}. {row}")

    confirm = input("\nОтправить данные? (y/n): ").strip().lower()
    if confirm != 'y':
        print("Отправка отменена")
        return

    # Отправка
    success, msg = send_to_kafka(producer, table_name, columns_list, rows)
    print(f"\n{msg}")


def input_data_file(producer, file_type):
    """Ввод данных из файла"""
    print(f"\n" + "-" * 40)
    print(f"ЗАГРУЗКА ИЗ {file_type.upper()} ФАЙЛА")
    print("-" * 40)

    # Ввод имени таблицы
    while True:
        table_name = input("Введите имя таблицы: ").strip()
        is_valid, msg = validate_table_name(table_name)
        if is_valid:
            break
        print(f"Ошибка: {msg}")

    # Ввод столбцов
    while True:
        columns = input("Введите имена столбцов через запятую: ").strip()
        is_valid, result = validate_columns(columns)
        if is_valid:
            columns_list = result
            break
        print(f"Ошибка: {result}")

    # Ввод пути к файлу
    while True:
        filepath = input(f"Введите путь к {file_type.upper()} файлу: ").strip()
        if os.path.exists(filepath):
            break
        print(f"Файл не найден: {filepath}")

    # Чтение файла
    if file_type == 'csv':
        data, error = read_csv_file(filepath)
    else:  # json
        data, error = read_json_file(filepath)

    if error:
        print(f"Ошибка: {error}")
        return

    # Обработка данных
    if file_type == 'csv':
        rows, error = process_csv_data(data, columns_list)
    else:  # json
        rows, error = process_json_data(data, columns_list)

    if error:
        print(f"Ошибка: {error}")
        return

    if not rows:
        print("Нет данных для отправки")
        return

    # Подтверждение
    print(f"\nПрочитано записей: {len(rows)}")
    print("\nПервые 3 записи:")
    for i, row in enumerate(rows[:3], 1):
        print(f"  {i}. {row}")

    confirm = input("\nОтправить данные? (y/n): ").strip().lower()
    if confirm != 'y':
        print("Отправка отменена")
        return

    # Отправка
    success, msg = send_to_kafka(producer, table_name, columns_list, rows)
    print(f"\n{msg}")


def check_kafka_connection(producer):
    """Проверка подключения к Kafka"""
    print("\n" + "-" * 40)
    print("ПРОВЕРКА ПОДКЛЮЧЕНИЯ К KAFKA")
    print("-" * 40)

    try:
        # Попытка получить список топиков
        from kafka.admin import KafkaAdminClient

        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        topics = admin.list_topics()
        admin.close()

        print(f"✓ Подключение к Kafka успешно")
        print(f"  Брокер: {KAFKA_BROKER}")
        print(f"  Топиков: {len(topics)}")
        print(f"  Целевой топик '{TOPIC}': {'есть' if TOPIC in topics else 'отсутствует'}")

        if TOPIC not in topics:
            print(f"\nПредупреждение: Топик '{TOPIC}' не существует!")
            create = input(f"Создать топик '{TOPIC}'? (y/n): ").strip().lower()
            if create == 'y':
                from kafka.admin import NewTopic
                admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
                topic_list = [NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)]
                admin.create_topics(new_topics=topic_list, validate_only=False)
                admin.close()
                print(f"Топик '{TOPIC}' создан")

    except Exception as e:
        print(f"✗ Ошибка подключения: {e}")


if __name__ == '__main__':
    try:
        main_menu()
    except KeyboardInterrupt:
        print("\n\nПрограмма завершена")
    except Exception as e:
        print(f"\nКритическая ошибка: {e}")
        sys.exit(1)