import csv
import random
import pandas as pd
from concurrent.futures import ProcessPoolExecutor

categories = ['A','B','C','D']

def generate_csv(filename, rows):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Категория', 'Значение'])
        for _ in range(rows):
            category = random.choice(categories)
            value = round(random.random() * 100, 2)
            writer.writerow([category, value])

def generate_files():
    for i in range(1,6):
        filename = f"file{i}.csv"
        while True:
            try:
                num = int(input(f"Введите количество строк в файле {i}: "))
                if num>0:
                    generate_csv(filename, num)
                    break
                else:
                    print("Число должно быть целым")
            except ValueError:
                print("Число должно быть целым")

def analyze_file(filename):
    df = pd.read_csv(filename)
    result = {"filename": filename, "median": {}, "deviation": {}}

    for category in categories:
        category_data = df[df['Категория'] == category]['Значение']

        # Вычисляем медиану и стандартное отклонение
        median_value = category_data.median()
        deviation_value = category_data.std()

        result["median"][category] = median_value
        result["deviation"][category] = deviation_value

    return result


def main():
    generate_files()
    filenames = [f"file{i}.csv" for i in range(1, 6)]
    with ProcessPoolExecutor() as executor:
        results = list(executor.map(analyze_file, filenames))

    all_medians = {category: [] for category in categories}

    for result in results:
        print("\n")
        print(f"Файл: {result['filename']}")
        print(f"{'-' * 40}")
        print("Категория | Медиана | Отклонение")
        print("-" * 40)

        for category in categories:
            median_val = round(result["median"][category], 2)
            deviation_val = round(result["deviation"][category], 2)
            all_medians[category].append(median_val)

            category_str = f"{category}".center(10)
            median_str = f"{median_val}".center(9)
            deviation_str = f"{deviation_val}".center(10)

            print(f"{category_str}|{median_str}|{deviation_str}")

    print(f"\n{'-' * 40}")
    print("ИТОГИ ПО ВСЕМ ФАЙЛАМ")
    print(f"{'-' * 40}")
    print("Категория | Медиана медиан | Отклонение медиан")
    print("-" * 40)

    for category in categories:
        if all_medians[category]:
            final_median = pd.Series(all_medians[category]).median()
            final_deviation = pd.Series(all_medians[category]).std()
            category_str = f"{category}".center(10)
            median_str = f"{round(final_median,2)}".center(16)
            deviation_str = f"{round(final_deviation,2)}".center(17)

            print(f"{category_str}|{median_str}|{deviation_str}")


if __name__ == "__main__":
    main()