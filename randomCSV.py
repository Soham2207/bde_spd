import csv
import random
from datetime import datetime, timedelta

# Generate list of 10 consecutive dates starting from today
date_list = [datetime.now().date() + timedelta(days=i) for i in range(10)]

# Generate list of 10 random scores between 0 and 1
score_list = [random.uniform(0, 1) for _ in range(10)]

# Write the date and score lists to a CSV file
with open('scores.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["date", "score"])
    for date, score in zip(date_list, score_list):
        writer.writerow([date.strftime('%Y-%m-%d'), score])
