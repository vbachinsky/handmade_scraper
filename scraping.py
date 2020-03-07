import requests
from bs4 import BeautifulSoup
from user_agent import generate_user_agent
import multiprocessing
import numpy
import json
import sqlite3


url = 'https://glav.su/members/%d/profile/'


def delimiter(cpus, page_list):
	return numpy.array_split(page_list, cpus)


def perform_extraction(page_ranges, cursor, conn):
	arr = []
	for page in page_ranges:
		try:
			r = requests.get(page, timeout=5, headers=headers)
			if r.status_code == 200:
				r.encoding = 'utf-8'
				soup = BeautifulSoup(r.text, 'lxml')
				name = soup.find('h1', {'class': "c-c-m-b10"})
				num_topics = soup.find(string='Сообщений')
				if r.status_code==200:
					arr.append((str(name.contents[0]).strip(), str(num_topics.parent.parent.parent.contents[5].contents[0]).strip()))
			else:
				print(r.status_code)
		except requests.Timeout as e:
			print('Timeout', str(e))
		except:
			print('Shit happens')
	try:
		cursor.executemany("INSERT OR IGNORE INTO users VALUES (?,?)", arr)
	except sqlite3.OperationalError:
		print("Некорректные данные для сохранения")
	conn.commit()


page_list = [url % x for x in range(0,30)]
workers = []
cpus = multiprocessing.cpu_count()
headers = {'User-Agent': generate_user_agent(device_type='desktop', os=('mac', 'linux'))}
page_bins = delimiter(cpus, page_list)

conn = sqlite3.connect('testDB.db')
cursor = conn.cursor()
cursor.execute("CREATE TABLE users (login text, topics integer)")

for cpu in range(cpus):
	worker = multiprocessing.Process(name=str(cpu), target=perform_extraction, args=(page_bins[cpu], cursor, conn))
	worker.start()
	workers.append(worker)

for worker in workers:
	worker.join()

print(cursor.execute("SELECT * FROM users").fetchall())
