from flask import Flask,render_template,session,request, jsonify, Response, redirect, url_for
import requests
import json

db_url = "http://admin:admin@localhost:5984/"

def get_existing_databases():
	r = requests.get(db_url + "_all_dbs")
	return r.json();

def create_database(name):
	r = requests.put(db_url+name)
	return r.json();

def check_for_database(name):
	databases = get_existing_databases()
	for db in databases:
		if db == name:
			return True
	return False


def create_db_if_not_exists(name):
	if not check_for_database(name):
		create_database(name)
		return True
	return False


def add(bd,form):
	r = requests.post(db_url + bd, json = form)
	return r.json();


def getData(bd,limit,skip):
	data = {
		"selector": {},
    	"limit": limit,
    	"skip": skip
	}

	r = requests.post(db_url + bd + '/_find', json = data)
	return r.json()['docs'];


def match_db_name(year):
	return "accidents_"+year

def get_number_of_docs(db):
	r = requests.get(db_url + db)
	return r.json()['doc_count']


create_db_if_not_exists("python")




app = Flask(__name__)


@app.route('/')
def insert_menu():
	return render_template('insert.html')




@app.route('/insert',  methods = ['POST'])
def insert_to_couchdb():
    data = request.form.to_dict()
    db_name = match_db_name(data['Year'])
    create_db_if_not_exists(db_name)
    add(db_name,data)
    return redirect(url_for('insert_menu'))

@app.route('/db_fragmentation')
def fragment_db():
	bd = "accidents"
	num_docs = get_number_of_docs(bd);
	limit = 25
	current = 0
	while current < num_docs:
		real_limit = limit;
		if num_docs - current < limit:
			real_limit = num_docs-current
		data = getData(bd,real_limit,current)
		for doc in data:
			print(doc['Year'])
			del doc['_id']
			del doc['_rev']
			db_name = match_db_name(doc['Year'])
			print(db_name)
			print(create_db_if_not_exists(db_name))
			print(add(db_name,doc))
		current += real_limit

	return render_template("insert.html")

if __name__ == '__main__':
    TEMPLATES_AUTO_RELOAD = True
    app.run()

