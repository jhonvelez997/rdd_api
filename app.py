import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from flask import Flask,request,render_template, jsonify, request, url_for, redirect, abort
spark = SparkSession.builder \
    .appName("Test") \
    .master("local[*]") \
    .getOrCreate()
sc = spark.sparkContext


additional_data_movie ={}
with open("ml-100k/u.item", encoding="latin-1") as file:
    lines = file.readlines()
    for line in lines:
        dat = line.replace("\n","").split("|")
        additional_data_movie[dat[0]] = dat[1:]
additional_data_movie = sc.broadcast(additional_data_movie)

ratings = sc \
    .textFile(r"ml-100k\u.data") \
    .map(lambda x: x.split("\t"))

data = ratings \
    .map(lambda x: (x[1], int(x[2])))  \
    .mapValues(lambda x : (x,1)) \
    .reduceByKey(lambda x,y : (x[0] + y[0] , x[1] + y[1])) \
    .mapValues(lambda x: ((x[0] / x[1]),x[1])) \
    .map(lambda x : ([x[0], x[1][0], x[1][1]] + additional_data_movie.value[x[0]]))\
    .map(lambda x: x[:] + [x[4][-4:]])




app = Flask(__name__)

@app.route("/", methods =["GET","POST"])
def landing():
    if request.method == 'POST':
        if request.form['button'] == 'preview':
            return redirect(url_for('preview'))
        if request.form["button"] == "explore":
            return redirect(url_for("explore"))
    return render_template("index.html")

###########  Clickable

@app.route("/preview")
def preview():
    matrix = data \
        .map(lambda x: [x[0],x[1],x[2],x[3],x[4]]) \
        .sample(False,0.07) \
        .collect()
    return render_template("preview_data.html",matrix= matrix)

@app.route("/explore")
def explore():
    return render_template("explore_endpoints.html")


########### Api --- Endpoints 

@app.route("/get_mv_rating_api")
def get_mv_rating_api():
    _rat = float(request.args.get("rating"))
    try:
        _rat = float(_rat)
    except:
        return jsonify({"Mensjae":f"El valor ingresado no fue aceptado ({_rat}), intenta con un entero o decimal separado por punto"}), 404
    if _rat > 5 or _rat < 0:
        return jsonify({"Mensjae":f"Error, debes ingresar un rating entre 1 y 5 . Ingresaste({_rat})"})
    else:
        res = data.filter(lambda x :x[1] >=_rat ) \
            .map(lambda x: [x[3],x[4],x[1]]).sortBy(lambda x : -x[1]) \
            .collect()
        return jsonify(res)

@app.route("/get_dist")
def get_dist_api():
    d = sorted(ratings.map(lambda x: x[2]).countByValue().items())
    return jsonify(d)

@app.route("/get_mv_yr")
def get_mv_year():
    yr = request.args.get("year")
    try:
        year = float(yr)
        print(year)
    except:
        return jsonify(
            {
                "Mensaje" : f"Tienes que usar un número. Recuerda no usar comas ni puntos. Tu argumento de busqueda fue : {yr}"
                }
            ), 404 
    if year < 1800 or year > 2025 :
        return jsonify(
            {
                "Mensaje" : f"Por favor ingresa un año entre 1800 y 2025, el año ingresado fue : {yr}"
                }
            ), 404 
    else:
        resp_data = data \
            .filter(lambda x: x[-1] == yr) \
            .map(lambda x: x[0:7]).sortBy(lambda x : -x[1]) \
            .collect()
        if len(resp_data)  == 0:
            return jsonify(
                {
                    "Mensaje" : f"No Hay peliculas para el año seleccionado : {yr}"
                    }
                ), 404 
        else:
            return jsonify(resp_data)


@app.route("/get_movie_by_genre")
def get_movie_by_genre():
    mapping = {
        "action" : 7,
        "adventure": 8,
        "animation":9,
        "children" :10,
        "comedy":11,
        "crime":12,
        "documentary":13,
        "drama":14,
        "fantasy":15,
        "noir":16,
        "horror":17,
        "musical":18,
        "mystery":19,
        "romance":20,
        "scifi":21,
        "thriller":22,
        "war":23,
        "western":24
    }
    gen =  request.args.get("genre")
    genre = gen.lower()

    if genre not in list(mapping.keys()):
        return jsonify(
            {
                "Mensaje" : f"El genero ingresado: ({gen}) no se encuentra entre la lista de generos permitidos"
            }
        ), 400
    else:
        res_ = data   \
            .filter(lambda j: j[mapping[genre]] == "1" )  \
            .map(lambda x: x[0:7]) \
            .sortBy(lambda x : -x[1])
        return jsonify(res_.collect()), 200
        
app.run( port = 8000)