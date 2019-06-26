from redis import Redis
from ast import literal_eval
from flask import Flask, request, jsonify

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

r = Redis()

@app.route('/getRecentItem')
def getRecentItem():
    date = request.args.get('date')
    recentItem = r.zrange(date, 0, 1, withscores = True)
    if not recentItem:
        return ('No records found')
    recentItem = recentItem[0][0].decode('utf-8')
    recentItem = literal_eval(r.get(recentItem).decode('utf-8'))
    try:
        del recentItem['dateAdded']
    except Exception as e:
        raise ValueError('key dateAdded not found')
    return jsonify(recentItem)

@app.route('/getitemsbyColor')
def getitemsbyColor():
    color = request.args.get('color')
    itembyColor = r.zrange(color+':color', 0, 10, withscores = True)
    if not itembyColor:
        return ('No records found')
    topTen = []
    for item in range(len(itembyColor)):
        ID = itembyColor[item][0].decode('utf-8')
        topTen.append(literal_eval(r.get(ID).decode('utf-8')))
    return jsonify(topTen)

@app.route('/getBrandsCount')
def getBrandsCount():
    brandsCount = []
    date = request.args.get('date')
    count = r.zcard(date+'_brand')
    brandsCountSet = r.zrange(date+'_brand', 0, count, desc=True, \
                           withscores = True, score_cast_func = type(1))
    if not brandsCountSet:
        return ('No records found')
    for brand in brandsCountSet:
        brandsCount.append((brand[0].decode('utf-8') , str(brand[1])))
    return jsonify(dict(brandsCount))

if __name__ == '__main__':
   app.run(debug = False) #uses default port 5000
   
