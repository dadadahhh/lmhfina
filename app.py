from flask import Flask, render_template

app = Flask(__name__)


@app.route('/', methods=['GET'])
def hello_world():  # put application's code here
    return render_template('a.html')


if __name__ == '__main__':
    app.run()
