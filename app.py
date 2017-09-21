import flask
import views


def main():
    app = flask.Flask(__name__)
    app.config['SECRET_KEY'] = 'secret!'
    app.add_url_rule('/', view_func=views.ServerInfo.as_view('server'))
    app.run(host='0.0.0.0', port=8080)


if __name__ == '__main__':
    main()
