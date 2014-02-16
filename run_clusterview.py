from antZoo.clusterView.app import app

if __name__ == "__main__":
    app.config["csrf_enabled"] = False
    app.secret_key = "hello"
    app.run( debug=True )
