from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_script import Manager
from flask_migrate import Migrate, MigrateCommand
from .config import Config

app = Flask(__name__)

app.config.from_object(Config)
# app.config['SQLALCHEMY_DATABASE_URI'] = "mysql+pymysql://root:hdrn59!@localhost/login"

# app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
# app.config['SECRET_KEY']='gfgsdjfghjkfgksdgfksgdkjgdskjfgsdkjgdkjgjd'

db = SQLAlchemy(app)
migrate = Migrate(app, db)

manager = Manager(app)
manager.add_command('db', MigrateCommand)

